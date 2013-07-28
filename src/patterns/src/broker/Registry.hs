{-# LANGUAGE CPP #-}
-------------------------------------------------------------------------------
-- |
-- Module     : Registry.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Backoffice for majordomo broker
-------------------------------------------------------------------------------
module Registry 
#ifndef TEST
       (insert, remove, 
        checkWorker, getWorker, freeWorker, 
        getServiceName,
        updWorkerHb, lookupService, 
        clean, setHbPeriod, 
        size, stat, statPerService,
        printQ, getQ)
#endif
where

  import           Heartbeat
  import           Network.Mom.Patterns.Types (Msec, Identity)

  import           Control.Concurrent
  import           Control.Monad (when)
  import           Control.Applicative ((<$>))

  import           System.IO.Unsafe

  import qualified Data.ByteString.Char8 as B
  import           Data.Map   (Map)
  import qualified Data.Map   as   M -- filter, keys
  import qualified Data.Sequence as  S
  import           Data.Sequence (Seq, (|>), (<|), (><), ViewL(..), ViewR(..)) 
  import           Data.Foldable (toList)
  import           Data.Time.Clock

  ------------------------------------------------------------------------
  -- Worker may be free or busy
  ------------------------------------------------------------------------
  data State = Free | Busy 
    deriving (Show, Eq)
                 
  ------------------------------------------------------------------------
  -- Worker 
  ------------------------------------------------------------------------
  data Worker = Worker {
                  wrkId    :: Identity,
                  wrkState :: State,
                  wrkHB    :: Heartbeat,
                  wrkQ     :: MVar Queue
                }

  ------------------------------------------------------------------------
  -- Base for Heartbeat calculation:
  -- heartbeat is expected common * tolerance
  -- The value is not too important,
  -- since the actual heartbeat is decided by a parameter --
  -- Nevertheless, the value should be configurable!
  ------------------------------------------------------------------------
  {-# NOINLINE _hb #-}
  _hb :: MVar Msec
  _hb = unsafePerformIO $ newMVar 2000 -- default: 2 seconds

  -------------------------------------------------------------------------
  -- Set common heartbeat period
  -------------------------------------------------------------------------
  setHbPeriod :: Msec -> IO ()
  setHbPeriod hb = modifyMVar_ _hb $ \_ -> return hb

  -------------------------------------------------------------------------
  -- Update worker's heartbeat according to second order function
  -------------------------------------------------------------------------
  updHb :: (UTCTime -> Heartbeat -> Heartbeat) -> 
            UTCTime -> WrkNode -> WrkNode
  updHb upd t (i, w) = (i, w {wrkHB = upd t $ wrkHB w})

  ------------------------------------------------------------------------
  -- A Worker Node is an Identity paired with a worker (lookup)
  -- A Worker Tree is a map of identities and services,
  -- where service is a queue of free and busy workers;
  -- the worker tree, hence, does not point directly to the worker,
  -- but to the queue where the worker can be found:
  --
  -- tree
  --  |
  --  ----> (i,srv)
  --            |
  --            ----> (sn,srvQ)
  --                       |
  --                       -----> Free Seq (i,wrk)
  --                       -----> Busy Seq (i,wrk)
  -------------------------------------------------------------------------
  type WrkNode = (Identity, Worker)
  type WrkTree = Map Identity Service

  -------------------------------------------------------------------------
  -- A service is a queue of free and busy workers
  -------------------------------------------------------------------------
  data Service = Service {
                   srvName :: B.ByteString,
                   srvQ    :: MVar Queue
                 }

  -------------------------------------------------------------------------
  -- A service node is a pair of (ServiceName, Service)
  -- A service tree is a map  of ServiceName and Service
  -------------------------------------------------------------------------
  type SrvNode = (B.ByteString, Service)
  type SrvTree = Map B.ByteString Service

  -------------------------------------------------------------------------
  -- service tree
  -------------------------------------------------------------------------
  {-# NOINLINE _srv #-}
  _srv :: MVar SrvTree
  _srv = unsafePerformIO $ newMVar M.empty 

  -------------------------------------------------------------------------
  -- A sequence of services, used for heartbeating:
  -- we beat the first commonHb workers of the first service,
  -- then we set the service to the end of the sequence
  -------------------------------------------------------------------------
  {-# NOINLINE _s #-}
  _s :: MVar (Seq B.ByteString)
  _s = unsafePerformIO $ newMVar S.empty 

  -------------------------------------------------------------------------
  -- worker tree
  -------------------------------------------------------------------------
  {-# NOINLINE _wrk #-}
  _wrk :: MVar WrkTree
  _wrk = unsafePerformIO $ newMVar M.empty 

  -------------------------------------------------------------------------
  -- Register new worker for service
  -------------------------------------------------------------------------
  insert :: Identity -> B.ByteString -> IO ()
  insert i s = do
    mbW <- lookupW i
    case mbW of
      Just _  -> return () -- identity already known
      Nothing -> do
        sn <- lookupS s >>= getSn
        initW   i sn
        insertW i sn
    where getSn mbS = 
            case mbS of
              Just sn -> return sn
              Nothing -> do
                 q <- newMVar $ Q S.empty S.empty
                 let sn = Service {
                            srvName = s,
                            srvQ    = q}
                 insertS s sn
                 return sn

  -------------------------------------------------------------------------
  -- Remove worker 
  -------------------------------------------------------------------------
  remove :: Identity -> IO ()
  remove i = do
    mbW <- lookupW i
    case mbW of
      Nothing -> return ()
      Just sn -> do
        deleteW i
        e <- modifyMVar (srvQ sn) $ \q -> 
               let q' = removeQ i q
                   e  = emptyQ q'
                in return (q', e)
        when e $ deleteS (srvName sn)

  -------------------------------------------------------------------------
  -- Free a worker that has been busy
  -- updating his heatbeat
  -------------------------------------------------------------------------
  freeWorker :: Identity -> IO ()
  freeWorker i = getCurrentTime >>= \now ->
                   freeWorkerWithUpd i (updHb updHim now)

  -------------------------------------------------------------------------
  -- Free a worker that has been busy
  -- updating heartbeat (because we apparently have feedback )
  -------------------------------------------------------------------------
  freeWorkerWithUpd :: Identity -> (WrkNode -> WrkNode) -> IO ()
  freeWorkerWithUpd i upd = do
    mbW <- lookupW i
    case mbW of
      Nothing -> return () -- silent error
      Just sn -> modifyMVar_ (srvQ sn) $ \q -> return $ setStateQ i Free upd q 

  -------------------------------------------------------------------------
  -- Update heartbeat
  -------------------------------------------------------------------------
  updWorkerHb :: Identity -> IO ()
  updWorkerHb i = getCurrentTime >>= \now -> updWorker i (updHb updHim now)

  -------------------------------------------------------------------------
  -- Generic worker update
  -------------------------------------------------------------------------
  updWorker :: Identity -> (WrkNode -> WrkNode) -> IO ()
  updWorker i f = do
    mbW <- lookupW i
    case mbW of
      Nothing -> return ()
      Just sn -> modifyMVar_ (srvQ sn) $ \q -> 
                   return $ updateQ i f q

  -------------------------------------------------------------------------
  -- Get service by worker identity
  -------------------------------------------------------------------------
  getServiceName :: Identity -> IO (Maybe B.ByteString)
  getServiceName i = do
    mbW <- lookupW i
    case mbW of
      Nothing -> return Nothing
      Just s  -> return $ Just (srvName s)

  -------------------------------------------------------------------------
  -- Check whether service exists
  -------------------------------------------------------------------------
  lookupService :: B.ByteString -> IO Bool
  lookupService s = do
    mbS <- lookupS s
    case mbS of
      Nothing -> return False
      Just  _ -> return True

  -------------------------------------------------------------------------
  -- Get worker for service:
  --   - removing unresponsive workers on the way 
  --   - get first of free q, put it to the busy queue 
  --                          and update its heartbeat
  --   - if free q is emtpy, take the first of the busy queue
  -------------------------------------------------------------------------
  getWorker :: B.ByteString -> IO (Maybe Identity)
  getWorker s = do
    now <- getCurrentTime
    mbS <- lookupS s
    case mbS of
      Nothing -> return Nothing
      Just sn -> do
        bury now sn -- remove non-responsive workers
        modifyMVar (srvQ sn) $ \q ->
          case firstFreeQ q of
            Just (i,_) ->
              return (setStateQ i Busy (updHb updMe now) q, Just i)
            Nothing    -> 
              case firstBusyQ q of
                Nothing    -> return (q, Nothing)
                Just (i,_) -> return (
                      setStateQ i Busy (updHb updMe now) q, Just i)
          
  -------------------------------------------------------------------------
  -- Remove non-responsive workers
  -- and heartbeat those that have been inactive 
  --     for at least one heartbeat period
  -------------------------------------------------------------------------
  checkWorker :: IO [Identity]
  checkWorker = do
    now <- getCurrentTime
    checkService >>= \mbS -> case mbS of
                               Nothing -> return ()
                               Just s  -> bury now s
    concat <$> withMVar _srv (mapM (getWorkers now) . M.elems)

    where getWorkers now s = do
            is <- withMVar (srvQ s) ( 
                    return . map fst . takeIfQ Free tst)
            mapM_ (`updWorker` updSt) is
            return is

            where updSt (i,w) = let hb  = updMe now $ wrkHB w
                                 in (i, w{wrkHB = hb}) 
                  tst = checkMe now . wrkHB . snd

  -------------------------------------------------------------------------
  -- Get next service to check
  -- check on the way if service still exists
  -- if not, remove service and try next.
  -- Note: there is no upper bound;
  --       if a lot of services have been removed since our last visit
  --          we will go through this many times
  -------------------------------------------------------------------------
  checkService :: IO (Maybe Service)
  checkService = do
    mbS <- modifyMVar _s getS
    case mbS of 
      Nothing -> return Nothing
      Just s  -> do
        mbSrv <- lookupS s
        case mbSrv of
          Nothing  -> rm >> checkService
          Just srv -> return $ Just srv
    where getS s = 
            case S.viewl s of
              EmptyL  -> return (s, Nothing)
              x :< xs -> 
                return (xs |> x, Just x)
          rmTail s = 
            case S.viewr s of
              EmptyR  -> return s
              xs :> _ -> return xs
          rm = modifyMVar_ _s rmTail

  -------------------------------------------------------------------------
  -- Remove unresponsive workers
  -- a worker is unresponsive if
  --   free and dead
  --   busy and (dead or sent)
  -- where dead means that
  --            we had already sent a heartbeat
  --            and had no response last time we checked
  --       send means that
  --            it is time to send a heartbeat
  -- The two cases are hence equivalent,
  -- since the busy worker had received a job
  --       this counts as a heartbeat already sent
  -------------------------------------------------------------------------
  bury :: UTCTime -> Service -> IO ()
  bury now sn = do
    q <- readMVar $ srvQ sn
    mapM_ (remove . fst) $ findDeads $ qFree q
    mapM_ (remove . fst) $ findDeads $ qBusy q
    where findDeads = filter (not . alive now . wrkHB . snd) . toList

  -------------------------------------------------------------------------
  -- Clean up global variables
  -------------------------------------------------------------------------
  clean :: IO ()
  clean = do modifyMVar_ _wrk $ \_ -> return M.empty
             modifyMVar_ _srv $ \_ -> return M.empty
             modifyMVar_ _s   $ \_ -> return S.empty

  -------------------------------------------------------------------------
  -- Number of workers
  -------------------------------------------------------------------------
  size :: IO Int
  size = withMVar _wrk $ \t -> return $ M.size t

  -------------------------------------------------------------------------
  -- Per service:
  --     number of free workers
  --     number of busy workers
  -------------------------------------------------------------------------
  statPerService :: B.ByteString -> IO (B.ByteString, Int, Int)
  statPerService s = do
    mbS <- lookupS s
    case mbS of
      Nothing -> return (s, 0, 0)
      Just sn -> perService (s, sn)

  -------------------------------------------------------------------------
  -- For all services:
  --     number of free workers
  --     number of busy workers
  -------------------------------------------------------------------------
  stat :: IO [(B.ByteString, Int, Int)]
  stat = withMVar _srv $ \t -> mapM perService $ M.assocs t

  -------------------------------------------------------------------------
  -- Get stats per service
  -------------------------------------------------------------------------
  perService :: SrvNode -> IO (B.ByteString, Int, Int)
  perService (s, sn) = withMVar (srvQ sn) $ \q ->
                         return (s, S.length $ qFree q,
                                    S.length $ qBusy q)

  -------------------------------------------------------------------------
  -- Debug: print q
  -------------------------------------------------------------------------
  printQ :: B.ByteString -> IO ()
  printQ s = do
    mbS <- lookupS s
    case mbS of
      Nothing -> return ()
      Just x  -> do q <- readMVar (srvQ x)
                    putStr "Free: "
                    print $ map fst $ toList $ qFree q
                    putStrLn ""
                    putStr "Busy: "
                    print $ map fst $ toList $ qBusy q 

  -------------------------------------------------------------------------
  -- Create worker
  --        initialising heartbeat
  --        inserting    into service queue
  -------------------------------------------------------------------------
  initW :: Identity -> Service -> IO ()
  initW i sn = modifyMVar_ (srvQ sn) $ \q -> do
                 hb <- withMVar _hb newHeartbeat 
                 let w = Worker {
                           wrkId    = i,
                           wrkState = Free,
                           wrkHB    = hb,
                           wrkQ     = srvQ sn}
                 return $ insertQ (i,w) q
                    
  -------------------------------------------------------------------------
  -- Lookup worker, returning service
  -------------------------------------------------------------------------
  lookupW :: Identity -> IO (Maybe Service)
  lookupW i = withMVar _wrk $ \t -> return $ M.lookup i t

  -------------------------------------------------------------------------
  -- Lookup service
  -------------------------------------------------------------------------
  lookupS :: B.ByteString -> IO (Maybe Service)
  lookupS sn = withMVar _srv $ \t -> return $ M.lookup sn t

  -------------------------------------------------------------------------
  -- Insert worker
  -------------------------------------------------------------------------
  insertW :: Identity -> Service -> IO ()
  insertW i sn = modifyMVar_ _wrk $ \t -> return $ M.insert i sn t

  -------------------------------------------------------------------------
  -- Insert service
  -------------------------------------------------------------------------
  insertS :: B.ByteString -> Service -> IO ()
  insertS s sn = do
    modifyMVar_ _srv $ \t  -> return $ M.insert s sn t
    modifyMVar_ _s   $ \ss -> return $ ss |> s
  
  -------------------------------------------------------------------------
  -- Delete worker
  -------------------------------------------------------------------------
  deleteW :: Identity -> IO ()
  deleteW i = modifyMVar_ _wrk $ \t -> return $ M.delete i t
  
  -------------------------------------------------------------------------
  -- Delete service
  -------------------------------------------------------------------------
  deleteS :: B.ByteString -> IO ()
  deleteS s = modifyMVar_ _srv $ \t -> return $ M.delete s t

  -------------------------------------------------------------------------
  -- Queue:
  --    Seq free workers
  --    Seq busy workers
  -------------------------------------------------------------------------
  data Queue = Q {
                 qFree :: Seq WrkNode,
                 qBusy :: Seq WrkNode 
               }

  -------------------------------------------------------------------------
  -- Get a worker (either from free or busy)
  -------------------------------------------------------------------------
  getQ :: Identity -> Queue -> Maybe WrkNode
  getQ i q = lookupQ i Free q ~> lookupQ i Busy q

  -------------------------------------------------------------------------
  -- lookup one seq, either
  --    free or
  --    busy
  -------------------------------------------------------------------------
  lookupQ :: Identity -> State -> Queue -> Maybe WrkNode
  lookupQ i s q = 
      let r = toList $ snd $ S.breakl (eq i) l
       in if null r then Nothing else Just $ head r
    where l  = getList q s
          
  -------------------------------------------------------------------------
  -- Insert worker into q (always free)
  -------------------------------------------------------------------------
  insertQ :: WrkNode -> Queue -> Queue
  insertQ w q = q{qFree = qFree q |> w}

  -------------------------------------------------------------------------
  -- Remove worker from q (either free or busy)
  -------------------------------------------------------------------------
  removeQ :: Identity -> Queue -> Queue
  removeQ i = removeWithStateQ i Free . removeWithStateQ i Busy

  -------------------------------------------------------------------------
  -- Remove worker from q depending on state
  -------------------------------------------------------------------------
  removeWithStateQ :: Identity -> State -> Queue -> Queue
  removeWithStateQ i s q = 
    case S.viewl t of
      EmptyL    -> q
      (_ :< xs) -> case s of
                     Free -> q{qFree = h >< xs}
                     Busy -> q{qBusy = h >< xs}
    where (h,t) = getWithStateQ i s q

  -------------------------------------------------------------------------
  -- Generic update of a worker in a queue
  -------------------------------------------------------------------------
  updateQ :: Identity -> (WrkNode -> WrkNode) -> Queue -> Queue
  updateQ i f = updateWithStateQ i Free f . updateWithStateQ i Busy f 

  -------------------------------------------------------------------------
  -- Generic update of a worker in a queue depending on state
  -------------------------------------------------------------------------
  updateWithStateQ :: Identity -> State    -> 
                      (WrkNode -> WrkNode) -> Queue -> Queue
  updateWithStateQ i s f q =
    case S.viewl t of
      EmptyL    -> q
      (x :< xs) -> case s of
                     Free -> q{qFree = h >< f x <| xs}
                     Busy -> q{qBusy = h >< f x <| xs}
    where (h,t) = getWithStateQ i s q

  -------------------------------------------------------------------------
  -- Remove worker from Seq with state x and
  -- Add    it     to   Seq with state y
  -------------------------------------------------------------------------
  setStateQ :: Identity -> State -> (WrkNode -> WrkNode) -> Queue -> Queue
  setStateQ i s f q = case S.viewl t of
                        EmptyL    -> q
                        (x :< xs) -> case s of 
                                       Free -> q{qFree = qFree q |> f x,
                                                 qBusy = h >< xs}
                                       Busy -> q{qBusy = qBusy q |> f x,
                                                 qFree = h >< xs}
    where (h,t) = let s' = case s of
                             Free -> Busy
                             Busy -> Free
                   in getWithStateQ i s' q 

  -------------------------------------------------------------------------
  -- Get view of Seq, breaking on worker
  -- either free or busy
  -------------------------------------------------------------------------
  getWithStateQ :: Identity -> State -> Queue -> (Seq WrkNode, Seq WrkNode)
  getWithStateQ i s q = case s of
                          Free -> S.breakl (eq i) $ qFree q
                          Busy -> S.breakl (eq i) $ qBusy q

  -------------------------------------------------------------------------
  -- Queue is empty
  -------------------------------------------------------------------------
  emptyQ :: Queue -> Bool
  emptyQ q = S.null (qFree q) && S.null (qBusy q)

  -------------------------------------------------------------------------
  -- Head of Seq free
  -------------------------------------------------------------------------
  firstFreeQ :: Queue -> Maybe WrkNode
  firstFreeQ q = firstQ (qFree q)

  -------------------------------------------------------------------------
  -- Head of Seq busy
  -------------------------------------------------------------------------
  firstBusyQ :: Queue -> Maybe WrkNode
  firstBusyQ q = firstQ (qBusy q)

  -------------------------------------------------------------------------
  -- Head
  -------------------------------------------------------------------------
  firstQ :: Seq WrkNode -> Maybe WrkNode
  firstQ f = case S.viewl f of
               EmptyL   -> Nothing
               (w :< _) -> Just w

  -------------------------------------------------------------------------
  -- Take n that fulfil f with state s
  -------------------------------------------------------------------------
  takeIfQ :: State -> (WrkNode -> Bool) -> Queue -> [WrkNode]
  takeIfQ s f q = case s of
                      Free -> takeIf f $ qFree q
                      Busy -> takeIf f $ qBusy q

  -------------------------------------------------------------------------
  -- Take n that fulfil f from Seq
  -------------------------------------------------------------------------
  takeIf :: (WrkNode -> Bool) -> Seq WrkNode -> [WrkNode]
  takeIf f s = case S.viewl s of
                 EmptyL    -> []
                 (w :< ws) | f w       -> w : takeIf f ws
                           | otherwise ->     takeIf f ws

  eq :: Identity -> WrkNode -> Bool
  eq i = (== i) . fst 

  getList :: Queue -> State -> Seq WrkNode
  getList q s = case s of
                  Free -> qFree q
                  Busy -> qBusy q

  -------------------------------------------------------------------------
  -- Simple Maybe combinator
  -------------------------------------------------------------------------
  infixl 9 ~>
  (~>) :: Maybe a -> Maybe a -> Maybe a
  (~>) f g = case f of
               Nothing -> g
               Just x  -> Just x
