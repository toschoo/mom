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
       (Registry, newReg,
        insert, remove, 
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
  import           Control.Applicative ((<$>))

  import qualified Data.ByteString.Char8 as B
  import           Data.Map   (Map)
  import qualified Data.Map   as   M -- filter, keys
  import qualified Data.Sequence as  S
  import           Data.Sequence (Seq, (|>), (<|), (><), ViewL(..), ViewR(..)) 
  import           Data.Foldable (toList)
  import           Data.Time.Clock
                 
  ------------------------------------------------------------------------
  -- Registry 
  ------------------------------------------------------------------------
  type Registry = MVar Reg
  data Reg      = NewReg {
                     rgHb  :: Msec,
                     rgSrv :: SrvTree,
                     rgWrk :: WrkTree,
                     rgS   :: Seq B.ByteString
                  }

  newReg :: Msec -> IO Registry
  newReg ms = newMVar NewReg {
                    rgHb  = ms,
                    rgSrv = M.empty,
                    rgWrk = M.empty,
                    rgS   = S.empty}

  modifyReg :: Registry -> (Reg -> IO (Reg, r)) -> IO r
  modifyReg = modifyMVar 

  modifyReg_ :: Registry -> (Reg -> IO Reg) -> IO ()
  modifyReg_ = modifyMVar_ 

  withReg :: Registry -> (Reg -> IO r) -> IO r
  withReg = withMVar 

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
                  wrkHB    :: Heartbeat,
                  wrkQ     :: MVar Queue
                }

  -------------------------------------------------------------------------
  -- Set common heartbeat period
  -------------------------------------------------------------------------
  setHbPeriod :: Registry -> Msec -> IO ()
  setHbPeriod r hb = modifyReg_ r $ \reg -> 
                       return reg{rgHb = hb}

  -------------------------------------------------------------------------
  -- Update worker's heartbeat descriptor according to function
  -------------------------------------------------------------------------
  updHb :: (UTCTime -> Heartbeat -> Heartbeat) -> 
            UTCTime -> WrkNode -> WrkNode
  updHb upd t (i, w) = (i, w {wrkHB = upd t $ wrkHB w})

  ------------------------------------------------------------------------
  -- A Worker Node is an Identity paired with a worker (lookup)
  -- A Worker Tree is a map of identities and services,
  -- where a service is basically a queue of free and busy workers;
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
  -- Register new worker for service
  -------------------------------------------------------------------------
  insert :: Registry -> Identity -> B.ByteString -> IO ()
  insert r i s = modifyReg_ r $ \reg -> 
    case M.lookup i (rgWrk reg) of
      Just _  -> return reg -- identity already known
      Nothing -> do
        (reg2, sn) <- case M.lookup s (rgSrv reg) of
                        Just x  -> return (reg, x) -- service exists
                        Nothing -> do              -- new service
                          q <- newMVar $ Q S.empty S.empty
                          let x  = Service {
                                    srvName = s,
                                    srvQ    = q}
                          return (reg {rgSrv = M.insert s x (rgSrv reg),
                                       rgS   = rgS reg |> s}, x)
        hb <- newHeartbeat (rgHb reg2)
        initW  i hb sn                  -- add worker to service queue
        return reg2 {rgWrk =            -- insert worker into wrk tree
          M.insert i sn $ rgWrk reg2}

  -------------------------------------------------------------------------
  -- Remove worker 
  -------------------------------------------------------------------------
  remove :: Registry -> Identity -> IO ()
  remove r i = modifyReg_ r $ \reg -> 
    case M.lookup i (rgWrk reg) of
      Nothing -> return reg                        -- worker does not exist
      Just sn -> do
        let reg2 = reg {rgWrk = M.delete i (rgWrk reg)} -- delete from tree
        e <- modifyMVar (srvQ sn) $ \q ->               -- remove from queue
               let q' = removeQ i q
                   e  = emptyQ q'
                in return (q', e)
        if e then return reg2{rgSrv =                -- remove empty service
                    M.delete (srvName sn) (rgSrv reg2)}
             else return reg2

  -------------------------------------------------------------------------
  -- Free a worker that has been busy
  -- updating his heatbeat
  -------------------------------------------------------------------------
  freeWorker :: Registry -> Identity -> IO ()
  freeWorker r i = getCurrentTime >>= \now ->
                     freeWorkerWithUpd r i (updHb updHim now)

  -------------------------------------------------------------------------
  -- Free a worker that has been busy
  -- updating heartbeat (because we apparently have feedback )
  -------------------------------------------------------------------------
  freeWorkerWithUpd :: Registry -> Identity -> (WrkNode -> WrkNode) -> IO ()
  freeWorkerWithUpd r i upd = withReg r $ \reg -> 
    case M.lookup i (rgWrk reg) of
      Nothing -> return ()                       -- worker does not exist
      Just sn -> modifyMVar_ (srvQ sn) $ \q -> 
                   return $ setStateQ i Free upd q -- set free and upd hb 

  -------------------------------------------------------------------------
  -- Update heartbeat
  -------------------------------------------------------------------------
  updWorkerHb :: Registry -> Identity -> IO ()
  updWorkerHb r i = getCurrentTime >>= \now -> 
                      updWorker r i (updHb updHim now)

  -------------------------------------------------------------------------
  -- Generic worker update
  -------------------------------------------------------------------------
  updWorker :: Registry -> Identity -> (WrkNode -> WrkNode) -> IO ()
  updWorker r i f = withReg r $ \reg ->
    case M.lookup i (rgWrk reg) of
      Nothing -> return ()        -- worker does not exist
      Just sn -> modifyMVar_ (srvQ sn) $ \q -> 
                   return $ updateQ i f q -- update worker

  -------------------------------------------------------------------------
  -- Get service by worker identity
  -------------------------------------------------------------------------
  getServiceName :: Registry -> Identity -> IO (Maybe B.ByteString)
  getServiceName r i = withReg r $ \reg -> 
    case M.lookup i (rgWrk reg) of
      Nothing -> return Nothing
      Just s  -> return $ Just (srvName s)

  -------------------------------------------------------------------------
  -- Check whether service exists
  -------------------------------------------------------------------------
  lookupService :: Registry -> B.ByteString -> IO Bool
  lookupService r s = do
    mbS <- lookupS r s
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
  getWorker :: Registry -> B.ByteString -> IO (Maybe Identity)
  getWorker r s = do
    now <- getCurrentTime
    mbS <- lookupS r s
    case mbS of
      Nothing -> return Nothing -- service does not exist
      Just sn -> do
        bury r now sn    -- remove non-responsive workers
        modifyMVar (srvQ sn) $ \q ->
          case firstFreeQ q of -- get first of free
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
  checkWorker :: Registry -> IO [Identity]
  checkWorker r = do
    now <- getCurrentTime
    checkService r >>= \mbS -> case mbS of
                                 Nothing -> return ()
                                 Just s  -> bury r now s
    srv <- M.elems <$> withReg r (return . rgSrv) 
    concat <$> mapM (getWorkers now) srv

    where getWorkers now s = do
            is <- withMVar (srvQ s) ( 
                    return . map fst . takeIfQ Free tst)
            mapM_ (\i -> updWorker r i updSt) is
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
  checkService :: Registry -> IO (Maybe Service)
  checkService r = do 
    mbS <- modifyReg r $ \reg -> do
      let (s, mbS) = getS (rgS reg)
      return (reg {rgS = s}, mbS)
    case mbS of 
      Nothing -> return Nothing
      Just s  -> do
        mbSrv <- lookupS r s
        case mbSrv of
          Nothing  -> rm >> checkService r
          Just srv -> return $ Just srv
    where getS s = 
            case S.viewl s of
              EmptyL  -> (s, Nothing)
              x :< xs -> (xs |> x, Just x)
          rmTail s = 
            case S.viewr s of
              EmptyR  -> s
              xs :> _ -> xs
          rm = modifyReg_ r $ \reg -> 
                 return reg {rgS = rmTail (rgS reg)}

  -------------------------------------------------------------------------
  -- Remove unresponsive workers
  -------------------------------------------------------------------------
  bury :: Registry -> UTCTime -> Service -> IO ()
  bury r now sn = do 
    q <- readMVar $ srvQ sn -- we use remove on registry, so don't block
    mapM_ (remove r . fst) $ findDead $ qFree q
    mapM_ (remove r . fst) $ findDead $ qBusy q
    where findDead = filter (not . alive now . wrkHB . snd) . toList

  -------------------------------------------------------------------------
  -- Clean up registry
  -------------------------------------------------------------------------
  clean :: Registry -> IO ()
  clean r = modifyReg_ r $ \reg -> return reg {
                                     rgWrk = M.empty,
                                     rgSrv = M.empty,
                                     rgS   = S.empty}

  -------------------------------------------------------------------------
  -- Number of workers
  -------------------------------------------------------------------------
  size :: Registry -> IO Int
  size r = withReg r $ \reg -> return $ M.size (rgWrk reg)

  -------------------------------------------------------------------------
  -- Per service:
  --     number of free workers
  --     number of busy workers
  -------------------------------------------------------------------------
  statPerService :: Registry -> B.ByteString -> IO (B.ByteString, Int, Int)
  statPerService r s = do
    mbS <- lookupS r s
    case mbS of
      Nothing -> return (s, 0, 0)
      Just sn -> perService (s, sn)

  -------------------------------------------------------------------------
  -- For all services:
  --     number of free workers
  --     number of busy workers
  -------------------------------------------------------------------------
  stat :: Registry -> IO [(B.ByteString, Int, Int)]
  stat r = withReg r $ \reg -> 
             mapM perService $ M.assocs (rgSrv reg)

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
  printQ :: Registry -> B.ByteString -> IO ()
  printQ r s = do
    mbS <- lookupS r s
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
  initW :: Identity -> Heartbeat -> Service -> IO ()
  initW i hb sn = modifyMVar_ (srvQ sn) $ \q -> do
                    let w = Worker {
                              wrkId    = i,
                              wrkHB    = hb,
                              wrkQ     = srvQ sn}
                    return $ insertQ (i,w) q
                    
  -------------------------------------------------------------------------
  -- Lookup service
  -------------------------------------------------------------------------
  lookupS :: Registry -> B.ByteString -> IO (Maybe Service)
  lookupS r sn = withMVar r $ \reg -> 
                   return $ M.lookup sn (rgSrv reg)

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
  -- Take those that fulfil f with state s
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
