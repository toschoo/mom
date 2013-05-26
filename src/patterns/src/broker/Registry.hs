{-# LANGUAGE CPP #-}
module Registry 
#ifndef TEST
       (insert, remove, getWorker, freeWorker, getServiceName,
        lookupService, clean, size, stat, statPerService)
#endif
where

  import           Heartbeat
  import           Network.Mom.Patterns.Broker.Common
  import           Network.Mom.Patterns.Streams.Types (Identity)

  import           Control.Concurrent
  import           Control.Monad (when)
  import           Control.Exception 
  import           Control.Applicative ((<$>))
  import           Prelude hiding (catch)

  import           System.IO.Unsafe

  import qualified Data.ByteString.Char8 as B
  import           Data.List (delete, deleteBy, find)
  import           Data.Map   (Map)
  import qualified Data.Map   as   M
  import qualified Data.Sequence as  S
  import           Data.Sequence (Seq, (|>), (<|), (><), ViewL(..), ViewR(..)) 
  import           Data.Foldable (toList)
  import           Data.Time.Clock

  data State = Free | Busy 
    deriving (Show, Eq)
                 
  data Worker = Worker {
                  wrkId    :: Identity,
                  wrkState :: State,
                  wrkHB    :: Heartbeat,
                  wrkQ     :: MVar Queue
                }

  commonHb :: Msec
  commonHb = 100

  nbCheck :: Int
  nbCheck = 10

  type WrkNode = (Identity, Worker)
  type WrkTree = Map Identity Service

  updHb :: UTCTime -> WrkNode -> WrkNode
  updHb t (i, w) = (i, w {wrkHB = updAction t $ wrkHB w})

  data Service = Service {
                   srvName :: B.ByteString,
                   srvQ    :: MVar Queue
                 }

  type SrvNode = (B.ByteString, Service)
  type SrvTree = Map B.ByteString Service

  {-# NOINLINE _srv #-}
  _srv :: MVar SrvTree
  _srv = unsafePerformIO $ newMVar M.empty 

  {-# NOINLINE _s #-}
  _s :: MVar (Seq B.ByteString)
  _s = unsafePerformIO $ newMVar S.empty 

  {-# NOINLINE _wrk #-}
  _wrk :: MVar WrkTree
  _wrk = unsafePerformIO $ newMVar M.empty 

  insert :: Identity -> B.ByteString -> IO ()
  insert i s = do
    mbW <- lookupW i
    case mbW of
      Just _  -> return ()
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

  freeWorker :: Identity -> IO ()
  freeWorker i = getCurrentTime >>= \now ->
                   freeWorkerWithUpd i (updHb now)

  freeWorkerWithUpd :: Identity -> (WrkNode -> WrkNode) -> IO ()
  freeWorkerWithUpd i upd = do
    mbW <- lookupW i
    case mbW of
      Nothing -> return () -- silent error
      Just sn -> modifyMVar_ (srvQ sn) $ \q -> return $ setStateQ i Free upd q 

  updWorkerHb :: Identity -> IO ()
  updWorkerHb i = getCurrentTime >>= \now -> updWorker i (updHb now)

  updWorker :: Identity -> (WrkNode -> WrkNode) -> IO ()
  updWorker i f = do
    mbW <- lookupW i
    case mbW of
      Nothing -> return ()
      Just sn -> modifyMVar_ (srvQ sn) $ \q -> 
                   return $ updateQ i f q

  getServiceName :: Identity -> IO (Maybe B.ByteString)
  getServiceName i = do
    mbW <- lookupW i
    case mbW of
      Nothing -> return Nothing
      Just s  -> return $ Just (srvName s)

  lookupService :: B.ByteString -> IO Bool
  lookupService s = do
    mbS <- lookupS s
    case mbS of
      Nothing -> return False
      Just  _ -> return True

  getWorker :: B.ByteString -> IO (Maybe Identity)
  getWorker s = do
    now <- getCurrentTime
    mbS <- lookupS s
    case mbS of
      Nothing -> return Nothing
      Just sn -> do
        burry now sn -- remove non-responsive workers
        modifyMVar (srvQ sn) $ \q ->
          case firstFreeQ q of
            Just (i,_) ->
              return (setStateQ i Busy (updHb now) q, Just i)
            Nothing    -> 
              case firstBusyQ q of
                Nothing    -> return (q, Nothing)
                Just (i,_) -> return (q, Just i)
          
  checkWorker :: IO [Identity]
  checkWorker = do
    mbS <- checkService 
    case mbS of
      Nothing -> return []
      Just  s -> do
        now <- getCurrentTime
        burry now s -- remove non-responsive workers
        is <- withMVar (srvQ s) $ \q -> do
                return $ map fst $ takeIfQ nbCheck Free (f now) q
        mapM_ (\i -> updWorker i (updSt now)) is
        -- when (not $ null is) $ putStrLn $ "checkWorker: " ++ show is
        return is
    where f now w = case (testHB now . wrkHB . snd) w of
                      HbSend -> True
                      _      -> False
          updSt now (i,w) = let hb  = wrkHB w
                                hb2 = hb {hbBeat = True}
                             in (i, w{wrkHB = hb2}) 

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
              xs :> x -> return xs
          rm = modifyMVar_ _s rmTail

  burry :: UTCTime -> Service -> IO ()
  burry now sn = do
    q <- readMVar $ srvQ sn
    let f = findDeads [HbDead        ] now $ qFree q
    let b = findDeads [HbDead, HbSend] now $ qBusy q 
    -- when (not $ null $ f++b) $ print $ map fst $ f++b
    mapM_ remove (map fst $ f++b)
    where findDeads sts now = toList . S.takeWhileL (dead sts now)
          dead sts now (_, w) | testHB now (wrkHB w) `elem` sts = True
                              | otherwise                       = False 

  clean :: IO ()
  clean = do modifyMVar_ _wrk $ \_ -> return M.empty
             modifyMVar_ _srv $ \_ -> return M.empty
             modifyMVar_ _s   $ \_ -> return S.empty

  size :: IO Int
  size = withMVar _wrk $ \t -> return $ M.size t

  statPerService :: B.ByteString -> IO (B.ByteString, Int, Int)
  statPerService s = do
    mbS <- lookupS s
    case mbS of
      Nothing -> return (s, 0, 0)
      Just sn -> perService (s, sn)

  stat :: IO [(B.ByteString, Int, Int)]
  stat = withMVar _srv $ \t -> mapM perService $ M.assocs t

  perService :: SrvNode -> IO (B.ByteString, Int, Int)
  perService (s, sn) = withMVar (srvQ sn) $ \q ->
                         return (s, S.length $ qFree q,
                                    S.length $ qBusy q)

  printQ :: B.ByteString -> IO ()
  printQ s = do
    mbS <- lookupS s
    case mbS of
      Nothing -> return ()
      Just s  -> do q <- readMVar (srvQ s)
                    putStr "Free: "
                    print $ map fst $ toList $ qFree q
                    putStrLn ""
                    putStr "Busy: "
                    print $ map fst $ toList $ qBusy q 

  initW :: Identity -> Service -> IO ()
  initW i sn = modifyMVar_ (srvQ sn) $ \q -> do
                 hb <- newHeartbeat commonHb
                 let w = Worker {
                           wrkId    = i,
                           wrkState = Free,
                           wrkHB    = hb,
                           wrkQ     = srvQ sn}
                 return $ insertQ (i,w) q
                    
  lookupW :: Identity -> IO (Maybe Service)
  lookupW i = withMVar _wrk $ \t -> return $ M.lookup i t

  lookupS :: B.ByteString -> IO (Maybe Service)
  lookupS sn = withMVar _srv $ \t -> return $ M.lookup sn t

  insertW :: Identity -> Service -> IO ()
  insertW i sn = modifyMVar_ _wrk $ \t -> return $ M.insert i sn t

  insertS :: B.ByteString -> Service -> IO ()
  insertS s sn = do
    modifyMVar_ _srv $ \t  -> return $ M.insert s sn t
    modifyMVar_ _s   $ \ss -> return $ ss |> s
  
  deleteW :: Identity -> IO ()
  deleteW i = modifyMVar_ _wrk $ \t -> return $ M.delete i t
  
  deleteS :: B.ByteString -> IO ()
  deleteS s = modifyMVar_ _srv $ \t -> return $ M.delete s t

  data Queue = Q {
                 qFree :: Seq WrkNode,
                 qBusy :: Seq WrkNode 
               }

  getQ :: Identity -> Queue -> Maybe WrkNode
  getQ i q = lookupQ i Free q ~> lookupQ i Busy q

  lookupQ :: Identity -> State -> Queue -> Maybe WrkNode
  lookupQ i s q = 
      let r = toList $ snd $ S.breakl (eq i) l
       in if null r then Nothing else Just $ head r
    where l  = getList q s
          
  insertQ :: WrkNode -> Queue -> Queue
  insertQ w q = q{qFree = qFree q |> w}

  removeQ :: Identity -> Queue -> Queue
  removeQ i q = (removeWithStateQ i Free . removeWithStateQ i Busy) q 

  removeWithStateQ :: Identity -> State -> Queue -> Queue
  removeWithStateQ i s q = 
    case S.viewl t of
      EmptyL    -> q
      (x :< xs) -> case s of
                     Free -> q{qFree = h >< xs}
                     Busy -> q{qBusy = h >< xs}
    where (h,t) = getWithStateQ i s q

  updateQ :: Identity -> (WrkNode -> WrkNode) -> Queue -> Queue
  updateQ i f q = (updateWithStateQ i Free f . updateWithStateQ i Busy f) q

  updateWithStateQ :: Identity -> State    -> 
                      (WrkNode -> WrkNode) -> Queue -> Queue
  updateWithStateQ i s f q =
    case S.viewl t of
      EmptyL    -> q
      (x :< xs) -> case s of
                     Free -> q{qFree = h >< (f x) <| xs}
                     Busy -> q{qBusy = h >< (f x) <| xs}
    where (h,t) = getWithStateQ i s q

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

  getWithStateQ :: Identity -> State -> Queue -> (Seq WrkNode, Seq WrkNode)
  getWithStateQ i s q = case s of
                          Free -> S.breakl (eq i) $ qFree q
                          Busy -> S.breakl (eq i) $ qBusy q

  emptyQ :: Queue -> Bool
  emptyQ q = S.null (qFree q) && S.null (qBusy q)

  firstFreeQ :: Queue -> Maybe WrkNode
  firstFreeQ q = firstQ (qFree q) q

  firstBusyQ :: Queue -> Maybe WrkNode
  firstBusyQ q = firstQ (qBusy q) q

  firstQ :: Seq WrkNode -> Queue -> Maybe WrkNode
  firstQ f q = case S.viewl f of
                 EmptyL    -> Nothing
                 (w :< ws) -> Just w

  takeIfQ :: Int -> State -> (WrkNode -> Bool) -> Queue -> [WrkNode]
  takeIfQ n s f q = case s of
                      Free -> takeIf n f $ qFree q
                      Busy -> takeIf n f $ qBusy q

  takeIf :: Int -> (WrkNode -> Bool) -> Seq WrkNode -> [WrkNode]
  takeIf 0 _ _ = []
  takeIf n f s = case S.viewl s of
                   EmptyL    -> []
                   (w :< ws) | f w       -> w : takeIf (n-1) f ws
                             | otherwise ->     takeIf  n    f ws

  eq :: Identity -> WrkNode -> Bool
  eq i = (== i) . fst 

  getList :: Queue -> State -> Seq WrkNode
  getList q s = case s of
                  Free -> qFree q
                  Busy -> qBusy q

  infixl 9 ~>
  (~>) :: Maybe a -> Maybe a -> Maybe a
  (~>) f g = case f of
               Nothing -> g
               Just x  -> Just x
