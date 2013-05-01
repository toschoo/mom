{-# LANGUAGE CPP #-}
module Registry 
#ifndef TEST
       (insert, remove, getWorker, freeWorker, getServiceName,
        clean, size, stat, statPerService)
#endif
where

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
  import           Data.Sequence (Seq, (|>), (<|), (><), ViewL(..)) 
  import           Data.Foldable (toList)


  data State = Free | Busy 
    deriving (Show, Eq)
                 
  data Worker = Worker {
                  wrkId    :: Identity,
                  wrkState :: State,
                  -- last action + heartbeat
                  wrkQ     :: MVar Queue
                }

  type WrkNode = (Identity, Worker)
  type WrkTree = Map Identity Service

  data Service = Service {
                   srvName :: B.ByteString,
                   srvQ    :: MVar Queue
                 }

  type SrvNode = (B.ByteString, Service)
  type SrvTree = Map B.ByteString Service

  {-# NOINLINE _srv #-}
  _srv :: MVar SrvTree
  _srv = unsafePerformIO $ newMVar M.empty 

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
        initW i sn
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
  freeWorker i = do
    mbW <- lookupW i
    case mbW of
      Nothing -> return () -- silent error
      Just sn -> modifyMVar_ (srvQ sn) $ \q -> 
                   return $ setStateQ i Free q -- update last action

  getServiceName :: Identity -> IO (Maybe B.ByteString)
  getServiceName i = do
    mbW <- lookupW i
    case mbW of
      Nothing -> return Nothing
      Just s  -> return $ Just (srvName s)

  getWorker :: B.ByteString -> IO (Maybe Identity)
  getWorker s = do
    mbS <- lookupS s
    case mbS of
      Nothing -> return Nothing
      Just sn -> modifyMVar (srvQ sn) $ \q ->
                   case firstFreeQ q of
                     Just (i,_) -> 
                       return (setStateQ i Busy q, Just i)  -- update last action
                     Nothing    -> case firstBusyQ q of
                                     Nothing    -> return (q, Nothing)
                                     Just (i,_) -> return (q, Just i)

  clean :: IO ()
  clean = do modifyMVar_ _wrk $ \_ -> return M.empty
             modifyMVar_ _srv $ \_ -> return M.empty

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
                 let w = Worker {
                           wrkId    = i,
                           wrkState = Free,
                           wrkQ     = srvQ sn}
                 return $ insertQ (i,w) q
                    
  lookupW :: Identity -> IO (Maybe Service)
  lookupW i = withMVar _wrk $ \t -> return $ M.lookup i t

  lookupS :: B.ByteString -> IO (Maybe Service)
  lookupS sn = withMVar _srv $ \t -> return $ M.lookup sn t

  insertW :: Identity -> Service -> IO ()
  insertW i sn = modifyMVar_ _wrk $ \t -> return $ M.insert i sn t

  insertS :: B.ByteString -> Service -> IO ()
  insertS s sn = modifyMVar_ _srv $ \t -> return $ M.insert s sn t
  
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

  setStateQ :: Identity -> State -> Queue -> Queue
  setStateQ i s q = case S.viewl t of
                     EmptyL    -> q
                     (x :< xs) -> case s of 
                                    Free -> q{qFree = qFree q |> x,
                                              qBusy = h >< xs}
                                    Busy -> q{qBusy = qBusy q |> x,
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

  -- remove all dead notes and return list of removed nodes
  purgeQ :: Queue -> (Queue, [Identity])
  purgeQ q = undefined

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
