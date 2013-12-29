{-# Language BangPatterns #-}
module Registry 
where

  import           Types
  import           Network.Mom.Stompl.Client.Queue 
  import           System.Timeout
  import           Data.Time
  import           Data.Char (isDigit, toUpper)
  import           Data.List (nub)
  import           Data.Maybe (fromMaybe)
  import           Data.Map (Map)
  import qualified Data.Map as M
  import           Data.Sequence (Seq, (|>), (<|), ViewL(..))
  import qualified Data.Sequence as S
  import           Data.Foldable (toList)
  import           Codec.MIME.Type (nullType)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, catches)
  import           Control.Concurrent 
  import           Control.Monad (forever)
  import           Control.Applicative ((<$>))

  -----------------------------------------------------------------------
  -- JobType: Service, Task or Topic
  -----------------------------------------------------------------------
  data JobType = Service | Task | Topic
    deriving (Eq, Show)

  readJobType :: String -> Maybe JobType
  readJobType s = 
    case map toUpper s of
      "SERVICE" -> Just Service
      "TASK"    -> Just Task
      "TOPIC"   -> Just Topic
      _         -> Nothing

  ------------------------------------------------------------------------
  -- | Connect to a registry to receive a service
  ------------------------------------------------------------------------
  register :: Con -> JobName -> JobType -> 
                     QName -> 
                     QName -> Int -> Int -> IO (StatusCode, Int)
  register c j t o i tmo me | null j    = return (BadRequest,0)
                            | otherwise =
      let i' = o ++ "/" ++ j ++ i
          hs = [("__type__",    "register"),
                ("__job-type__",    show t),
                ("__job__",              j),
                ("__queue__",            i),
                ("__hb__",         show me),
                ("__channel__",         i')]
       in withWriter c "RegistryW" o  [] [] nobody     $ \w -> 
          withReader c "RegistryR" i' [] [] ignorebody $ \r -> do
            writeQ w nullType hs ()
            mbF <- timeout tmo $ readQ r 
            case mbF of
              Nothing -> throwIO $ TimeoutX
                                    "No response from registry"
              Just m  -> do eiS <- getSC m
                            case eiS of 
                              Left  s  -> throwIO $ BadStatusCodeX s
                              Right OK -> do h <- getHB m
                                             return (OK, h)
                              Right sc ->    return (sc, 0)

  ------------------------------------------------------------------------
  -- | Disconnect from a registry
  ------------------------------------------------------------------------
  unRegister :: Con -> JobName -> 
                       QName   -> QName -> Int -> IO StatusCode
  unRegister c j o i tmo | null j    = return BadRequest
                         | otherwise = 
      let i' = o ++ "/" ++ j ++ i
          hs = [("__type__", "unreg"),
                ("__job__",        j),
                ("__queue__",      i),
                ("__channel__",   i')]
       in withWriter c "RegistryW" o  [] [] nobody     $ \w -> 
          withReader c "RegistryR" i' [] [] ignorebody $ \r -> do
            writeQ w nullType hs ()
            mbF <- timeout tmo $ readQ r 
            case mbF of
              Nothing -> throwIO $ TimeoutX "No response from register"
              Just m  -> do eiS <- getSC m
                            case eiS of
                              Left s   -> throwIO $ BadStatusCodeX s
                              Right sc -> return sc

  ------------------------------------------------------------------------
  -- | Send heartbeats
  ------------------------------------------------------------------------
  heartbeat :: MVar HB -> Writer () -> JobName -> QName -> IO ()
  heartbeat m w j q = 
    let hs = [("__type__", "hb"),
              ("__job__",     j),
              ("__queue__",   q)]
     in do now <- getCurrentTime 
           modifyMVar_ m (go now hs)
    where go now hs hb@(HB me nxt)
            | me > 0 && nxt < now = do writeQ w nullType hs ()
                                       return hb{hbMeNext = timeAdd now me}
            | otherwise           =    return hb

  ------------------------------------------------------------------------
  -- Provider: a queue and heartbeat data
  ------------------------------------------------------------------------
  data Provider = Provider {
                    prvQ   :: QName,
                    prvHb  :: Int,
                    prvNxt :: UTCTime
                  }
    deriving Show

  ------------------------------------------------------------------------
  -- Two providers are identical if they have the same queue name
  ------------------------------------------------------------------------
  instance Eq Provider where
    x == y = prvQ x == prvQ y

  ------------------------------------------------------------------------
  -- Add provider to seq or, if already in, 
  -- update according to the values of the new node.
  ------------------------------------------------------------------------
  updOrAddProv :: (Provider -> Provider) -> Provider -> 
                  Seq Provider -> Seq Provider
  updOrAddProv upd p s = 
    case S.viewl s of
      S.EmptyL -> S.singleton p
      x :< ss  -> if prvQ x == prvQ p 
                    then upd x <| ss
                    else     x <| updOrAddProv upd p ss

  ------------------------------------------------------------------------
  -- Remove one provider from the seq
  ------------------------------------------------------------------------
  remProv :: QName -> Seq Provider -> Seq Provider
  remProv q s =
    case S.viewl s of
      S.EmptyL -> S.empty
      x :< ss  -> if prvQ x == q then ss
                                 else x <| remProv q ss

  ------------------------------------------------------------------------
  -- Get head of seq and add to end of sequence;
  -- remove all "dead" nodes on the way
  ------------------------------------------------------------------------
  getHeads :: UTCTime -> Seq Provider -> ([Provider], Seq Provider)
  getHeads now s = 
    case S.viewl s of
      S.EmptyL -> ([], S.empty)
      x :< ss  -> if prvHb  x > 0 &&
                     prvNxt x < now then getHeads now ss
                                    else ([x], ss |> x)

  ------------------------------------------------------------------------
  -- Job: 'JobType' plus 'Sequence' of 'Provider's
  ------------------------------------------------------------------------
  data JobNode = JobNode {
                    jobType  :: JobType,
                    jobProvs :: Seq Provider
                  }

  ------------------------------------------------------------------------
  -- The inner heart of the registry: 
  -- a 'Map' of 'JobName', 'JobNode'
  ------------------------------------------------------------------------
  data Reg = Reg {
               regName :: String,
               regWork :: Map JobName JobNode
             }

  ------------------------------------------------------------------------
  -- Registry (MVar of Reg)
  ------------------------------------------------------------------------
  data Registry = Registry {
                    regM :: MVar Reg
                  }

  ------------------------------------------------------------------------
  -- Use registry (with return value)
  ------------------------------------------------------------------------
  useRegistry :: Registry -> (Reg -> IO (Reg, r)) -> IO r
  useRegistry r = modifyMVar (regM r)

  ------------------------------------------------------------------------
  -- Use registry (without return value)
  ------------------------------------------------------------------------
  useRegistry_ :: Registry -> (Reg -> IO Reg) -> IO ()
  useRegistry_ r = modifyMVar_ (regM r)

  ------------------------------------------------------------------------
  -- Add provider to job
  ------------------------------------------------------------------------
  insertR :: Registry -> JobName -> JobType -> QName -> Int -> IO ()
  insertR r jn w qn i = 
    useRegistry_ r $ \reg -> do now <- getCurrentTime
                                return reg{regWork = ins now $ regWork reg}
    where ins now m = 
            let j  = fromMaybe (JobNode w S.empty) $ M.lookup jn m
                p  = Provider qn i $ nextHB now True i
                ps = updOrAddProv (upd p) p $ jobProvs j
             in M.insert jn j{jobProvs = ps} m
          upd n _ = n

  ------------------------------------------------------------------------
  -- Update heartbeat of provider 
  ------------------------------------------------------------------------
  updR :: Registry -> JobName -> QName -> IO ()
  updR r jn qn  = 
    useRegistry_ r $ \reg -> do now <- getCurrentTime
                                return reg{regWork = ins now $ regWork reg}
    where ins now m = 
            case M.lookup jn m of
              Nothing -> m
              Just j  -> let p  = Provider qn 0 now
                             ps = updOrAddProv (upd now) p 
                                               (jobProvs j)
                          in M.insert jn j{jobProvs = ps} m
          upd now o = o{prvNxt = nextHB now True $ tolerance * prvHb o}
      
  ------------------------------------------------------------------------
  -- Remove 'Provider' from the job
  ------------------------------------------------------------------------
  removeR :: Registry -> JobName -> QName -> IO ()
  removeR r jn qn = 
    useRegistry_ r $ \reg -> return reg{regWork = ins $ regWork reg}
    where ins m = 
            case M.lookup jn m of
              Nothing -> m
              Just j  -> 
                let ps = remProv qn $ jobProvs j
                 in if S.null ps then M.delete jn m
                                 else M.insert jn j{jobProvs = ps} m

  ------------------------------------------------------------------------
  -- | Map action to 'Provider's of job 'JobName';
  --   mapping means different things for:
  --
  --   * Serice, Task: action is applied to the first
  --                   active provider and this provider
  --                   is then sent to the back of the list.
  --
  --   * Topic: action is applied to all providers. 
  ------------------------------------------------------------------------
  mapR :: Registry -> JobName -> (Provider -> IO ()) -> IO Bool
  mapR r jn f = 
    useRegistry r $ \reg -> getCurrentTime        >>= \now ->
                            ins now (regWork reg) >>= \(js,t) ->
                            return (reg{regWork = js},t)
    where ins now m = 
            case M.lookup jn m of
              Nothing -> return (m, False)
              Just j  -> 
                let (xs, ps) = if jobType j `elem` [Service, Task]
                                 then getHeads now $ jobProvs j
                                 else (toList $ jobProvs j, 
                                                jobProvs j)
                 in mapM_ f xs >> 
                    return (M.insert jn j{jobProvs = ps} m, True)

  ------------------------------------------------------------------------
  -- | Map action to all 'Provider's of job 'JobName'
  --   (independent of 'JobType')
  ------------------------------------------------------------------------
  mapAllR :: Registry -> JobName -> (Provider -> Provider) -> IO ()
  mapAllR r jn f = 
    useRegistry_ r $ \reg -> ins (regWork reg) >>= \m ->
                             return reg{regWork = m}
    where ins m = 
            case M.lookup jn m of
              Nothing -> return m
              Just j  -> return (M.insert jn j{jobProvs = go $ jobProvs j} m)
          go s = case S.viewl s of
                   S.EmptyL -> S.empty
                   x :< ss  -> f x <| go ss
              
  ------------------------------------------------------------------------
  -- | Get n 'Provider's for job 'JobName'
  ------------------------------------------------------------------------
  getProvider :: Registry -> JobName -> Int -> IO [Provider]
  getProvider r jn n = 
    useRegistry r $ \reg -> do now <- getCurrentTime
                               let (x,m) = ins now $ regWork reg
                               return (reg{regWork = m}, x)
    where ins now m   = case M.lookup jn m of
                          Nothing -> ([], m)
                          Just j  -> 
                            let (x,ps) = go now (jobProvs j) n
                             in (x, M.insert jn j{jobProvs = ps} m)
          go  now ps i | i <= 0    = ([],ps)
                       | otherwise = let (!x ,ps1) = getHeads now ps
                                         (!x',ps2) = go now ps1 (i-1)
                                      in (nub (x++x'), ps2)

  ------------------------------------------------------------------------
  -- | Debug: show all jobs and providers
  ------------------------------------------------------------------------
  showRegistry :: Registry -> IO ()
  showRegistry r = 
    useRegistry_ r $ \reg -> let l  = map fst $ M.toList (regWork reg)
                                 p  = map (getProvs reg) l
                                 lp = zip l p
                              in print lp >> return reg
    where getProvs reg jn = case M.lookup jn $ regWork reg of
                              Nothing -> []
                              Just ps -> toList $ jobProvs ps

  ------------------------------------------------------------------------
  -- | Create a registry and use it
  ------------------------------------------------------------------------
  withRegistry :: Con -> String -> QName -> (Int, Int)
                      -> OnError -> (Registry -> IO a) -> IO a
  withRegistry c n rq (mn, mx) onErr action = do
    let nm  = n ++ "Registry"
    reg <- Registry <$> newMVar (Reg nm M.empty)
    withThread (startReg reg nm) (action reg)
    where startReg reg nm = 
            withReader   c (n ++ "Reader") rq [] [] ignorebody $ \r -> 
              withWriter c (n ++ "Writer") "unknown" [] [] nobody $ \w -> 
                forever $ catches 
                  (do m <- readQ    r
                      t <- getMType m
                      case t of
                        "register" -> handleRegister   reg m w (mn,mx)
                        "unreg"    -> handleUnRegister reg m w
                        "hb"       -> handleHeartbeat  reg m
                        x          -> throwIO $ HeaderX "__type__" $
                                                "Unknown type: " ++ x)
                  (ignoreHandler Error nm onErr)

  ------------------------------------------------------------------------
  -- Handle registration request
  ------------------------------------------------------------------------
  handleRegister :: Registry -> Message m -> Writer () -> (Int, Int) -> IO ()
  handleRegister r m w (mn,mx) = do
    (j,q) <- getJobQueue m
    ch    <- getChannel m
    t     <- getJobType m 
    hb    <- getHB m 
    let h | hb < mn || hb > mx = if (mn - hb) < (hb - mx) then mn else mx
          | otherwise          = hb
    insertR r j t q h
    let hs = [("__sc__", show OK),
              ("__hb__", show h)]
    writeAdHoc w ch nullType hs ()

  ------------------------------------------------------------------------
  -- Handle unRegister request
  ------------------------------------------------------------------------
  handleUnRegister :: Registry -> Message m -> Writer () -> IO ()
  handleUnRegister r m w = do
    (j,q) <- getJobQueue m
    ch    <- getChannel m
    removeR r j q 
    let hs=[("__sc__", show OK)]
    writeAdHoc w ch nullType hs ()

  ------------------------------------------------------------------------
  -- Handle heartbeat
  ------------------------------------------------------------------------
  handleHeartbeat :: Registry -> Message m -> IO ()
  handleHeartbeat r m = do
    (j,q) <- getJobQueue m
    updR r j q
    -- print $ msgHdrs m -- test

  ------------------------------------------------------------------------
  -- Get JobQueue
  ------------------------------------------------------------------------
  getJobQueue :: Message m -> IO (String, String)
  getJobQueue m = getJobName m >>= \j -> getQueue m >>= \q -> return (j,q)

  ------------------------------------------------------------------------
  -- Message Type from headers
  ------------------------------------------------------------------------
  getMType :: Message m -> IO String
  getMType = getHeader "__type__" "No message type in headers"

  ------------------------------------------------------------------------
  -- Job Name from headers
  ------------------------------------------------------------------------
  getJobName :: Message m -> IO String
  getJobName = getHeader "__job__" "No job name in headers" 

  ------------------------------------------------------------------------
  -- Reply queue (channel) from headers
  ------------------------------------------------------------------------
  getChannel :: Message m -> IO String
  getChannel = getHeader "__channel__" "No response q in headers" 

  ------------------------------------------------------------------------
  -- Queue name from headers
  ------------------------------------------------------------------------
  getQueue :: Message m -> IO String
  getQueue = getHeader "__queue__" "No queue q in headers" 

  ------------------------------------------------------------------------
  -- Job type from headers
  ------------------------------------------------------------------------
  getJobType :: Message m -> IO JobType
  getJobType m = 
    getHeader "__job-type__" "No job type in headers"  m >>= \x ->
      case readJobType x of
        Nothing -> throwIO $ HeaderX "__job-type__" $
                                     "unknown type: " ++ x 
        Just t  -> return t  

  ------------------------------------------------------------------------
  -- Heartbeat specification from headers
  ------------------------------------------------------------------------
  getHB :: Message m -> IO Int
  getHB m = 
    case lookup "__hb__" $ msgHdrs m of
      Nothing -> return 0 
      Just v  -> if all isDigit v 
                   then return $ read v
                   else throwIO $ HeaderX "__hb__" $
                          "heartbeat not numeric: "  ++ show v

  ------------------------------------------------------------------------
  -- Status code from headers
  ------------------------------------------------------------------------
  getSC :: Message m -> IO (Either String StatusCode)
  getSC m = readStatusCode <$> getHeader "__sc__"
                                 "No status code in message" m
                               
  ------------------------------------------------------------------------
  -- Generic function to retrieve a header value
  ------------------------------------------------------------------------
  getHeader :: String -> String -> Message m -> IO String
  getHeader h e m = case lookup h $ msgHdrs m of
                      Nothing -> throwIO $ HeaderX h e
                      Just v  -> return v

