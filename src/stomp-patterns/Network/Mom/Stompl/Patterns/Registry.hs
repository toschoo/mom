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
  import           Control.Exception (throwIO, catches)
  import           Control.Concurrent 
  import           Control.Monad (forever)
  import           Control.Applicative ((<$>))

  -----------------------------------------------------------------------
  -- | JobType: Service, Task or Topic
  -----------------------------------------------------------------------
  data JobType = Service | Task | Topic
    deriving (Eq, Show)

  -----------------------------------------------------------------------
  -- | Safe read method for JobType
  -----------------------------------------------------------------------
  readJobType :: String -> Maybe JobType
  readJobType s = 
    case map toUpper s of
      "SERVICE" -> Just Service
      "TASK"    -> Just Task
      "TOPIC"   -> Just Topic
      _         -> Nothing

  ------------------------------------------------------------------------
  -- | A helper that shall ease the use of the registers.
  --   A registry to which a call wants to connect is described as
  --   
  --   * The 'QName' through which the registry receives requests;
  --
  --   * The 'Timeout' in microseconds, /i.e./ the time the caller
  --                   will wait before the request fails;
  --
  --   * A triple of heartbeat specifications:
  --     the /best/ value, /i.e./ 
  --          the rate at which the caller 
  --                   prefers to send heartbeats,
  --     the /minimum/ rate at which the caller 
  --                   can accept to send heartbeats,
  --     the /maximum/ rate at which the caller 
  --                   can accept to send heartbeats.
  --     Note that all these values are in milliseconds!
  ------------------------------------------------------------------------
  type RegistryDesc = (QName, Int, (Int, Int, Int))

  ------------------------------------------------------------------------
  -- | Connect to a registry:
  --   The caller registers itself at the registry.
  --   The owner of the registry will then
  --   use the caller depending on its purpose.
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * 'JobName': The name of the job provided by the caller;
  --
  --   * 'JobType': The type of the job provided by the caller;
  --
  --   * 'QName': The registry's registration queue;
  --
  --   * 'QName': The queue to register;
  --              this is the queue the register will actually
  --              use (for forwarding requests or whatever
  --              it does in this specific case).
  --              The registry, internally,
  --              uses 'JobName' together with this queue
  --              as a /key/ to identify the provider. 
  --
  --   * Int: Timeout in microseconds;
  --
  --   * Int: Preferred heartbeat in milliseconds
  --          (0 for no heartbeats).
  --
  -- The function returns a tuple of 'StatusCode' 
  -- and the heartbeat proposed by the registry
  -- (which may differ from the preferred heartbeat of the caller).
  -- Whenever the 'StatusCode' is not 'OK', 
  -- the heartbeat is 0.
  -- If the 'JobName' is null, the 'StatusCode' will be 'BadRequest'.
  -- If the timeout expires, register throws 'TimeoutX'.
  ------------------------------------------------------------------------
  register :: Con -> JobName -> JobType -> 
                     QName   -> QName   -> 
                     Int -> Int -> IO (StatusCode, Int)
  register c j t o i to me | null j    = return (BadRequest,0)
                           | otherwise =
      let i' = o ++ "/" ++ j ++ "/" ++ i
          hs = [("__type__",    "register"),
                ("__job-type__",    show t),
                ("__job__",              j),
                ("__queue__",            i),
                ("__hb__",         show me),
                ("__channel__",         i')]
       in withWriter c "RegistryW" o  [] [] nobody     $ \w -> 
          withReader c "RegistryR" i' [] [] ignorebody $ \r -> do
            writeQ w nullType hs ()
            mbF <- timeout to $ readQ r 
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
  -- | Disconnect from a registry:
  --   The caller disconnects from a registry
  --   to which it has registered before.
  --   For the case that the registry is not receiving heartbeats
  --   from the caller,
  --   it is essential to unregister, when
  --   the service is no longer provided.
  --   Otherwise, the registry has no way to know
  --   that it should not send requests to this provider anymore.
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * 'JobName': The 'JobName' to unregister;
  --
  --   * 'QName': The registry's registration queue ;
  --
  --   * 'QName': The queue to unregister;
  --
  --   * Int: The timeout in microseconds.
  --
  --  The function returns a 'StatusCode'. 
  --  If 'JobName' is null, the 'StatusCode' will be 'BadRequest'.
  --  If the timeout expires, the function will throw 'TimeoutX'.
  ------------------------------------------------------------------------
  unRegister :: Con -> JobName -> 
                       QName   -> QName -> 
                       Int     -> IO StatusCode
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
  -- | Send heartbeats:
  --
  --   * 'MVar' 'HB': An MVar of type 'HB', this MVar will be used
  --                  to keep track of when the heartbeat has actually to
  --                  be sent.
  --
  --   * 'Writer' (): The writer through which to send the heartbeat;
  --                  The queue name of the writer is the registration queue
  --                  of the registry; note that its type is ():
  --                  heartbeats are empty messages.
  --
  --   * 'JobName': The 'JobName' for which to send heartbeats;
  --
  --   * 'QName': The queue for which to send heartbeats.
  ------------------------------------------------------------------------
  heartbeat :: MVar HB -> Writer () -> JobName -> QName -> IO ()
  heartbeat m w j q | null q    = return ()
                    | otherwise = 
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
  -- | A provider is an opaque data type;
  --   most of its attributes are used only internally by the registry.
  --   Interesting for user applications, however, is the queue
  --   that identifies the provider.
  ------------------------------------------------------------------------
  data Provider = Provider {
                    -- | Queue through which the job is provided 
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
  updOrAddProv :: Bool -> (Provider -> Provider) -> Provider -> 
                  Seq Provider -> Seq Provider
  updOrAddProv add upd p s = 
    case S.viewl s of
      S.EmptyL -> if add then S.singleton p else S.empty
      x :< ss  -> if prvQ x == prvQ p 
                    then upd x <| ss
                    else     x <| updOrAddProv add upd p ss

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
  -- | Registry: An opaque data type
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
                ps = updOrAddProv True (upd p) p $ jobProvs j
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
                             ps = updOrAddProv False (upd now) p 
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
  --                   active provider of a list of providers
  --                   and this provider
  --                   is then sent to the back of the list,
  --                   hence, implementing a balancer.
  --
  --   * Topic: action is applied to all providers,
  --            hence, implementing a publisher.
  --
  --   Parameters:
  --
  --   * 'Registry': The registry to use;
  --
  --   * 'JobName': The job to which to apply the action;
  --
  --   * ('Provider' -> IO ()): The action to apply.
  --
  --   The function returns False iff the requested job is not available
  --   and True otherwise. (Note that a job without providers is removed;
  --   when the function returns True, the job, thus, 
  --   was applied at least once.
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
  -- | Map function of type 
  --
  --   > 'Provider' -> 'Provider'
  --
  --   to all 'Provider's of job 'JobName'
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
  -- | Retrieves /n/ 'Provider's of a certain job;
  --   getProvider works, for all 'JobType's
  --   according to the work balancer logic, /i.e./:
  --   it returns the first n providers of the list for this job
  --   and moves them to the end of the list.
  --   'getProvider' is used, for instance, in the Desk pattern. 
  --
  --   * 'Registry': The registry in use;
  --
  --   * 'JobName': The job for which the caller needs a provider;
  --
  --   * Int: The number /n/ of providers to retrieve; 
  --          if less than /n/ providers are available for this job,
  --          all available providers will be returned,
  --          but no error event is created.
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
  -- | This function shows all jobs with all their providers
  --   in a registry; the function is intended for debugging only.
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
  -- | A registry is used through a function 
  --   that, internally, creates a registry
  --   and defines its lifetime in terms of the scope of an action
  --   passed in to the function:
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * String: Name of the registry used for error handling;
  --
  --   * 'QName': Name of the registration queue.
  --              It is this queue to which 'register'
  --              sends a registration request;
  --
  --   * (Int, Int): Minimal and maximal accepted heartbeat interval;
  --
  --   * 'OnError': Error handler;
  --
  --   * ('Registry' -> IO r): The action that defines 
  --                           the registry's lifetime;
  --                           the result of this action, /r/, 
  --                           is also the result of /withRegistry/.
  ------------------------------------------------------------------------
  withRegistry :: Con -> String -> QName -> (Int, Int)
                      -> OnError -> (Registry -> IO r) -> IO r
  withRegistry c n rq (mn, mx) onErr action = 
    -- always start the reader in the main thread -------------
    -- for if started in the background thread    -------------
    -- the action may send a message              -------------
    -- without the reader having subscribed to its queue ------
    withReader c (n ++ "Reader") rq [] [] ignorebody $ \r -> do
      let nm  = n ++ "Registry"
      reg <- Registry <$> newMVar (Reg nm M.empty)
      withThread (startReg reg r nm) (action reg)
    where startReg reg r nm = 
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
                (ignoreHandler nm onErr)

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
  -- | Get JobQueue
  --   (and throw an exception if at least 
  --    one of the headers does not exist)
  ------------------------------------------------------------------------
  getJobQueue :: Message m -> IO (String, String)
  getJobQueue m = getJobName m >>= \j -> getQueue m >>= \q -> return (j,q)

  ------------------------------------------------------------------------
  -- | Get Message Type from headers
  --   (and throw an exception if the header does not exist)
  ------------------------------------------------------------------------
  getMType :: Message m -> IO String
  getMType = getHeader "__type__" "No message type in headers"

  ------------------------------------------------------------------------
  -- | Get Job name from headers
  --   (and throw an exception if the header does not exist)
  ------------------------------------------------------------------------
  getJobName :: Message m -> IO String
  getJobName = getHeader "__job__" "No job name in headers" 

  ------------------------------------------------------------------------
  -- | Get Reply queue (channel) from headers
  --   (and throw an exception if the header does not exist)
  ------------------------------------------------------------------------
  getChannel :: Message m -> IO String
  getChannel = getHeader "__channel__" "No response q in headers" 

  ------------------------------------------------------------------------
  -- | Get Queue name from headers
  --   (and throw an exception if the header does not exist)
  ------------------------------------------------------------------------
  getQueue :: Message m -> IO String
  getQueue = getHeader "__queue__" "No queue q in headers" 

  ------------------------------------------------------------------------
  -- | Get Job type from headers
  --   (and throw an exception if the header does not exist
  --        or contains an invalid value)
  ------------------------------------------------------------------------
  getJobType :: Message m -> IO JobType
  getJobType m = 
    getHeader "__job-type__" "No job type in headers"  m >>= \x ->
      case readJobType x of
        Nothing -> throwIO $ HeaderX "__job-type__" $
                                     "unknown type: " ++ x 
        Just t  -> return t  

  ------------------------------------------------------------------------
  -- | Get Heartbeat specification from headers
  --   (and throw an exception if the header does not exist
  --        or if its value is not numeric)
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
  -- | Get Status code from headers
  --   (and throw an exception if the header does not exist)
  ------------------------------------------------------------------------
  getSC :: Message m -> IO (Either String StatusCode)
  getSC m = readStatusCode <$> getHeader "__sc__"
                                 "No status code in message" m
                               
  ------------------------------------------------------------------------
  -- | Get Generic function to retrieve a header value
  --   (and throw an exception if the header does not exist):
  --
  --   * String: Key of the wanted header
  --
  --   * String: Error message in case there is no such header
  --
  --   * 'Message' m: The message whose headers we want to search
  ------------------------------------------------------------------------
  getHeader :: String -> String -> Message m -> IO String
  getHeader h e m = case lookup h $ msgHdrs m of
                      Nothing -> throwIO $ HeaderX h e
                      Just v  -> return v

