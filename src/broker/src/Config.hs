{-# OPTIONS -fglasgow-exts -fno-cse #-}
module Config 
where

  import           Types

  import qualified Network.Mom.Stompl.Frame as F

  import           Control.Exception 
  import           Control.Concurrent
  import           Control.Applicative ((<$>))

  import           System.IO
  import           System.IO.Unsafe

  import qualified Data.Configurator       as C
  import qualified Data.Configurator.Types as CT
  import           Data.Text (pack, unpack)
  import           Data.Char (toLower)
  import           Data.Typeable (Typeable)

  data Config = Cfg {
                  cfgSnd      :: Chan SndRequest,
                  cfgLog      :: Chan LogRequest,
                  cfgName     :: F.SrvDesc,       -- changeable
                  cfgVers     :: [F.Version],     -- changeable
                  cfgBeat     :: F.Heart,         -- changeable
                  cfgHost     :: String,
                  cfgPort     :: Int,
                  cfgPend     :: Int,             -- changeable
                  cfgCons     :: Int,             -- changeable
                  cfgPacket   :: Int,             -- changeable
                  cfgThreads  :: Int,             -- changeable
                  cfgParallel :: Bool,            -- changeable
                  cfgCpus     :: Int,             -- changeable
                  cfgResend   :: Int,             -- changeable
                  -- cfgPattern  :: Regex,        -- changealbe
                  -- cfgStore :: Regex,           -- changealbe
                  -- cfgPersistent :: Regex,
                  cfgDynamic  :: Bool,            -- changeable
                  cfgUsrs     :: [User],          -- changeable
                  cfgLogger   :: Log,             -- changeable
                  cfgLogType  :: LogType,
                  cfgLogFormat:: LogFormat,
                  cfgLogFile  :: FilePath,
                  cfgLogHost  :: String,
                  cfgLogPort  :: Int,
                  cfgLogLevel :: Priority
                  -- cfgAuth     :: Regex
                }

  handle :: ConId -> F.Frame -> Con -> IO ()
  handle cid f c = withMVar _cfg sendReq
    where sendReq cfg = writeChan (cfgSnd cfg) (FrameReq cid f c)

  waitSndRequest :: IO SndRequest
  waitSndRequest = readMVar _cfg >>= \cfg -> readChan (cfgSnd cfg)

  waitLogRequest :: IO LogRequest
  waitLogRequest = readMVar _cfg >>= \cfg -> readChan (cfgLog cfg)

  putLogRequest :: LogRequest -> IO ()
  putLogRequest l = readMVar _cfg >>= \cfg -> writeChan (cfgLog cfg) l

  getCfgName :: IO String
  getCfgName = F.srvToStr <$> getCfg cfgName

  getCfgVers :: IO [F.Version]
  getCfgVers = getCfg cfgVers

  getCfgBeat :: IO F.Heart
  getCfgBeat = getCfg cfgBeat

  getCfgHost :: IO String
  getCfgHost = getCfg cfgHost

  getCfgPort :: IO Int
  getCfgPort = getCfg cfgPort

  getCfgPend :: IO Int
  getCfgPend = getCfg cfgPend

  getCfgCons :: IO Int
  getCfgCons = getCfg cfgCons
 
  getCfgPacket :: IO Int
  getCfgPacket = getCfg cfgPacket

  getCfgThreads :: IO Int
  getCfgThreads = getCfg cfgThreads

  getCfgParallel :: IO Bool
  getCfgParallel = getCfg cfgParallel

  getCfgCpus :: IO Int
  getCfgCpus = getCfg cfgCpus

  getCfgResend :: IO Int
  getCfgResend = getCfg cfgResend

  getCfgDynamic :: IO Bool
  getCfgDynamic = getCfg cfgDynamic

  getCfgLogType  :: IO LogType
  getCfgLogType  = getCfg cfgLogType

  getCfgLogLevel :: IO Priority
  getCfgLogLevel = getCfg cfgLogLevel

  getCfgLogFormat :: IO LogFormat
  getCfgLogFormat = getCfg cfgLogFormat

  getCfgLogFile :: IO FilePath
  getCfgLogFile = getCfg cfgLogFile

  getCfgLogger :: IO Log
  getCfgLogger = getCfg cfgLogger

  getCfg :: (Config -> a) -> IO a
  getCfg get = withMVar _cfg $ \c -> return $ get c

  data User = Usr {
                usrName :: String,
                usrCons :: Int,
                usrPacket :: Int,
                usrDynamic :: Bool
                -- usrTo     :: Regex,
                -- usrSub    :: Regex
              }

  data ConfigException = ConfigException String
    deriving (Show, Read, Typeable, Eq)

  instance Exception ConfigException

  throwConfig :: String -> IO a
  throwConfig = throwIO . ConfigException 

  {-# NOINLINE _cfg #-}
  _cfg :: MVar Config
  _cfg = unsafePerformIO newEmptyMVar 

  initCfg :: Config -> IO ()
  initCfg = putMVar _cfg 

  mkConfig :: FilePath -> IO ()
  mkConfig f = do
    s <- newChan
    l <- newChan
    if null f 
      then loadDefault   s l
      else loadConfig  f s l 

  loadDefault :: Chan SndRequest -> Chan LogRequest -> IO ()
  loadDefault s l = 
    initCfg Cfg {
      cfgSnd      = s,
      cfgLog      = l,
      cfgName     = F.strToSrv "NoName",
      cfgVers     = [(1,1), (1,0)], 
      cfgBeat     = (500,500),
      cfgHost     = "localhost",     
      cfgPort     = 61618,
      cfgPend     = 32,
      cfgCons     = 1000,
      cfgPacket   = 1024,
      cfgThreads  = 1000,
      cfgParallel = True,
      cfgCpus     = 2,
      cfgResend   = 5,
      -- cfgPattern  = thePattern,
      cfgDynamic  = True,
      cfgUsrs     = [],
      cfgLogger   = StdLog stderr,
      cfgLogType  = StdErr,
      cfgLogFormat = Native,
      cfgLogFile  = "",
      cfgLogHost  = "",
      cfgLogPort  = 0,
      cfgLogLevel = INFO
    }

  loadConfig :: FilePath -> Chan SndRequest -> Chan LogRequest -> IO ()
  loadConfig f s l = do
    (c, tid) <- C.autoReload (CT.AutoConfig 10 onErr) [CT.Required f]

    theName     <- F.strToSrv <$> C.lookupDefault "NoName"    
                                    c (pack "Broker.name")
    theVersions <- valToVers  <$> C.lookupDefault "1.1" 
                                    c (pack "Broker.versions")
    theBeat     <- C.lookupDefault (500,500)   c (pack "Broker.beat")
    theHost     <- C.lookupDefault "localhost" c (pack "Broker.host")
    thePort     <- C.lookupDefault 61618       c (pack "Broker.port")
    theSocks    <- C.lookupDefault 32          c (pack "Broker.sockets")
    theCons     <- C.lookupDefault 0           c (pack "Broker.connections")
    thePacket   <- C.lookupDefault 1024        c (pack "Broker.packetsize")
    theThreads  <- C.lookupDefault 1024        c (pack "Queues.qsPerThread")
    theParallel <- C.lookupDefault True        c (pack "Queues.parallel")
    theCpus     <- C.lookupDefault 2           c (pack "Queues.cpus")
    theResend   <- C.lookupDefault 5           c (pack "Queues.resend")
    theDynamic  <- C.lookupDefault True        c (pack "Queues.dynamic")
    theLogType  <- C.lookupDefault "stderr"    c (pack "Logging.type")
    theLogFormat <- C.lookupDefault "native"    c (pack "Logging.format")
    theLogFile  <- C.lookupDefault ""          c (pack "Logging.file")
    theLogHost  <- C.lookupDefault ""          c (pack "Logging.host")
    theLogPort  <- C.lookupDefault 0           c (pack "Logging.port")
    theLogLevel <- C.lookupDefault "INFO"      c (pack "Logging.level")
    theLog      <- buildLog c

    subscribe c

    initCfg Cfg {
              cfgSnd      = s,
              cfgLog      = l,
              cfgName     = theName,
              cfgVers     = theVersions, 
              cfgBeat     = theBeat,
              cfgHost     = theHost,     
              cfgPort     = thePort,
              cfgPend     = theSocks,
              cfgCons     = theCons,
              cfgPacket   = thePacket,
              cfgThreads  = theThreads,
              cfgParallel = theParallel,
              cfgCpus     = theCpus,
              cfgResend   = theResend,
              -- cfgPattern  = thePattern,
              cfgDynamic  = theDynamic,
              cfgUsrs     = [],
              cfgLogger   = theLog,
              cfgLogType  = read theLogType,
              cfgLogFormat = read theLogFormat,
              cfgLogFile  = theLogFile,
              cfgLogHost  = theLogHost,
              cfgLogPort  = theLogPort,
              cfgLogLevel = parsePrio theLogLevel
            }

  buildLog :: CT.Config -> IO Log
  buildLog c = do
    theType <- (map toLower) <$> C.lookupDefault "stderr" c (pack "Logging.type")
    case theType of
      "file"   -> do 
         theFile <- C.lookup c (pack "Logging.file") 
         case theFile of
           Nothing -> throwConfig "File Logger without file!"
           Just f  -> return $ FileLog f
      "server" -> do 
        theHost <- C.lookup c (pack "Logging.host")
        thePort <- C.lookup c (pack "Logging.port")
        case theHost of
          Nothing -> throwConfig "Server Logger without host!"
          Just h  -> case thePort of
                       Nothing -> throwConfig "Server Logger without port!"
                       Just p  -> return $ SrvLog h (read p)
      "stderr" -> return $ StdLog stderr
      "stdout" -> return $ StdLog stdout

  mkLogger :: Config -> Log
  mkLogger c = case cfgLogType c of
                 StdErr -> StdLog stderr
                 StdOut -> StdLog stdout
                 File   -> FileLog (cfgLogFile c)
                 Server -> SrvLog  (cfgLogHost c)
                                   (cfgLogPort c)
 
  subscribe :: CT.Config -> IO ()
  subscribe c = do
    C.subscribe c (C.prefix $ pack "Broker" ) (change onBroker)
    C.subscribe c (C.prefix $ pack "Queues" ) (change onBroker)
    C.subscribe c (C.prefix $ pack "Logging") (change onLogger)

  change :: (String -> String -> Config -> IO Config) -> CT.ChangeHandler
  change onChange k mbV =
    case mbV of
      Nothing -> return ()
      Just v  -> modifyMVar_ _cfg $ \c -> onChange (unpack k) (getValue v) c

  onBroker :: String -> String -> Config -> IO Config
  onBroker k v c = return $
    case k of 
      "Broker.name"        -> c {cfgName = F.strToSrv v}
      "Broker.versions"    -> case F.valToVers v of 
                                Nothing -> c
                                Just vs -> c {cfgVers = vs}
      "Broker.heartbeat"   -> case F.valToBeat v of
                                Nothing -> c
                                Just b  -> c {cfgBeat = b}
      "Broker.sockets"     -> if F.numeric v 
                                then c {cfgPend = read v}
                                else c
      "Broker.connections" -> if F.numeric v
                                then c {cfgCons = read v}
                                else c
      "Broker.packetsize"  -> if F.numeric v
                                then c {cfgPacket = read v}
                                else c
      "Queues.qsPerThread" -> if F.numeric v
                                then c {cfgThreads = read v}
                                else c
      "Queues.parallel"    -> if isBool v 
                                then c {cfgParallel = read v}
                                else c 
      "Queues.cpus"        -> if F.numeric v
                                then c {cfgCpus = read v}
                                else c
      "Queues.resend"      -> if F.numeric v
                                then c {cfgResend = read v}
                                else c
      -- "Queues.one"         ->
      -- "Queues.store"       ->
      -- "Queues.persistent"  ->
      "Queues.dynamic"     -> if isBool v
                                then c {cfgDynamic = read v}
                                else c
      _                    -> c

  onLogger :: String -> String -> Config -> IO Config
  onLogger k v c = if consistent c'
                     then writeChan (cfgLog c') LogCfgReq >> 
                          return c' {cfgLogger = mkLogger c'}
                     else return c

    where consistent cfg = case cfgLogType cfg of 
                             StdErr -> True
                             StdOut -> True
                             File   -> if null (cfgLogFile cfg) 
                                         then False 
                                         else True 
                             Server -> if null (cfgLogHost cfg) then False
                                         else if cfgLogPort cfg <= 0 
                                                then False
                                                else True
          c' = case k of
               "Logging.type"       -> if isLogType v
                                         then c {cfgLogType = read v}
                                         else c
               "Logging.format"     -> if isLogFormat v
                                         then c {cfgLogFormat = read v}
                                         else c
               "Logging.file"       -> c {cfgLogFile = v}
               "Logging.host"       -> c {cfgLogHost = v}
               "Logging.port"       -> if F.numeric v
                                         then c {cfgLogPort = read v}
                                         else c
               "Logging.level"      -> if isPrio v
                                         then c {cfgLogLevel = read v}
                                         else c
               _                    -> c

  valToVers :: String -> [F.Version]
  valToVers s = case F.valToVers s of
                  Nothing -> error $ "Cannot parse versions '" ++ s ++ "'"
                  Just vs -> vs

  onErr :: SomeException -> IO ()
  onErr e = putStrLn $ "Panic! Cannot reload configuration: " ++ show e

  isBool :: String -> Bool
  isBool s = case s of
               "True"  -> True
               "False" -> True
               _       -> False

  getValue :: CT.Value -> String
  getValue v = case v of
                 CT.Bool t   -> show   t
                 CT.String t -> unpack t
                 CT.Number t -> show   t
                 CT.List  ts -> show   ts
  
