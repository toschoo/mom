module Config (
                           Config,
                           getBroker, getLogger, getSender,
                           setBroker, setLogger, setSender,
                           getName, setName, getMName, getSharedName,
                           getLogFile, getLogLevel, getLogChan,
                           setLogFile, setLogLevel, 
                           getSndChan,
                           getHost, getPort, getSocks, getCons, 
                           getMCons, getSharedCons, setSharedCons,
                           incCons, decCons,
                           getMaxRcv, setMaxRcv,
                           ChangeAction,
                           mkConfig)
where

  import Types

  import           Control.Concurrent
  import           Control.Exception.Base (SomeException)
  import qualified System.Log.Logger as Log
  import qualified Data.Configurator as Cfg
  import qualified Data.Configurator.Types as CfgT
  import           Data.Text (pack, unpack)

  data Config = EmptyCfg 
                 | MainCfg {
                   cfgBroker :: Config,
                   cfgLogger :: Config,
                   cfgSender :: Config,
                   cfgSubs   :: Config}
                 | BrokerCfg {
                     cfgName    :: String,
                     cfgHost    :: String,
                     cfgPort    :: Int,
                     cfgUser    :: String,
                     cfgSocks   :: Int,
                     cfgCons    :: Int,
                     cfgMax     :: Int,
                     cfgMCons   :: MVar Int,
                     cfgMName   :: MVar String,
                     cfgSndChan :: Chan SubMsg, 
                     cfgLogChan :: Chan LogMsg 
                     -- cfgProts :: [Protocol]
                    }
                  | LoggerCfg {
                      cfgName     :: String,
                      cfgLogFile  :: FilePath,
                      cfgLogLevel :: Log.Priority,
                      cfgLogChan  :: Chan LogMsg
                    }
                  | SenderCfg {
                      cfgName    :: String,
                      cfgSndChan :: Chan SubMsg,
                      cfgLogChan :: Chan LogMsg
                    }
                  | SubCfg {
                      cfgName    :: String,
                      cfgSubSubs :: String}

  getBroker, getLogger, getSender :: Config -> Config
  getBroker EmptyCfg = EmptyCfg
  getBroker cfg      = cfgBroker cfg
  getLogger EmptyCfg = EmptyCfg
  getLogger cfg      = cfgLogger cfg
  getSender EmptyCfg = EmptyCfg
  getSender cfg      = cfgSender cfg

  setBroker :: Config -> Config -> Config
  setBroker _ EmptyCfg = EmptyCfg
  setBroker b cfg      = cfg {cfgBroker = b}

  setLogger :: Config -> Config -> Config
  setLogger _ EmptyCfg = EmptyCfg
  setLogger l cfg      = cfg {cfgLogger = l}

  setSender :: Config -> Config -> Config
  setSender _ EmptyCfg = EmptyCfg
  setSender s cfg      = cfg {cfgSender = s}

  getName :: Config -> String
  getName = cfgName

  getMName :: Config -> MVar String
  getMName = cfgMName

  setName :: String -> Config -> Config
  setName _ EmptyCfg = EmptyCfg
  setName s cfg = cfg {cfgName = s}

  getSharedName :: Config -> IO String
  getSharedName c = 
    let m = getMName c
    in  readMVar m

  getHost :: Config -> String
  getHost = cfgHost

  getPort :: Config -> Int
  getPort = cfgPort

  getSocks :: Config -> Int
  getSocks = cfgSocks

  getCons :: Config -> Int
  getCons = cfgCons

  getMCons :: Config -> MVar Int
  getMCons = cfgMCons

  getSharedCons :: Config -> IO Int
  getSharedCons c  = let m = getMCons c
                     in readMVar m

  setSharedCons :: Int -> Config -> IO Int
  setSharedCons cnt cfg = do
    let m = getMCons cfg
    swapMVar m cnt

  incCons :: Config -> IO ()
  incCons = modifyCons (\c -> return $ c + 1)

  decCons :: Config -> IO ()
  decCons = modifyCons (\c -> return $ c - 1)

  modifyCons :: (Int -> IO Int) -> Config -> IO ()
  modifyCons m cfg = modifyMVar_ (getMCons cfg) m

  getSndChan :: Config -> Chan SubMsg
  getSndChan = cfgSndChan

  getLogFile :: Config -> FilePath
  getLogFile = cfgLogFile

  setLogFile :: FilePath -> Config -> Config
  setLogFile f cfg = cfg {cfgLogFile = f}

  getLogLevel :: Config -> Log.Priority
  getLogLevel = cfgLogLevel

  setLogLevel :: Log.Priority -> Config -> Config
  setLogLevel p cfg = cfg {cfgLogLevel = p}

  getLogChan :: Config -> Chan LogMsg
  getLogChan = cfgLogChan

  getMaxRcv :: Config -> Int
  getMaxRcv = cfgMax

  setMaxRcv :: Int -> Config -> Config
  setMaxRcv m c = c {cfgMax = m}

  type ChangeAction = MVar Config -> IO ()

  mkConfig :: String -> ChangeAction -> ChangeAction -> IO (MVar Config)
  mkConfig f brk l = do
    (cfg, _) <- Cfg.autoReload (CfgT.AutoConfig 10 onErr) [Cfg.Required f]
    m        <- buildConfig EmptyCfg cfg
    v        <- newMVar m
    Cfg.subscribe cfg (Cfg.prefix $ pack "Broker")  (onBrokerChange v brk)
    Cfg.subscribe cfg (Cfg.prefix $ pack "Logging") (onLoggerChange v l)
    return v

  buildConfig :: Config -> CfgT.Config -> IO Config
  buildConfig old cfg = do
    brk <- buildBroker old cfg
    l <- buildLogger old cfg (getLogChan brk)
    snd <- buildSender old cfg (getSndChan brk) (getLogChan brk)
    sub <- buildSubs   old cfg
    return MainCfg {
             cfgBroker = brk,
             cfgLogger = l,
             cfgSender = snd,
             cfgSubs   = sub}

  onErr :: SomeException -> IO ()
  onErr e = putStrLn $ "Error on Configuration: " ++ (show e)

  buildBroker :: Config -> CfgT.Config -> IO Config
  buildBroker old cfg = do
    let b = getBroker old
    n   <- Cfg.lookupDefault (cfgName  b) cfg (pack "Broker.name") 
    h   <- Cfg.lookupDefault (cfgHost  b) cfg (pack "Broker.host") 
    p   <- Cfg.lookupDefault (cfgPort  b) cfg (pack "Broker.port") 
    u   <- Cfg.lookupDefault (cfgUser  b) cfg (pack "Broker.user") 
    s   <- Cfg.lookupDefault (cfgSocks b) cfg (pack "Broker.sockets") 
    c   <- Cfg.lookupDefault (cfgCons  b) cfg (pack "Broker.connections") 
    m   <- Cfg.lookupDefault (cfgMax   b) cfg (pack "Broker.msgsize") 
    snc <- newChan
    lgc <- newChan
    nm  <- newMVar n
    ss  <- newMVar 0
    return BrokerCfg {
             cfgName    = n, 
             cfgHost    = h,
             cfgPort    = p,
             cfgUser    = u,
             cfgSocks   = s,
             cfgCons    = c,
             cfgMax     = m,
             cfgMCons   = ss,
             cfgMName   = nm,
             cfgSndChan = snc,
             cfgLogChan = lgc
           }

  buildLogger :: Config -> CfgT.Config -> Chan LogMsg -> IO Config
  buildLogger old cfg lgc = do
    let l = getLogger old
    n <- Cfg.lookupDefault (cfgName l)            cfg (pack "Broker.name")
    f <- Cfg.lookupDefault (cfgLogFile l)         cfg (pack "Logging.file")
    p <- Cfg.lookupDefault (show $ cfgLogLevel l) cfg (pack "Logging.level")
    return LoggerCfg {
             cfgName     = n,
             cfgLogFile  = f,
             cfgLogLevel = read p,
             cfgLogChan  = lgc}

  buildSender :: Config      -> 
                 CfgT.Config -> 
                 Chan SubMsg -> 
                 Chan LogMsg -> IO Config
  buildSender old cfg snc lgc = do
    let b = getBroker old
    n <- Cfg.lookupDefault (cfgName b) cfg (pack "Broker.name")
    return SenderCfg {
             cfgName    = n,
             cfgSndChan = snc,
             cfgLogChan = lgc}

  buildSubs :: Config -> CfgT.Config -> IO Config
  buildSubs old cfg = do
    let b = getBroker old
    n <- Cfg.lookupDefault (cfgName b) cfg (pack "Broker.name")
    return $ SubCfg (getName b) "unused"

  getValue :: CfgT.Value -> String
  getValue v = case v of
                 CfgT.Bool t   -> show t
                 CfgT.String t -> unpack t
                 CfgT.Number t -> show t
                 CfgT.List  ts -> show ts

  onBrokerChange :: MVar Config -> ChangeAction -> CfgT.ChangeHandler
  onBrokerChange c act k mbV = 
    case mbV of
      Nothing -> return ()
      Just  v -> do
        modifyMVar_ c (\old -> do
          let b = getBroker old
          let t = getValue v
          b' <- case unpack k of
                  "Broker.name"        -> do
                     _ <- swapMVar (cfgMName b) t
                     return b {cfgName  = t}
                  "Broker.host"        -> return b {cfgHost  =        t}
                  "Broker.port"        -> return b {cfgPort  = read $ t}
                  "Broker.user"        -> return b {cfgUser  =        t}
                  "Broker.sockets"     -> return b {cfgSocks = read $ t}
                  "Broker.connections" -> return b {cfgCons  = read $ t}
                  "Broker.msgsize"     -> return b {cfgMax   = read $ t}
                  _                    -> return b 
          return old {cfgBroker = b'})
        act c

  onLoggerChange :: MVar Config -> ChangeAction -> CfgT.ChangeHandler
  onLoggerChange c act k mbV =
    case mbV of
      Nothing -> return ()
      Just v  -> do
        let t = getValue v
        modifyMVar_ c (\old -> do
          let l = getLogger old
          let l' = case unpack k of 
                     "Logging.file"  -> l {cfgLogFile  =        t}
                     "Logging.level" -> l {cfgLogLevel = read $ t}
                     _               -> l
          return old {cfgLogger = l'})
        act c
