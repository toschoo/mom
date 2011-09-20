module Server (startServer)
where

  import           Types 
  import           Logger
  import           Config  
  import           Session as Ses
  import           Sender
 
  import qualified Network.Socket         as S 
  import           Network.BSD (getProtocolNumber)
  import           Control.Concurrent

  roll :: Int
  roll = 999999999

  data Server = Server {
                  srvSock  :: S.Socket,
                  srvCfg   :: Config,
                  srvCount :: Int}

  startServer :: FilePath -> IO ()
  startServer f = 
    S.withSocketsDo $ do
      putStrLn "Starting"
      v    <- mkConfig f onBrokerChange onLoggerChange
      cfg  <- readMVar v
      let brk = getBroker cfg
      let port = (fromIntegral $ getPort brk)::S.PortNumber
      srv  <- mkServer cfg (getHost brk) port
      mkLogger srv
      mkSender srv
      S.listen (srvSock srv) (getSocks brk)
      listen srv
   
  listen :: Server -> IO ()
  listen srv = do
    let s    = srvSock srv
    (s', _) <- S.accept s
    srv' <- updServer srv
    mx   <- maxReached srv
    if mx then listen srv'
      else do  startSession srv' s' 
               listen srv'

  maxReached :: Server -> IO Bool
  maxReached srv = do
    cn <- getSharedCons $ brk srv
    if cn >= (mx srv) then return True else return False
    where brk = getBroker . srvCfg 
          mx  = getCons . brk

  startSession :: Server -> S.Socket -> IO ()
  startSession srv s = do
    let cid  = srvCount srv
    let brk  = getBroker  $ srvCfg srv
    registerCon (writeSender brk) cid s
    _ <- forkIO (handleSession cid brk s)
    return ()

  updServer :: Server -> IO Server
  updServer srv = changeName srv >>= changeCount

  changeName :: Server -> IO Server
  changeName srv = do
    let cfg = srvCfg srv
    let brk = getBroker cfg
    let n   = getName brk
    n' <- getSharedName brk
    if n' /= n 
      then do
        let brk' = setName n' brk 
        return srv {srvCfg = setBroker brk' cfg}
      else 
        return srv

  changeCount :: Server -> IO Server
  changeCount srv = do
    incCons $ cfg srv
    return srv {srvCount = if srvCount srv == roll 
                             then 1 
                             else 1 + srvCount srv}
    where cfg = getBroker . srvCfg

  mkSocket :: String -> S.PortNumber -> IO S.Socket
  mkSocket h p = do
    proto <- getProtocolNumber "tcp"
    sock  <- S.socket S.AF_INET S.Stream proto
    addr  <- S.inet_addr h
    S.bindSocket sock (S.SockAddrInet p addr)
    return sock

  mkServer :: Config -> String -> S.PortNumber -> IO Server
  mkServer c h p = do
    sock <- mkSocket h p
    return Server {
             srvSock  = sock,
             srvCfg   = c,
             srvCount = 1}

  mkSender :: Server -> IO ()
  mkSender srv = do
    _ <- forkIO (startSender $ getSender $ srvCfg srv)
    return ()

  mkLogger :: Server -> IO ()
  mkLogger srv = do
    _ <- forkIO (startLogger $ getLogger $ srvCfg srv)
    return ()

  onBrokerChange :: ChangeAction
  onBrokerChange v = do
    modifyMVar_ v (\cfg -> do
      let b = getBroker cfg
      let l = getLogger cfg
      let s = getSender cfg
      let n  = getName b
      if n /= (getName l) ||
         n /= (getName s) 
        then do
          let l' = setName n l
          let s' = setName n s 
          writeSender s' (CfgSndMsg n)
          return $ (setLogger l' . setSender s') cfg
        else 
          return cfg)

  onLoggerChange :: ChangeAction
  onLoggerChange v = do
    cfg <- readMVar v
    let l    = getLogger cfg
    let f    = getLogFile  l
    let p    = getLogLevel l
    let n    = getName     l
    let wLog = writeLog $ getBroker cfg
    wLog (CfgLogMsg n f p) 
