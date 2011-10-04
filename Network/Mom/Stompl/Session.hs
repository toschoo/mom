module Session (
                handleSession
               )
where

  import           Network.Mom.Stompl.Frame
  import           Network.Mom.Stompl.Parser
  import           Types
  import           Sender (bookFrame, unRegisterCon)
  import           Config
  import           Logger

  import qualified Network.Socket        as S
  import           Network.Socket.ByteString (send, recv)

  import qualified Data.Attoparsec as A

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U

  import           Control.Concurrent
  import           Control.Exception (finally)
  import           Control.Monad.State

  -- Exception and SessionError
  -- Exception for I/O Error:
  -- terminate session
  -- SessionError for errors in protocol
  -- in this case send ERROR frame
  data SessionError = SessionError {
                        seMsg  :: String,
                        seBody :: B.ByteString
                      }
    deriving (Show, Read, Eq)

  data Connection = Connection {
                      conId   :: Int,
                      conName :: String,
                      conCon  :: Bool,
                      conSock :: S.Socket,
                      conCfg  :: Config,
                      conPend :: Maybe Result}

  getLog :: Connection -> (LogMsg -> IO())
  getLog = writeLog . conCfg

  buildName :: String -> Int -> String
  buildName b cid = b ++ "." ++ "Session-" ++ (show cid)

  type Result = A.Result Frame

  type Session a = StateT Connection IO a

  handleSession :: Int -> Config -> S.Socket -> IO ()
  handleSession cid cfg s = do
    finally handleSession'
            (cleanSession cid cfg)
    where handleSession' = do
            let nm = buildName (getName cfg) cid
            logIO cfg nm INFO "New Connection"
            m <- recv s (getMaxRcv cfg)
            if B.length m == 0
              then return ()
              else  
                case stompAtOnce m of
                  Left e  -> handleError cfg nm cid $ 
                               SessionError "Parse Error" (B.pack e)
                  Right f -> do
                    eiC <- getCon cid s nm cfg f
                    case eiC of
                      Left  e -> handleError cfg nm cid e
                      Right c -> 
                        evalStateT handleConnection c

  cleanSession :: Int -> Config -> IO ()
  cleanSession  cid cfg = do
    decCons         cfg
    dbgClean    cid cfg
    cleanSender cid cfg

  cleanSender :: Int -> Config -> IO ()
  cleanSender cid cfg = do
    unRegisterCon (writeSender cfg) cid

  dbgClean :: Int -> Config -> IO ()
  dbgClean cid cfg = do
    let nm = getMName cfg
    n <- readMVar nm
    let ln  = buildName n cid
    t <- getSharedCons cfg
    logIO cfg ln DEBUG ("Cleaning Session: " ++ (show t))
  
  logS :: Priority -> String -> Session ()
  logS p s = do
    c <- get
    liftIO $ logX (getLog c) (conName c) p s 
  
  logIO :: Config -> String -> Priority -> String -> IO ()
  logIO c n p s = do
    logX (writeLog c) n p s

  updConnection :: Session ()
  updConnection = do
    c <- get
    let mn = getMName $ conCfg c
    let n  = getName  $ conCfg c
    n' <- liftIO $ readMVar mn
    if n' /= n 
      then do
         let cfg = setName n' $ conCfg c
         put c {conCfg  = cfg,
                conName = buildName n' (conId c)}
      else return ()

  handleConnection :: Session ()
  handleConnection = do
    updConnection
    c <- get
    if conCon c 
      then do 
        let s  = conSock c
        let mx = getMaxRcv $ conCfg c
        m <- liftIO $ catch (recv s mx)
                            (onError (conCfg c) (conName c) s)
        if B.length m == 0 
          then do
            logS INFO "Peer disconnected"
            disconnectMe 
          else do
            p <- case conPend c of
                   Nothing -> do
                     logS DEBUG "Start parsing message"
                     return $ A.parse stompParser

                   Just r  -> do 
                     logS DEBUG "Start parsing message"
                     return $ A.feed  r  
            handleInput p m
      else 
        logS INFO "Terminating Connection"
      where onError cfg nm s e = do
              c <- S.sIsConnected s
              if not c
                then do
                  logIO cfg nm INFO "Peer disconnected"
                  return B.empty
                else do
                  logIO cfg nm WARNING $ "Socket Error: " ++ (show e)
                  return B.empty

  handleInput :: (B.ByteString -> Result) -> B.ByteString -> Session ()
  handleInput p m = 
    case p m of
      A.Fail str ctx e -> do
        c <- get
        let s = (U.toString m) ++ (U.toString str) ++ ": " ++ e
        logS WARNING ("Error: " ++ s)
        liftIO $ handleError (conCfg c) (conName c) (conId c) 
                             (SessionError "Parse Error" $ B.pack e)
      A.Partial r      -> do 
        logS DEBUG "Got partial result"
        c <- get
        put c {conPend = Just $ A.Partial r} 
        handleConnection 
      A.Done str f     -> do
        logS DEBUG "Got complete result"
        handleFrame f
        c <- get
        if conCon c && B.length str > 0 
          then do
            updConnection
            handleInput p str
          else do
            c' <- get
            put c' {conPend = Nothing}
            handleConnection 
        
  handleFrame :: Frame -> Session ()
  handleFrame f = do
    let t = typeOf f 
    if t == Disconnect 
      then disconnect f 
      else do
        c <- get
        liftIO $ bookFrame (writeSender $ conCfg c) (conId c) f

  disconnectMe :: Session ()
  disconnectMe = 
    case mkDisFrame [] of
      Left e -> 
        logS CRITICAL $ "Can't create DisFrame: " ++ (show e)
      Right f -> do
        logS DEBUG "Disconnecting"
        disconnect f

  disconnect :: Frame -> Session ()
  disconnect _ = do
    c <- get
    put c {conCon = False}
      
  handleError :: Config -> String -> Int -> SessionError -> IO ()
  handleError cfg nm cid e = do
    logIO cfg nm NOTICE $ "Error: " ++ (seMsg e)
    let b   = seBody e
    let eiF = mkErrFrame [mkMsgHdr $ seMsg e] (B.length b) b 
    case eiF of
      Left  er -> 
        logIO cfg nm CRITICAL ("Can't send Error Frame: " ++ er)
      Right f -> do
        bookFrame (writeSender cfg) cid f

  getCon :: Int -> S.Socket -> String -> Config -> Frame -> IO (Either SessionError Connection)
  getCon cid s nm cfg f = 
    case typeOf f of
      Connect -> handleConnect cid s nm cfg f
      _       -> 
        return $ Left $ SessionError "No Connection" B.empty

  handleConnect :: Int -> S.Socket  -> 
                   String -> Config -> 
                   Frame -> IO (Either SessionError Connection)
  handleConnect cid s nm cfg f = do
    let mbC = conToCond myVersion "1" myBeat f -- mkCondFrame [mkSesHdr "1"]
                                               -- Create unique Session ID
    case mbC of 
      Nothing -> 
        return $ Left $ SessionError "Not a Connection Frame!" B.empty
      Just c -> do
        let m = putFrame c
        l <- send s m -- should go via Sender?
        if l /= B.length m
          then 
            -- throw
            return $ Left $ SessionError "Can't send!" B.empty
          else 
            return $ Right $ Connection {
                               conId   = cid, 
                               conName = nm,
                               conCon  = True,
                               conSock = s, 
                               conCfg  = cfg,
                               conPend = Nothing}

