module Session (startSession)
where

  import           Types
  import           Config
  import           Factory
  import qualified Socket as S
  import           State  
  import           Queue 
  import           Fifo hiding  (empty)
  import qualified Fifo as FIFO (empty) 
  import           Sender
  import           Exception

  import qualified Network.Mom.Stompl.Frame  as F
  import qualified Network.Socket as Sock

  import           Control.Concurrent
  import           Prelude hiding (catch)
  import           Control.Exception hiding (handle)
  import           Control.Monad
  import           Control.Monad.State
  import           Control.Applicative ((<$>))

  import           Data.List  (insert, delete, find)
  import           Data.Maybe (catMaybes)
  import qualified Data.ByteString           as B

  data Connection = Connection {
                      conId   :: ConId,
                      conName :: String,
                      -- version
                      -- heartbeat
                      conUp   :: Bool,
                      conErr  :: String,
                      conRc   :: S.Receiver,
                      conCon  :: Con}

  getSock :: Connection -> Sock.Socket
  getSock = conSock . conCon

  getWriter :: Connection -> S.Writer
  getWriter = conWr . conCon

  getReader :: Connection -> S.Receiver
  getReader = conRc

  type Session a = StateT Connection IO a

  startSession :: Sock.Socket -> IO ThreadId
  startSession s = forkIO (initSession s)
  
  initSession :: Sock.Socket -> IO ()
  initSession s = 
    catch (do cid <- newConId
              report (mkLog cid) INFO "Starting Session" 
              finally (initSession' cid)
                      (cleanSession cid s))
          (\e -> do let _ = id (e::SomeException)
                    return ()) -- ignore
    where initSession' cid = do
            rc  <- S.initReceiver
            wr  <- S.initWriter
            eiF <- S.receive rc s 1024 -- from config
            case eiF of
              Left  e -> 
                reportError (mkLog cid) NOTICE 
                            ProtocolException $ 
                            "Cannot parse frame: " ++ show e
              Right f -> do
                let c = Connection {
                          conId   = cid,
                          conName = "Session-" ++ show cid, -- config
                          conUp   = False,
                          conErr  = "",
                          conRc   = rc,
                          conCon  = newCon s wr}
                c' <- handleConFrame c f
                if not (conUp c') 
                  then reportError (mkLog cid) 
                                   NOTICE ConnectException (conErr c')
                  else evalStateT handleConnection c' -- start heartbeat

  handleConFrame :: Connection -> F.Frame -> IO Connection
  handleConFrame c f = 
    case F.typeOf f of
      -- server desc, heartbeat, versions from config
      F.Connect -> case F.conToCond "Test/1.1" 
                                    (show $ conId c) 
                                    (0,0) [(1,0), (1,1)] f of 
                     Nothing -> return c {conErr = 
                                  "Cannot convert to Connected: " ++
                                  F.toString f} -- log error
                     Just f' -> do
                       catch (S.send (getWriter c) (getSock c) f')
                             (\e -> reportError (mkLog $ conId c) NOTICE 
                                                SocketException $
                                                show (e::SomeException)) 
                       return c {conUp = True} -- heartbeat, version
      _         -> return c -- log error ?

  cleanSession :: ConId -> Sock.Socket -> IO ()
  cleanSession cid s = do S.disconnect s -- decrement no of active sessions
                          report (mkLog cid) INFO "Connection terminated"

  handleConnection :: Session ()
  handleConnection = forever $ do
    c   <- get
    eiF <- liftIO $ S.receive (getReader c) (getSock c) 1024
    case eiF of
      Left  e -> liftIO $ reportError (mkLog $ conId c) 
                                      NOTICE ConnectException $
                                      "Cannot receive: " ++ e
      Right f -> handleFrame f

  handleFrame :: F.Frame -> Session ()
  handleFrame f =
    case F.typeOf f of
      F.Disconnect  -> undefined -- raise disconnect error
      F.Send        -> haveItMadeTx f
      F.Subscribe   -> haveItMade   f -- handle, Tx! 
      F.Unsubscribe -> haveItMade   f -- handle, Tx! 
      F.Ack         -> haveItMadeTx f -- handle, Tx! 
      F.Nack        -> haveItMadeTx f -- handle, Tx! 
      F.HeartBeat   -> undefined -- update heartbeat in Connection
      F.Begin       -> undefined -- startTx
      F.Commit      -> undefined -- endTx
      F.Abort       -> undefined -- endTx
      _             -> do
        cid <- conId <$> get
        liftIO $ reportError (mkLog cid) NOTICE ProtocolException $
                             "Unexpected Frame: " ++ F.toString f

  haveItMadeTx :: F.Frame -> Session ()
  haveItMadeTx f 
    | null (F.getTrans f) = haveItMade f
    | otherwise           = handleTx   f

  haveItMade :: F.Frame -> Session ()
  haveItMade f = get >>= \c -> liftIO $ handle (conId c) f (conCon c)

  handleTx :: F.Frame -> Session ()
  handleTx f = undefined

  mkLog :: ConId -> String
  mkLog cid = "Session-" ++ show cid

