module Protocol (Connection, mkConnection, 
                 conBeat, getVersion,
                 getSock, getWr, getRc,
                 connected, getErr, conMax,
                 connect, disconnect,
                 Subscription, mkSub,
                 subscribe, unsubscribe,
                 begin, commit, abort, sendBeat,
                 Message(..), mkMessage,
                 MsgId(..),
                 send, ack, nack)
                 
                 
where

  import qualified Socket  as S
  import qualified Factory as Fac
  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Client.Exception
  import           Network.Socket (Socket)

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.Maybe (fromMaybe)

  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, catch, SomeException)
  import           Control.Applicative ((<$>))
  import           Codec.MIME.Type as Mime (Type) 

  defVersion :: F.Version
  defVersion = (1,0)

  data Connection = Connection {
                       conAddr :: String,
                       conPort :: Int,
                       conVers :: [F.Version],
                       conBeat :: F.Heart,
                       conSrv  :: String,
                       conSes  :: String,
                       conUsr  :: String,
                       conPwd  :: String,
                       conMax  :: Int,
                       conSock :: Maybe Socket,
                       conRcv  :: Maybe S.Receiver,
                       conWrt  :: Maybe S.Writer,
                       conErr  :: Int,
                       conErrM :: String, -- connection error
                       conTcp  :: Bool,
                       conBrk  :: Bool}

  data Subscription = Sub {
                        subId   :: Fac.Sub,
                        subName :: String,
                        subMode :: F.AckMode}
    deriving (Show)

  mkSub :: Fac.Sub -> String -> F.AckMode -> Subscription
  mkSub sid qn am = Sub {
                      subId   = sid,
                      subName = qn,
                      subMode = am}

  data MsgId = MsgId String | NoMsg
    deriving (Eq)

  instance Show MsgId where
    show (MsgId s) = s
    show (NoMsg)   = ""

  ------------------------------------------------------------------------
  -- | Any content received from a queue
  --   is wrapped in a message.
  --   It is, in particular, the return value of /readQ/.
  ------------------------------------------------------------------------
  data Message a = Msg {
                     -- | The message Identifier
                     msgId   :: MsgId,
                     -- | The subscription
                     msgSub  :: Fac.Sub,
                     -- | The destination
                     msgDest :: String,
                     -- | The Stompl headers
                     --   that came with the message
                     msgHdrs :: [F.Header],
                     -- | The /MIME/ type of the 
                     --   encoded content
                     msgType :: Mime.Type,
                     -- | The length of the 
                     --   encoded content
                     msgLen  :: Int,
                     -- | The transaction, in which 
                     --   the message was received
                     msgTx   :: Fac.Tx,
                     -- | The encoded content             
                     msgRaw  :: B.ByteString,
                     -- | The content             
                     msgCont :: a}
  
  mkMessage :: MsgId -> Fac.Sub -> String -> 
               Mime.Type -> Int -> Fac.Tx -> 
               B.ByteString -> a -> Message a
  mkMessage mid sub dst typ len tx raw cont = Msg {
                                          msgId   = mid,
                                          msgSub  = sub,
                                          msgDest = dst,
                                          msgHdrs = [], 
                                          msgType = typ, 
                                          msgLen  = len, 
                                          msgTx   = tx,
                                          msgRaw  = raw,
                                          msgCont = cont}

  mkConnection :: String -> Int -> Int -> String -> String -> [F.Version] -> F.Heart -> Connection
  mkConnection host port mx usr pwd vers beat = 
    Connection {
       conAddr = host,
       conPort = port,
       conVers = vers,
       conBeat = beat,
       conSrv  = "",
       conSes  = "",
       conUsr  = usr, 
       conPwd  = pwd,
       conMax  = mx,
       conSock = Nothing,
       conRcv  = Nothing,
       conWrt  = Nothing,
       conErr  = 0,
       conErrM = "",
       conTcp  = False,
       conBrk  = False}

  incompleteErr :: String
  incompleteErr = "uncomplete Connection touched!"

  getSock :: Connection -> Socket
  getSock = fromMaybe (error incompleteErr) . conSock 

  getWr :: Connection -> S.Writer
  getWr = fromMaybe (error incompleteErr) . conWrt

  getRc :: Connection -> S.Receiver
  getRc = fromMaybe (error incompleteErr) . conRcv

  connected :: Connection -> Bool
  connected c = conTcp c && conBrk c

  getErr :: Connection -> String
  getErr = conErrM

  getVersion :: Connection -> F.Version
  getVersion c = if null (conVers c) 
                   then defVersion
                   else head $ conVers c

  connect :: String -> Int -> Int -> String -> String -> [F.Version] -> F.Heart -> IO Connection
  connect host port mx usr pwd vers beat = do
    let c = mkConnection host port mx usr pwd vers beat
    eiS <- catch (Right <$> S.connect host port)
                 (\e -> return $ Left $ show (e::SomeException))
    case eiS of
      Left  e -> return c {conErrM = e}
      Right s ->
        connectBroker mx vers beat c {conSock = Just s, conTcp = True}

  disconnect :: Connection -> IO Connection
  disconnect c 
    | not (connected c) = return c {conErrM = "Not connected!"}
    | conBrk c          = case mkDiscF "" of
                            Left  e -> return c {
                                         conErrM = 
                                           "Cannot create Frame: " ++ e}
                            Right f -> do
                              S.send (getWr c) (getSock c) f 
                              disc c
    | otherwise         = disc c

  begin :: Connection -> String -> String -> IO ()
  begin c tx receipt = sendFrame c tx receipt [] mkBeginF

  commit :: Connection -> String -> String -> IO ()
  commit c tx receipt = sendFrame c tx receipt [] mkCommitF

  abort :: Connection -> String -> String -> IO ()
  abort c tx receipt = sendFrame c tx receipt [] mkAbortF

  ack :: Connection -> Message a -> String -> IO ()
  ack c m receipt = sendFrame c m receipt []  (mkAckF True)

  nack :: Connection -> Message a -> String -> IO ()
  nack c m receipt = sendFrame c m receipt [] (mkAckF False)

  subscribe :: Connection -> Subscription -> String -> [F.Header] -> IO ()
  subscribe c sub receipt hs = sendFrame c sub receipt hs mkSubF

  unsubscribe :: Connection -> Subscription -> String -> [F.Header] -> IO ()
  unsubscribe c sub receipt hs = sendFrame c sub receipt hs mkUnSubF

  send :: Connection -> Message a -> String -> [F.Header] -> IO ()
  send c msg receipt hs = sendFrame c msg receipt hs mkSendF

  sendBeat :: Connection -> IO ()
  sendBeat c = sendFrame c () "" [] (\_ _ _ -> Right F.mkBeat)

  sendFrame :: Connection -> a -> String -> [F.Header] -> 
               (a -> String -> [F.Header] -> Either String F.Frame) -> IO ()
  sendFrame c m receipt hs mkF = 
    if not (connected c) then throwIO $ ConnectException "Not connected!"
      else case mkF m receipt hs of
             Left  e -> throwIO $ ProtocolException $
                          "Cannot create Frame: " ++ e
             Right f -> 
               S.send (getWr c) (getSock c) f

  disc :: Connection -> IO Connection
  disc c = do
    let c' = c {conTcp  = False,
                conBrk  = False,
                conWrt  = Nothing,
                conRcv  = Nothing,
                conSock = Nothing}
    S.disconnect (getSock c)
    return c'

  connectBroker :: Int -> [F.Version] -> F.Heart -> Connection -> IO Connection
  connectBroker mx vers beat c = 
    case mkConF (conUsr c) (conPwd c) vers beat of
      Left e  -> return c {conErrM = e}
      Right f -> do
        rc  <- S.initReceiver
        wr  <- S.initWriter
        eiR <- catch (S.send wr (getSock c) f >> return (Right c))
                     (\e -> return $ Left $ show (e::SomeException))
        case eiR of
          Left  e -> return c {conErrM = e}
          Right _ -> do
           eiC <- S.receive rc (getSock c) mx 
           case eiC of
             Left e  -> return c {conErrM = e}
             Right r -> do
               let c' = handleConnected r
                          c {conRcv = Just rc, conWrt = Just wr}
               if period c' > 0 && period c' < fst beat
                 then return c {conErrM = "Beat frequency too high"}
                 else return c'
    where period = snd . conBeat

  handleConnected :: F.Frame -> Connection -> Connection
  handleConnected f c = 
    case F.typeOf f of
      F.Connected -> c {
                      conSrv  =  let srv = F.getServer f
                                 in F.getSrvName srv ++ "/"  ++
                                    F.getSrvVer  srv ++ " (" ++
                                    F.getSrvCmts srv ++ ")",
                      conBeat =  F.getBeat    f,
                      conVers = [F.getVersion f],
                      conSes  =  F.getSession f,
                      conBrk  = True}
      F.Error     -> c {conErrM = errToMsg f}
      _           -> c {conErrM = "Unexpected Frame: " ++ U.toString (F.putCommand f)}

  errToMsg :: F.Frame -> String
  errToMsg f = let msg = if B.length (F.getBody f) == 0 
                           then "."
                           else ": " ++ U.toString (F.getBody f)
               in F.getMsg f ++ msg

  mkConF :: String -> String -> [F.Version] -> F.Heart -> Either String F.Frame
  mkConF usr pwd vers beat = 
    F.mkConFrame [F.mkLogHdr  usr,
                  F.mkPassHdr pwd,
                  F.mkAcVerHdr $ F.versToVal vers, 
                  F.mkBeatHdr  $ F.beatToVal beat]

  mkDiscF :: String -> Either String F.Frame
  mkDiscF receipt =
    F.mkDisFrame $ mkReceipt receipt

  mkSubF :: Subscription -> String -> [F.Header] -> Either String F.Frame
  mkSubF sub receipt hs = 
    F.mkSubFrame $ [F.mkIdHdr   $ show $ subId sub,
                    F.mkDestHdr $ subName sub,
                    F.mkAckHdr  $ show $ subMode sub] ++ 
                   mkReceipt receipt ++ hs

  mkUnSubF :: Subscription -> String -> [F.Header] -> Either String F.Frame
  mkUnSubF sub receipt hs =
    let dh = if null (subName sub) then [] else [F.mkDestHdr $ subName sub]
    in  F.mkUSubFrame $ [F.mkIdHdr $ show $ subId sub] ++ dh ++ 
                        mkReceipt receipt ++ hs

  mkReceipt :: String -> [F.Header]
  mkReceipt receipt = if null receipt then [] else [F.mkRecHdr receipt]

  mkSendF :: Message a -> String -> [F.Header] -> Either String F.Frame
  mkSendF msg receipt hs = 
    Right $ F.mkSend (msgDest msg) (show $ msgTx msg)  receipt 
                     (msgType msg) (msgLen msg) hs 
                     (msgRaw  msg) 

  mkAckF :: Bool -> Message a -> String -> [F.Header] -> Either String F.Frame
  mkAckF ok msg receipt _ =
    let sh = if null $ show $ msgSub msg then [] 
               else [F.mkSubHdr $ show $ msgSub msg]
        th = if null $ show $ msgTx msg 
               then [] else [F.mkTrnHdr $ show $ msgTx msg]
        rh = mkReceipt receipt
        mk = if ok then F.mkAckFrame else F.mkNackFrame
    in mk $ F.mkMIdHdr (show $ msgId msg) : (sh ++ rh ++ th)

  mkBeginF :: String -> String -> [F.Header] -> Either String F.Frame
  mkBeginF tx receipt _ = 
    F.mkBgnFrame $ F.mkTrnHdr tx : mkReceipt receipt

  mkCommitF :: String -> String -> [F.Header] -> Either String F.Frame
  mkCommitF tx receipt _ =
    F.mkCmtFrame $ F.mkTrnHdr tx : mkReceipt receipt

  mkAbortF :: String -> String -> [F.Header] -> Either String F.Frame
  mkAbortF tx receipt _ =
    F.mkAbrtFrame $ F.mkTrnHdr tx : mkReceipt receipt

