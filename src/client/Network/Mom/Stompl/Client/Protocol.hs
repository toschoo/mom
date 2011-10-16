module Protocol
where

  import qualified Socket as S
  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Exception
  import           Network.Socket (Socket)

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U

  import           Control.Concurrent
  import           Control.Applicative ((<$>))
  import           Control.Exception (throwIO)

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

  data TxState = TxStarted | TxAborted | TxCommitted

  data Transaction = Tx {
                       txId    :: String,
                       txState :: TxState}

  data Subscription = Sub {
                        subId   :: String,
                        subName :: String,
                        subMode :: F.AckMode}

  mkSub :: String -> String -> F.AckMode -> Subscription
  mkSub sid qn am = Sub {
                      subId   = sid,
                      subName = qn,
                      subMode = am}

  data Message a = Msg {
                     msgId   :: String,
                     msgSub  :: String,
                     msgType :: String,
                     msgLen  :: Int,
                     msgTx   :: String,
                     msgRaw  :: B.ByteString,
                     msgCont :: a}
  
  mkMessage :: String -> String -> String -> Int -> String -> B.ByteString -> a -> Message a
  mkMessage mid typ sub len tx raw cont = Msg {
                                          msgId   = mid,
                                          msgSub  = sub,
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

  uncompleteErr :: String
  uncompleteErr = "uncomplete Connection touched!"

  getSock :: Connection -> Socket
  getSock c = case conSock c of
                Just s  -> s
                Nothing -> error uncompleteErr

  getWr :: Connection -> S.Writer
  getWr c = case conWrt c of
              Just w  -> w
              Nothing -> error uncompleteErr

  getRc :: Connection -> S.Receiver
  getRc c = case conRcv c of
              Just r  -> r
              Nothing -> error uncompleteErr

  connected :: Connection -> Bool
  connected c = conTcp c && conBrk c

  getErr :: Connection -> String
  getErr = conErrM

  ok :: Connection -> Bool
  ok c = (0 == conErr c) &&
         (null $ conErrM c)

  getVersion :: Connection -> F.Version
  getVersion c = if null (conVers c) 
                   then defVersion
                   else head $ conVers c

  getReceipt :: Connection -> String
  getReceipt _ = "rc-1"

  connect :: String -> Int -> Int -> String -> String -> [F.Version] -> F.Heart -> IO Connection
  connect host port mx usr pwd vers beat = do
    let c = mkConnection host port mx usr pwd vers beat
    s <- S.connect host port
    connectBroker mx vers beat c {conSock = Just s, conTcp = True}

  disconnect :: Connection -> String -> IO Connection
  disconnect c receipt = 
    if not $ connected c 
      then return c {conErrM = "Not connected!"}
      else 
        if conBrk c
          then case mkDiscF receipt of
                 Left  e -> return c {conErrM = "Cannot create Frame: " ++ e}
                 Right f -> do
                   r <- S.send (getWr c) (getSock c) f 
                   disc receipt c
        else disc "" c

  begin :: Connection -> String -> String -> IO ()
  begin c tx receipt = sendFrame c tx receipt [] mkBeginF

  commit :: Connection -> String -> String -> IO ()
  commit c tx receipt = sendFrame c tx receipt [] mkCommitF

  abort :: Connection -> String -> String -> IO ()
  abort c tx receipt = sendFrame c tx receipt [] mkAbortF

  ack :: Connection -> Message a -> String -> IO ()
  ack c m receipt = sendFrame c m receipt [] mkAckF

  nack :: Connection -> Message a -> String -> IO ()
  nack c mid receipt = undefined

  subscribe :: Connection -> Subscription -> String -> [F.Header] -> IO ()
  subscribe c sub receipt hs = sendFrame c sub receipt hs mkSubF

  unsubscribe :: Connection -> Subscription -> String -> [F.Header] -> IO ()
  unsubscribe c sub receipt hs = sendFrame c sub receipt hs mkUnSubF

  send :: Connection -> String -> Message a -> String -> [F.Header] -> IO ()
  send c q msg receipt hs = 
    sendFrame c msg receipt ([F.mkDestHdr q] ++ hs) mkSendF

  sendFrame :: Connection -> a -> String -> [F.Header] -> 
               (a -> String -> [F.Header] -> Either String F.Frame) -> IO ()
  sendFrame c m receipt hs mkF = 
    if not $ connected c then throwIO $ ConnectException "Not connected!"
      else case mkF m receipt hs of
             Left e  -> throwIO $ ProtocolException $
                          "Cannot create Frame: " ++ e
             Right f -> S.send (getWr c) (getSock c) f

  disc :: String -> Connection -> IO Connection
  disc receipt c = do
    let c' = c {conTcp  = False,
                conBrk  = False,
                conWrt  = Nothing,
                conRcv  = Nothing,
                conSock = Nothing}
    if null receipt
      then do 
        S.disconnect (getSock c)
        return c'
      else do
        eiC <- S.receive (getRc c) (getSock c) (conMax c)
        S.disconnect (getSock c)
        case eiC of
          Left  e -> do
            return c' {conErrM = "Cannot disconnect from Broker: " ++ e}
          Right f ->
            case F.typeOf f of
              F.Receipt -> 
                return $ if receipt == F.getReceipt f
                            then c'
                            else c' {conErrM = "Wrong receipt: " ++ F.getReceipt f}
              F.Error     -> return c' {conErrM = errToMsg f}
              _           -> return c' {conErrM = "Unexpected Frame: " ++ (U.toString $ F.putCommand f)}

  connectBroker :: Int -> [F.Version] -> F.Heart -> Connection -> IO Connection
  connectBroker mx vers beat c = 
    case mkConF (conUsr c) (conPwd c) vers beat of
      Left e  -> return c {conErrM = e}
      Right f -> do
        rc <- S.initReceiver
        wr <- S.initWriter
        eiR <- catch (S.send wr (getSock c) f >> (return $ Right c))
                     (\e -> return $ Left $ show e)
        case eiR of
          Left  e -> return c {conErrM = e}
          Right _ -> do
           eiC <- S.receive rc (getSock c) mx 
           case eiC of
             Left e  -> return c {conErrM = e}
             Right r -> do
               let c' = handleConnected r
                          c {conRcv = Just rc, conWrt = Just wr} 
               return c'

  startListener :: Connection -> IO ()
  startListener c = undefined

  handleConnected :: F.Frame -> Connection -> Connection
  handleConnected f c = 
    case F.typeOf f of
      F.Connected -> c {
                      conSrv  =  let srv = F.getServer f
                                 in (F.getSrvName srv) ++ "/"  ++
                                    (F.getSrvVer  srv) ++ " (" ++
                                    (F.getSrvCmts srv) ++ ")",
                      conBeat =  F.getBeat    f,
                      conVers = [F.getVersion f],
                      conSes  =  F.getSession f,
                      conBrk  = True}
      F.Error     -> c {conErrM = errToMsg f}
      _           -> c {conErrM = "Unexpected Frame: " ++ (U.toString $ F.putCommand f)}

  errToMsg :: F.Frame -> String
  errToMsg f = let msg = if (B.length $ F.getBody f) == 0 
                           then "."
                           else ": " ++ (U.toString $ F.getBody f)
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
    F.mkSubFrame $ [F.mkIdHdr   $ subId sub,
                    F.mkDestHdr $ subName sub,
                    F.mkAckHdr  $ show $ subMode sub] ++ 
                   (mkReceipt receipt) ++ hs

  mkUnSubF :: Subscription -> String -> [F.Header] -> Either String F.Frame
  mkUnSubF sub receipt hs =
    F.mkUSubFrame $ [F.mkIdHdr $ subId sub] ++ (mkReceipt receipt) ++ hs

  mkReceipt :: String -> [F.Header]
  mkReceipt receipt = if null receipt then [] else [F.mkRecHdr receipt]

  mkSendF :: Message a -> String -> [F.Header] -> Either String F.Frame
  mkSendF msg receipt hs = 
    let th = if null $ msgTx msg then [] else [F.mkTrnHdr $ msgTx msg]
        rh = mkReceipt receipt
    in  F.mkSndFrame ([F.mkMimeHdr $ msgType msg] ++ th ++ rh ++ hs)
                     (msgLen msg)
                     (msgRaw msg)

  mkAckF :: Message a -> String -> [F.Header] -> Either String F.Frame
  mkAckF msg receipt _ =
    let sh = if null $ msgSub msg then [] else [F.mkSubHdr $ msgSub msg]
        th = if null $ msgTx msg then [] else [F.mkTrnHdr $ msgTx msg]
        rh = mkReceipt receipt
    in F.mkAckFrame ([F.mkMIdHdr $ msgId msg] ++ sh ++ rh ++ th)

  mkBeginF :: String -> String -> [F.Header] -> Either String F.Frame
  mkBeginF tx receipt _ = 
    F.mkBgnFrame $ [F.mkTrnHdr tx] ++ (mkReceipt receipt)

  mkCommitF :: String -> String -> [F.Header] -> Either String F.Frame
  mkCommitF tx receipt _ =
    F.mkCmtFrame $ [F.mkTrnHdr tx] ++ (mkReceipt receipt)

  mkAbortF :: String -> String -> [F.Header] -> Either String F.Frame
  mkAbortF tx receipt _ =
    F.mkAbrtFrame $ [F.mkTrnHdr tx] ++ (mkReceipt receipt)

