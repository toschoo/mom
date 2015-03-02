{-# Language CPP #-}
module Protocol (
                 Subscription, mkSub,
                 subscribe, unsubscribe,
                 begin, commit, abort, sendBeat,
                 Message(..), mkMessage,
                 MsgId(..),
                 send, ack, nack)
where

  import           Stream

  import           Data.Conduit (($$))
  import qualified Data.Conduit.List  as CL
  import           Data.Conduit.Network
  import           Data.Conduit.Network.TLS

  import qualified Factory as Fac
  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Client.Exception
  import           Network.Socket (Socket)

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.Maybe (fromMaybe)

  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, catch, 
                                      SomeException, bracketOnError)
  import           Control.Concurrent
#ifdef _DEBUG
  import           Control.Monad (when)
#endif
  import           Codec.MIME.Type as Mime (Type) 

  ---------------------------------------------------------------------
  -- Default version, when broker does not send a version
  ---------------------------------------------------------------------
  defVersion :: F.Version
  defVersion = (1,2)

  ---------------------------------------------------------------------
  -- Subscribe abstraction
  ---------------------------------------------------------------------
  data Subscription = Sub {
                        subId   :: Fac.Sub,   -- subscribe identifier
                        subName :: String,    -- queue name
                        subMode :: F.AckMode  -- ack mode
                      }
    deriving (Show)

  mkSub :: Fac.Sub -> String -> F.AckMode -> Subscription
  mkSub sid qn am = Sub {
                      subId   = sid,
                      subName = qn,
                      subMode = am}

  ---------------------------------------------------------------------
  -- | Message Identifier
  ---------------------------------------------------------------------
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
                     -- | The Ack identifier
                     msgAck  :: String,
                     -- | The Stomp headers
                     --   that came with the message
                     msgHdrs :: [F.Header],
                     -- | The /MIME/ type of the content
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
  
  ---------------------------------------------------------------------
  -- Create a message
  ---------------------------------------------------------------------
  mkMessage :: MsgId -> Fac.Sub -> String -> String ->
               Mime.Type -> Int -> Fac.Tx -> 
               B.ByteString -> a -> Message a
  mkMessage mid sub dst ak typ len tx raw cont = Msg {
                                             msgId   = mid,
                                             msgSub  = sub,
                                             msgDest = dst,
                                             msgAck  = ak,
                                             msgHdrs = [], 
                                             msgType = typ, 
                                             msgLen  = len, 
                                             msgTx   = tx,
                                             msgRaw  = raw,
                                             msgCont = cont}

  ---------------------------------------------------------------------
  -- begin transaction
  ---------------------------------------------------------------------
  begin :: Connection -> String -> String -> IO ()
  begin c tx receipt = sendFrame c tx receipt [] mkBeginF

  ---------------------------------------------------------------------
  -- commit transaction
  ---------------------------------------------------------------------
  commit :: Connection -> String -> String -> IO ()
  commit c tx receipt = sendFrame c tx receipt [] mkCommitF

  ---------------------------------------------------------------------
  -- abort transaction
  ---------------------------------------------------------------------
  abort :: Connection -> String -> String -> IO ()
  abort c tx receipt = sendFrame c tx receipt [] mkAbortF

  ---------------------------------------------------------------------
  -- ack
  ---------------------------------------------------------------------
  ack :: Connection -> Message a -> String -> IO ()
  ack c m receipt = sendFrame c m receipt []  (mkAckF True)

  ---------------------------------------------------------------------
  -- nack
  ---------------------------------------------------------------------
  nack :: Connection -> Message a -> String -> IO ()
  nack c m receipt = sendFrame c m receipt [] (mkAckF False)

  ---------------------------------------------------------------------
  -- subscribe
  ---------------------------------------------------------------------
  subscribe :: Connection -> Subscription -> String -> [F.Header] -> IO ()
  subscribe c sub receipt hs = sendFrame c sub receipt hs mkSubF

  ---------------------------------------------------------------------
  -- unsubscribe
  ---------------------------------------------------------------------
  unsubscribe :: Connection -> Subscription -> String -> [F.Header] -> IO ()
  unsubscribe c sub receipt hs = sendFrame c sub receipt hs mkUnSubF

  ---------------------------------------------------------------------
  -- send
  ---------------------------------------------------------------------
  send :: Connection -> Message a -> String -> [F.Header] -> IO ()
  send c msg receipt hs = sendFrame c msg receipt hs mkSendF

  ---------------------------------------------------------------------
  -- heart beat
  ---------------------------------------------------------------------
  sendBeat :: Connection -> IO ()
  sendBeat c = sendFrame c () "" [] (\_ _ _ -> Right F.mkBeat)

  ---------------------------------------------------------------------
  -- generic sendFrame:
  -- takes a connection some data (like subscribe, message, etc.)
  -- some headers, a function that creates a frame or returns an error
  -- creates the frame and sends it
  ---------------------------------------------------------------------
  sendFrame :: Connection -> a -> String -> [F.Header] -> 
               (a -> String -> [F.Header] -> Either String F.Frame) -> IO ()
  sendFrame c m receipt hs mkF = 
    if not (connected c) then throwIO $ ConnectException "Not connected!"
      else case mkF m receipt hs of
             Left  e -> throwIO $ ProtocolException $
                          "Cannot create Frame: " ++ e
             Right f -> 
#ifdef _DEBUG
               do when (not $ F.complies (1,2) f) $
                    putStrLn $ "Frame does not comply with 1.2: " ++ show f 
#endif
                  writeChan (getChn c) f
 

  ---------------------------------------------------------------------
  -- transform an error frame into a string
  ---------------------------------------------------------------------
  errToMsg :: F.Frame -> String
  errToMsg f = F.getMsg f ++ if B.length (F.getBody f) == 0 
                                   then "."
                                   else ": " ++ U.toString (F.getBody f)
 
  ---------------------------------------------------------------------
  -- frame constructors
  -- this needs review...
  ---------------------------------------------------------------------
  mkReceipt :: String -> [F.Header]
  mkReceipt receipt = if null receipt then [] else [F.mkRecHdr receipt]

  mkConF :: ([F.Header] -> Either String F.Frame) ->
            String -> String -> String -> String  -> 
            [F.Version] -> F.Heart -> [F.Header]  -> Either String F.Frame
  mkConF mk host usr pwd cli vers beat hs = 
    let uHdr = if null usr then [] else [F.mkLogHdr  usr]
        pHdr = if null pwd then [] else [F.mkPassHdr pwd]
        cHdr = if null cli then [] else [F.mkCliIdHdr cli]
     in mk $ [F.mkHostHdr host,
              F.mkAcVerHdr $ F.versToVal vers, 
              F.mkBeatHdr  $ F.beatToVal beat] ++
             uHdr ++ pHdr ++ cHdr ++ hs

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

  mkSendF :: Message a -> String -> [F.Header] -> Either String F.Frame
  mkSendF msg receipt hs = 
    Right $ F.mkSend (msgDest msg) (show $ msgTx msg)  receipt 
                     (msgType msg) (msgLen msg) hs -- escape headers! 
                     (msgRaw  msg) 

  mkAckF :: Bool -> Message a -> String -> [F.Header] -> Either String F.Frame
  mkAckF ok msg receipt _ =
    let sh = if null $ show $ msgSub msg then [] 
               else [F.mkSubHdr $ show $ msgSub msg]
        th = if null $ show $ msgTx msg 
               then [] else [F.mkTrnHdr $ show $ msgTx msg]
        rh = mkReceipt receipt
        mk = if ok then F.mkAckFrame else F.mkNackFrame
    in mk $ F.mkIdHdr (msgAck msg) : (sh ++ rh ++ th)

  mkBeginF :: String -> String -> [F.Header] -> Either String F.Frame
  mkBeginF tx receipt _ = 
    F.mkBgnFrame $ F.mkTrnHdr tx : mkReceipt receipt

  mkCommitF :: String -> String -> [F.Header] -> Either String F.Frame
  mkCommitF tx receipt _ =
    F.mkCmtFrame $ F.mkTrnHdr tx : mkReceipt receipt

  mkAbortF :: String -> String -> [F.Header] -> Either String F.Frame
  mkAbortF tx receipt _ =
    F.mkAbrtFrame $ F.mkTrnHdr tx : mkReceipt receipt
    


