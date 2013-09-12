-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Stompl/Frame.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL
-- Stability  : experimental
-- Portability: portable
--
-- Stomp Frames and some useful operations on them
-------------------------------------------------------------------------------
module Network.Mom.Stompl.Frame (
                       -- * Frames
                       -- $stomp_frames
                       Frame, FrameType(..),
                       Header, Body, Heart, Version,
                       AckMode(..), isValidAck, 
                       SrvDesc,
                       getSrvName, getSrvVer, getSrvCmts,
                       -- * Frame Constructors
                       -- $stomp_constructors

                       -- ** Basic Frame Constructors
                       mkConnect, mkConnected, 
                       mkSubscribe, mkUnsubscribe, mkSend,
                       mkMessage, 
                       mkBegin, mkCommit,  mkAbort,
                       mkAck, mkNack, 
                       mkDisconnect,  
                       mkBeat,
                       mkErr, mkReceipt,
                       -- ** Header-based Frame Constructors
                       mkConFrame, mkCondFrame, 
                       mkSubFrame, mkUSubFrame, mkMsgFrame,
                       mkSndFrame, mkDisFrame,  mkErrFrame,
                       mkBgnFrame, mkCmtFrame,  mkAbrtFrame,
                       mkAckFrame, mkNackFrame, mkRecFrame,
                       -- * Working with Headers
                       -- $stomp_headers
                       mkLogHdr,   mkPassHdr, mkDestHdr, 
                       mkLenHdr,   mkTrnHdr,  mkRecHdr, 
                       mkSelHdr,   mkIdHdr,   mkAckHdr, 
                       mkSesHdr,   mkMsgHdr,  mkMIdHdr,
                       mkAcVerHdr, mkVerHdr,  mkHostHdr,
                       mkBeatHdr,  mkMimeHdr, mkSrvHdr,
                       mkSubHdr, mkCliIdHdr,
                       valToVer, valToVers, verToVal, versToVal,
                       beatToVal, valToBeat,
                       ackToVal, valToAck,
                       strToSrv, srvToStr,
                       negoVersion, negoBeat,
                       rmHdr, rmHdrs, 
                       getAck, getLen,   
                       -- * Working with Frames
                       typeOf, putFrame, toString, putCommand,
                       sndToMsg, conToCond,
                       resetTrans, 
                       complies,
                       -- * Get Access to Frames
                       getDest, getTrans, getReceipt,
                       getLogin, getPasscode, getCliId,
                       getHost, getVersions, getVersion,
                       getBeat, 
                       getSession, getServer, 
                       getSub, getSelector, getId, getAcknow,
                       getBody, getMime, getLength, 
                       getMsg, getHeaders,
                       -- * Sequence Operators to work on 'ByteString'
                       (|>), (<|), (>|<),
                       -- * Some random helpers 
                       --   (that do not really belong here)
                       upString, numeric)
where

  -- Todo:
  -- - conformance to protocol

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.Char (toUpper, isDigit)
  import           Data.List (find, sortBy, foldl')
  import           Data.List.Split (splitWhen)
  import           Data.Maybe (catMaybes)
  import           Codec.MIME.Type as Mime (showType, Type, nullType)
  import           Codec.MIME.Parse        (parseMIMEType)

  ------------------------------------------------------------------------
  -- | Tuple of (key, value)
  ------------------------------------------------------------------------
  type Header = (String, String)

  ------------------------------------------------------------------------
  -- | The Frame body is represented as /strict/ 'ByteString'.
  ------------------------------------------------------------------------
  type Body   = B.ByteString

  ------------------------------------------------------------------------
  -- | The Stomp version used or accepted by the sender;
  --   the first 'Int' is the major version number,
  --   the second is the minor.
  --   For details on version negotiation, please refer to 
  --   the Stomp specification.
  ------------------------------------------------------------------------
  type Version = (Int, Int)
  
  ------------------------------------------------------------------------
  -- | Heart-beat configuration;
  --   the first 'Int' of the pair represents the frequency 
  --   in which the sender wants to send heart-beats; 
  --   the second represents the highest frequency
  --   in which the sender can accept heart-beats.
  --   The frequency is expressed as 
  --   the period in milliseconds between two heart-beats.
  --   For details on negotiating heart-beats, 
  --   please refer to the Stomp specification.
  ------------------------------------------------------------------------
  type Heart   = (Int, Int)

  -------------------------------------------------------------------------
  -- | Description of a server consisting of
  --   name, version and comments
  -------------------------------------------------------------------------
  type SrvDesc = (String, String, String)

  -------------------------------------------------------------------------
  -- | get name from 'SrvDesc'
  -------------------------------------------------------------------------
  getSrvName :: SrvDesc -> String
  getSrvName (n, _, _) = n

  -------------------------------------------------------------------------
  -- | get version from 'SrvDesc'
  -------------------------------------------------------------------------
  getSrvVer :: SrvDesc -> String
  getSrvVer  (_, v, _) = v

  -------------------------------------------------------------------------
  -- | get comments from 'SrvDesc'
  -------------------------------------------------------------------------
  getSrvCmts :: SrvDesc -> String
  getSrvCmts (_, _, c) = c

  noBeat :: Heart
  noBeat = (0,0)

  defMime :: Mime.Type
  defMime =  Mime.nullType

  defVerStr :: String
  defVerStr = "1.0"

  defVersion :: Version
  defVersion = (1, 0)

  noSrvDesc :: SrvDesc
  noSrvDesc = ("","","")

  hdrLog, hdrPass, hdrDest, hdrSub, hdrLen, hdrTrn, hdrRec, hdrRecId,
    hdrSel, hdrId, hdrAck, hdrSes, hdrMsg, hdrMId, hdrSrv,
    hdrAcVer, hdrVer, hdrBeat, hdrHost, hdrMime, hdrCliId :: String
  hdrLog   = "login"
  hdrPass  = "passcode"
  hdrCliId = "client-id"
  hdrDest  = "destination"
  hdrSub   = "subscription"
  hdrLen   = "content-length"
  hdrMime  = "content-type"
  hdrTrn   = "transaction"
  hdrRec   = "receipt"
  hdrRecId = "receipt-id"
  hdrSel   = "selector"
  hdrId    = "id"
  hdrAck   = "ack"
  hdrSes   = "session-id"
  hdrMsg   = "message"
  hdrMId   = "message-id"
  hdrAcVer = "accept-version"
  hdrVer   = "version"
  hdrHost  = "host"
  hdrBeat  = "heart-beat"
  hdrSrv   = "server"

  mkHeader :: String -> String -> Header
  mkHeader k v = (k, v)

  ------------------------------------------------------------------------
  -- | make /login/ header
  ------------------------------------------------------------------------
  mkLogHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /passcode/ header
  ------------------------------------------------------------------------
  mkPassHdr  :: String -> Header
  ------------------------------------------------------------------------
  -- | make /client-id/ header
  ------------------------------------------------------------------------
  mkCliIdHdr  :: String -> Header
  ------------------------------------------------------------------------
  -- | make /destination/ header
  ------------------------------------------------------------------------
  mkDestHdr  :: String -> Header
  ------------------------------------------------------------------------
  -- | make /content-length/ header
  ------------------------------------------------------------------------
  mkLenHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /content-type/ header
  ------------------------------------------------------------------------
  mkMimeHdr  :: String -> Header
  ------------------------------------------------------------------------
  -- | make /transaction/ header
  ------------------------------------------------------------------------
  mkTrnHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /receipt/ header
  ------------------------------------------------------------------------
  mkRecHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /receipt-id/ header
  ------------------------------------------------------------------------
  mkRecIdHdr :: String -> Header
  ------------------------------------------------------------------------
  -- | make /selector/ header
  ------------------------------------------------------------------------
  mkSelHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /message-id/ header
  ------------------------------------------------------------------------
  mkMIdHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /id/ header (subscribe frame)
  ------------------------------------------------------------------------
  mkIdHdr    :: String -> Header
  ------------------------------------------------------------------------
  -- | make /subscription/ header
  ------------------------------------------------------------------------
  mkSubHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /server/ header (connected frame)
  ------------------------------------------------------------------------
  mkSrvHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /ack/ header (subscribe frame)
  ------------------------------------------------------------------------
  mkAckHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /session/ header (connected frame)
  ------------------------------------------------------------------------
  mkSesHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /accept-version/ header (connect frame)
  ------------------------------------------------------------------------
  mkAcVerHdr :: String -> Header
  ------------------------------------------------------------------------
  -- | make /version/ header (connected frame)
  ------------------------------------------------------------------------
  mkVerHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /host/ header (connect frame)
  ------------------------------------------------------------------------
  mkHostHdr  :: String -> Header
  ------------------------------------------------------------------------
  -- | make /message/ header (error frame)
  ------------------------------------------------------------------------
  mkMsgHdr   :: String -> Header
  ------------------------------------------------------------------------
  -- | make /heart-beat/ header
  ------------------------------------------------------------------------
  mkBeatHdr  :: String -> Header

  mkLogHdr   = mkHeader hdrLog
  mkPassHdr  = mkHeader hdrPass
  mkCliIdHdr = mkHeader hdrCliId
  mkDestHdr  = mkHeader hdrDest
  mkLenHdr   = mkHeader hdrLen
  mkMimeHdr  = mkHeader hdrMime
  mkTrnHdr   = mkHeader hdrTrn
  mkRecHdr   = mkHeader hdrRec
  mkRecIdHdr = mkHeader hdrRecId
  mkSelHdr   = mkHeader hdrSel
  mkIdHdr    = mkHeader hdrId
  mkMIdHdr   = mkHeader hdrMId
  mkAckHdr   = mkHeader hdrAck
  mkSubHdr   = mkHeader hdrSub
  mkSesHdr   = mkHeader hdrSes
  mkMsgHdr   = mkHeader hdrMsg
  mkVerHdr   = mkHeader hdrVer
  mkAcVerHdr = mkHeader hdrAcVer
  mkHostHdr  = mkHeader hdrHost
  mkBeatHdr  = mkHeader hdrBeat
  mkSrvHdr   = mkHeader hdrSrv

  {- $stomp_frames
     Frames are the building blocks of the Stomp protocol.
     They are exchanged between broker and application 
     and contain commands or status and error messages.

     Frames follow a simple text-based format.
     They consist of a /command/ (the 'FrameType'),
     a list of key\/value-pairs, called 'Header',
     and a 'Body' (which is empty for most frame types).
  -}

  -------------------------------------------------------------------------
  -- | This is a frame
  -------------------------------------------------------------------------
  data Frame = ConFrame {
                   frmLogin :: String,
                   frmPass  :: String,
                   frmHost  :: String,
                   frmBeat  :: Heart,
                   frmAcVer :: [Version], -- 1.1
                   frmCliId :: String
                 }
               | CondFrame {
                   frmSes   :: String,
                   frmBeat  :: Heart,
                   frmVer   :: Version, -- 1.1
                   frmSrv   :: SrvDesc
                 }
               | SubFrame {
                   frmDest  :: String,
                   frmAck   :: AckMode,
                   frmSel   :: String,
                   frmId    :: String,
                   frmRec   :: String
                 }
               | USubFrame {
                   frmDest  :: String,
                   frmId    :: String,
                   frmRec   :: String
                 }
               | SndFrame {
                   frmHdrs  :: [Header],
                   frmDest  :: String,
                   frmTrans :: String,
                   frmRec   :: String,
                   frmLen   :: Int,
                   frmMime  :: Mime.Type,
                   frmBody  :: Body}
               | DisFrame {
                   frmRec   :: String
                 }
               | BgnFrame {
                   frmTrans :: String,
                   frmRec   :: String
                 }
               | CmtFrame {
                   frmTrans :: String,
                   frmRec   :: String
                 }
               | AckFrame {
                   frmId    :: String,
                   frmSub   :: String,
                   frmTrans :: String,
                   frmRec   :: String
                 }
               | NackFrame {
                   frmId    :: String,
                   frmSub   :: String,
                   frmTrans :: String,
                   frmRec   :: String
                 }
               | AbrtFrame {
                   frmTrans :: String,
                   frmRec   :: String
                 }
               | MsgFrame {
                   frmHdrs  :: [Header],
                   frmSub   :: String,
                   frmDest  :: String,
                   frmId    :: String,
                   frmLen   :: Int,
                   frmMime  :: Mime.Type,
                   frmBody  :: Body}
               | RecFrame {
                   frmRec   :: String
                 }
               | ErrFrame {
                   frmMsg  :: String,
                   frmRec  :: String,
                   frmLen  :: Int,
                   frmMime :: Mime.Type,
                   frmBody :: Body}
               | BeatFrame
    deriving (Show, Eq)

  {- $stomp_constructors
     There are two different interfaces to construct frames:
     
     * a set of conventional, /basic/ constructors and

     * a set of header-based constructors 

     The /basic/ constructors receive the frame attributes
     directly, /i.e./ with the types, in which they will be stored.
     These constructors are, hence, type-safe.
     They are, however, unsafe in terms of protocol compliance.
     Headers that identify some entity are stored as plain strings. 
     The basic constructors do not verify 
     if an identifier is required for a given frame type.
     Using plain strings for identifiers
     may appear to be odd on the first sight.
     Since this library is intended for any
     implementation of Stomp programs (brokers and applications) where
     identifers (for messages, transactions, receipts, /etc./)
     may have completely different formats,
     no choice was made on dedicated identifier types.

     Header-based constructors, on the other hand, 
     receive attributes packed in a list of 'Header'.
     The types are converted by the constructor. 
     The constructor, additionally, verfies the protocol compliance.
     Header-based constructors are, hence, more reliable.
     This implies, however, that they can fail.
     For this reason, Header-based constructors return 'Either'. 
  -}

  ----------------------------------------------------------------------
  -- | make a 'Connect' frame (Application -> Broker).
  --   The parameters are:
  --
  --   * User: user to authenticate at the broker.
  --
  --   * Passcode: password to authenticate at the broker.
  --
  --   * Host: broker's virtual hoast (/e.g./ 
  --           stomp.broker.github.org).
  --
  --   * 'HeartBeat': the clients bid in negotiating
  --                  the heart-beat.
  --
  --   * 'Version': the versions supported by the client.
  --
  --   * 'ClientId': Client identification for persistent connections.
  ----------------------------------------------------------------------
  mkConnect :: String -> String -> String -> Heart -> [Version] -> String -> Frame
  mkConnect usr pwd hst beat vers cli =
    ConFrame {
       frmLogin = usr,
       frmPass  = pwd,
       frmHost  = hst,
       frmBeat  = beat,
       frmAcVer = vers,
       frmCliId = cli}

  ----------------------------------------------------------------------
  -- | make a 'Connect' frame (Broker -> Application).
  --   The parameters are:
  --
  --   * Session: A unique identifier created by the broker
  --              and identifying the session
  --
  --   * 'HeartBeat': The heart-beat agreed by the broker
  --
  --   * 'Version': The version accepted by the broker
  --
  --   * 'SrvDesc': The server description
  ----------------------------------------------------------------------
  mkConnected :: String -> Heart -> Version -> SrvDesc -> Frame
  mkConnected ses beat ver srv =
    CondFrame {
      frmSes  = ses,
      frmBeat = beat,
      frmVer  = ver, 
      frmSrv  = srv}

  ----------------------------------------------------------------------
  -- | make a 'Subscribe' frame (Application -> Broker).
  --   The parameters are:
  --   
  --   * Destination: The name of the queue as it is known by the broker
  --                  and other applications using the queue
  --   
  --   * 'AckMode': The Acknowledge Mode for this subscription
  --   
  --   * Selector: An expression defining those messages
  --               that are of actual for client.
  --               The Stomp protocol does not define
  --               a language for selectors;
  --               it is even not entirely clear,
  --               where messages are selected:
  --               already at the broker, or only by the client.
  --               Some brokers provide pre-selection of messages, 
  --               others do not.  
  --   
  --   * Subscription Id: A unique identifier distinguishing this 
  --                      subscription from others to the same queue.
  --                      The identifier is defined by the application.
  --   
  --   * Receipt: A unique identifier defined by the application
  --              to request confirmation of receipt of this frame.
  --              If no receipt is wanted, the string shall be empty.
  ----------------------------------------------------------------------
  mkSubscribe :: String -> AckMode -> String -> String -> String -> Frame
  mkSubscribe dst ack sel sid rc =
    SubFrame {
      frmDest = dst,
      frmAck  = ack,
      frmSel  = sel,
      frmId   = sid,
      frmRec  = rc}

  ----------------------------------------------------------------------
  -- | make an 'Unsubscribe' frame (Application -> Broker).
  --   The parameters are:
  -- 
  --   * Destination: The queue name; either a destination or a 
  --                  subscription id must be given. 
  --                  (According to protocol version 1.1,
  --                   the subscription id is mandatory on
  --                   both, 'Subscribe' and 'Unsubscribe'.)
  -- 
  --   * Subscription Id: The subscription identifier (see 'mkSubscribe')
  -- 
  --   * Receipt: The receipt (see 'mkSubscribe')
  ----------------------------------------------------------------------
  mkUnsubscribe :: String -> String -> String -> Frame
  mkUnsubscribe dst sid rc =
    USubFrame {
      frmDest = dst,
      frmId   = sid,
      frmRec  = rc}
 
  ----------------------------------------------------------------------
  -- | make a 'Send' frame (Application -> Broker).
  --   The parameters are:
  --
  --   * Destination: The name of the queue 
  --                  where the message should be published
  --
  --   * Transaction: A unique identifier indicating
  --                  a running transaction;
  --                  if sent with a transaction,
  --                  the message will not be delivered
  --                  to subscribing applications,
  --                  before the transaction is committed.
  --                  If the 'Send' is not part of a transaction,
  --                  the string shall be empty.
  --
  --   * Receipt: A receipt (see 'mkSubscribe' for details)
  --
  --   * 'Mime.Type': The content type of the payload message
  --                  as MIME Type
  --
  --   * Length: The length of the type in bytes
  --
  --   * 'Header': List of additional headers;
  --               Stomp protocol requires that user-specified
  --               headers are passed through to subscribing applications.
  --               These headers may, for instance, be use
  --               by selectors to select messages. 
  --
  --   * 'Body': The payload message
  ----------------------------------------------------------------------
  mkSend :: String    -> String -> String   -> 
            Mime.Type -> Int    -> [Header] -> 
            Body      -> Frame
  mkSend dst trn rec mime len hs bdy = 
    SndFrame {
      frmHdrs  = hs,
      frmDest  = dst,
      frmTrans = trn,
      frmRec   = rec,
      frmLen   = len,
      frmMime  = mime,
      frmBody  = bdy}

  ----------------------------------------------------------------------
  -- | make a 'Message' frame (Broker -> Application).
  --   The parameters are:
  --
  --   * Subscription Id: The message was sent
  --                      because the application subscribed to the queue
  --                      with this subscription id (see 'mkSubscribe').
  --
  --   * Destination: The name of the queue, in wich the message was published.
  --
  --   * Message Id: A unique message identifier, defined by the broker
  --
  --   * 'Mime.Type': The type of the playload as MIME Type
  --
  --   * Length: The length of the payload in bytes
  --
  --   * 'Header': A list of user-defined headers (see 'mkSend' for details)
  --
  --   * 'Body': The payload
  ----------------------------------------------------------------------
  mkMessage :: String    -> String -> String   ->
               Mime.Type -> Int    -> [Header] -> 
               Body      -> Frame
  mkMessage sub dst mid mime len hs bdy =
    MsgFrame {
      frmHdrs  = hs,
      frmSub   = sub,
      frmDest  = dst,
      frmId    = mid,
      frmLen   = len,
      frmMime  = mime,
      frmBody  = bdy}

  ----------------------------------------------------------------------
  -- | make a 'Begin' frame (Application -> Broker).
  --   The parameters are:
  --
  --   * Transaction: A unique transaction identifier
  --                  defined by the application. 
  --
  --   * Receipt: A receipt (see 'mkSubscribe' for details)
  ----------------------------------------------------------------------
  mkBegin  :: String -> String -> Frame
  mkBegin = BgnFrame

  ----------------------------------------------------------------------
  -- | make a 'Commit' frame (Application -> Broker).
  --   The parameters are:
  --
  --   * Transaction: A unique transaction identifier
  --                  defined by the application. 
  --
  --   * Receipt: A receipt (see 'mkSubscribe' for details)
  ----------------------------------------------------------------------
  mkCommit :: String -> String -> Frame
  mkCommit = CmtFrame

  ----------------------------------------------------------------------
  -- | make an 'Abort' frame (Application -> Broker).
  --   The parameters are:
  --
  --   * Transaction: A unique transaction identifier
  --                  defined by the application. 
  --
  --   * Receipt: A receipt (see 'mkSubscribe' for details)
  ----------------------------------------------------------------------
  mkAbort  :: String -> String -> Frame
  mkAbort = AbrtFrame

  ----------------------------------------------------------------------
  -- | make an 'Ack' frame (Application -> Broker).
  --   The parameters are:
  --
  --   * Message Id: The identifier of the message to be ack'd
  --
  --   * Subscription Id: The subscription, through which
  --                      the message was received
  --
  --   * Transaction: Acks may be part of a transaction
  --                  (see 'mkSend' for details).
  --
  --   * Receipt: see 'mkSubscribe' for details
  ----------------------------------------------------------------------
  mkAck :: String -> String -> String -> String -> Frame
  mkAck mid sid trn rc = AckFrame {
                           frmId    = mid,
                           frmSub   = sid,
                           frmTrans = trn,
                           frmRec   = rc}

  ----------------------------------------------------------------------
  -- | make a 'Nack' frame (Application -> Broker).
  --   The parameters are:
  --
  --   * Message Id: The identifier of the message to be nack'd
  --
  --   * Subscription Id: The subscription, through which
  --                      the message was received
  --
  --   * Transaction: Nacks may be part of a transaction
  --                  (see 'mkSend' for details).
  --
  --   * Receipt: see 'mkSubscribe' for details
  ----------------------------------------------------------------------
  mkNack :: String -> String -> String -> String -> Frame
  mkNack mid sid trn rc = NackFrame {
                            frmId    = mid,
                            frmSub   = sid,
                            frmTrans = trn,
                            frmRec   = rc}

  ----------------------------------------------------------------------
  -- | make a 'HeatBeat' frame (Application -> Broker and
  --                            Broker      -> Application)
  ----------------------------------------------------------------------
  mkBeat :: Frame
  mkBeat = BeatFrame

  ----------------------------------------------------------------------
  -- | make a 'Disconnect' frame (Application -> Broker).
  --   The parameter is:
  --
  --   * Receipt: see 'mkSubscribe' for details
  ----------------------------------------------------------------------
  mkDisconnect :: String -> Frame
  mkDisconnect rec = DisFrame rec

  ----------------------------------------------------------------------
  -- | make a 'Receipt' frame (Broker -> Application).
  --   The parameter is:
  -- 
  --   * Receipt: The receipt identifier received from the application
  ----------------------------------------------------------------------
  mkReceipt :: String -> Frame
  mkReceipt rec = RecFrame rec

  ----------------------------------------------------------------------
  -- | make a 'Receipt' frame (Broker -> Application).
  --   The parameters are:
  --
  --   * Error Message Id: A short error description
  --
  --   * Receipt Id: The receipt of frame sent by the application
  --                 to which this error relates
  --
  --   * 'Mime.Type': The format of the error message as MIME Type
  --
  --   * Length: The length of the error message
  --
  --   * 'Body': The error message
  ----------------------------------------------------------------------
  mkErr :: String -> String -> Mime.Type -> Int -> Body -> Frame
  mkErr mid rc mime len bdy =
    ErrFrame {
      frmMsg  = mid,
      frmRec  = rc,
      frmLen  = len,
      frmMime = mime,
      frmBody = bdy}

  ------------------------------------------------------------------------
  -- | get /destination/ 
  --   from 'Subscribe', 'Unsubscribe', 'Send' or 'Message'
  ------------------------------------------------------------------------
  getDest :: Frame -> String
  getDest = frmDest
  ------------------------------------------------------------------------
  -- | get /transaction/ from 'Send', 'Ack', 'Nack', 
  --                          'Begin', 'Commit' or 'Abort'
  ------------------------------------------------------------------------
  getTrans :: Frame -> String
  getTrans = frmTrans 
  ------------------------------------------------------------------------
  -- | get /receipt/ or /receipt-id/ from any frame, but
  --   'Connect', 'Connected', 'Message', 'Error'
  ------------------------------------------------------------------------
  getReceipt :: Frame -> String
  getReceipt = frmRec
  ------------------------------------------------------------------------
  -- | get /host/ from 'Connect'
  ------------------------------------------------------------------------
  getHost :: Frame -> String
  getHost = frmHost
  ------------------------------------------------------------------------
  -- | get /accept-version/ from 'Connect'
  ------------------------------------------------------------------------
  getVersions :: Frame -> [Version]
  getVersions = frmAcVer
  ------------------------------------------------------------------------
  -- | get /heart-beat/ from 'Connect' or 'Connected'
  ------------------------------------------------------------------------
  getBeat :: Frame -> Heart
  getBeat = frmBeat
  ------------------------------------------------------------------------
  -- | get /login/ from 'Connect'
  ------------------------------------------------------------------------
  getLogin :: Frame -> String
  getLogin = frmLogin 
  ------------------------------------------------------------------------
  -- | get /passcode/ from 'Connect'
  ------------------------------------------------------------------------
  getPasscode :: Frame -> String
  getPasscode = frmPass
  ------------------------------------------------------------------------
  -- | get /client-id/ from 'Connect'
  ------------------------------------------------------------------------
  getCliId :: Frame -> String
  getCliId = frmCliId
  ------------------------------------------------------------------------
  -- | get /version/ from 'Connected'
  ------------------------------------------------------------------------
  getVersion :: Frame -> Version
  getVersion = frmVer
  ------------------------------------------------------------------------
  -- | get /session/ from 'Connected'
  ------------------------------------------------------------------------
  getSession :: Frame -> String
  getSession = frmSes
  ------------------------------------------------------------------------
  -- | get /server/ from 'Connected'
  ------------------------------------------------------------------------
  getServer :: Frame -> SrvDesc
  getServer = frmSrv
  ------------------------------------------------------------------------
  -- | get /id/ from 'Subscribe' or 'Unsubscribe'
  ------------------------------------------------------------------------
  getId :: Frame -> String
  getId = frmId
  ------------------------------------------------------------------------
  -- | get /ack/ from 'Subscribe'
  ------------------------------------------------------------------------
  getAcknow :: Frame -> AckMode
  getAcknow = frmAck
  ------------------------------------------------------------------------
  -- | get /selector/ from 'Subscribe'
  ------------------------------------------------------------------------
  getSelector :: Frame -> String
  getSelector = frmSel
  ------------------------------------------------------------------------
  -- | get /subscription/ from 'Ack', 'Nack' or 'Message'
  ------------------------------------------------------------------------
  getSub :: Frame -> String
  getSub = frmSub
  ------------------------------------------------------------------------
  -- | get /body/ from 'Send', 'Message', 'Error'
  ------------------------------------------------------------------------
  getBody :: Frame -> B.ByteString
  getBody = frmBody
  ------------------------------------------------------------------------
  -- | get /content-type/ from 'Send', 'Message', 'Error'
  ------------------------------------------------------------------------
  getMime :: Frame -> Mime.Type
  getMime = frmMime
  ------------------------------------------------------------------------
  -- | get /content-length/ from 'Send', 'Message', 'Error'
  ------------------------------------------------------------------------
  getLength :: Frame -> Int
  getLength = frmLen
  ------------------------------------------------------------------------
  -- | get /message/ from 'Error'
  ------------------------------------------------------------------------
  getMsg :: Frame -> String
  getMsg = frmMsg
  ------------------------------------------------------------------------
  -- | get all additional headers from 'Send' or 'Message'
  ------------------------------------------------------------------------
  getHeaders :: Frame -> [Header]
  getHeaders = frmHdrs

  ------------------------------------------------------------------------
  -- | The frame type identifies, what the Stomp protocol calls /command/;
  --   
  --   * commands sent from application to broker are:
  --     Connect, Disconnect, Subscribe, Unsubscribe, Send, 
  --     Begin, Commit, Abort, Ack, Nack, HeartBeat
  --
  --   * commands sent from broker to application are:
  --     Connected, Message, Error, HeartBeat
  ------------------------------------------------------------------------
  data FrameType = 
    -- | Sent by the application to initiate a connection
    Connect   
    -- | Sent by the broker to confirm the connection
    | Connected   
    -- | Sent by the application to end the connection
    | Disconnect 
    -- | Sent by the application to publish a message in a queue
    | Send       
    -- | Sent by the broker to forward a message
    --   published in a queue to which
    --   the application has subscribed
    | Message 
    -- | Sent by the application to subscribe to a queue
    | Subscribe 
    -- | Sent by the application to unsubscribe from a queue
    | Unsubscribe
    -- | Sent by the application to start a transaction
    | Begin
    -- | Sent by the application to commit a transaction
    | Commit
    -- | Sent by the application to abort a transaction
    | Abort
    -- | Sent by the application to acknowledge a message
    | Ack
    -- | Sent by the application to negatively acknowledge a message
    | Nack
    -- | Keep-alive message sent by both, application and broker
    | HeartBeat
    -- | Sent by the broker to report an error
    | Error   
    -- | Sent by the broker to confirm the receipt of a frame
    | Receipt 
    deriving (Show, Read, Eq)

  ------------------------------------------------------------------------
  -- | gets the 'FrameType' of a 'Frame'
  ------------------------------------------------------------------------
  typeOf :: Frame -> FrameType
  typeOf f = case f of
              (ConFrame  _ _ _  _ _ _  ) -> Connect
              (CondFrame _ _ _ _       ) -> Connected
              (DisFrame  _             ) -> Disconnect
              (SubFrame  _ _ _ _ _     ) -> Subscribe
              (USubFrame _ _ _         ) -> Unsubscribe
              (SndFrame  _ _ _ _ _ _ _ ) -> Send
              (BgnFrame  _ _           ) -> Begin
              (CmtFrame  _ _           ) -> Commit
              (AbrtFrame _ _           ) -> Abort
              (AckFrame  _ _ _ _       ) -> Ack
              (NackFrame _ _ _ _       ) -> Nack
              (MsgFrame  _ _ _ _ _ _ _ ) -> Message
              (RecFrame  _             ) -> Receipt
              (ErrFrame  _ _ _ _ _     ) -> Error
              (BeatFrame               ) -> HeartBeat

  ------------------------------------------------------------------------
  -- The AckMode of a Subscription
  ------------------------------------------------------------------------
  data AckMode = 
    -- | A successfully sent message is automatically considered ack\'d
    Auto 
    -- | The client is expected to explicitly confirm the receipt
    --   of a message by sending an 'Ack' frame;
    --   all message older than the ack'd message
    --   since the last 'Ack' (or the beginning of the session)
    --   are implicitly ack\'d as well.
    --   This is called /cumulative/ ack.
    | Client 
    -- | Non-cumulative ack:
    --   The client is expected to explicitly confirm the receipt
    --   of a message by sending an 'Ack' frame;
    --   only the message with the msg-id in the 'Ack' frame
    --   is actually ack\'d
    | ClientIndi
    deriving (Eq)

  instance Show AckMode where
    show Auto       = "auto"
    show Client     = "client"
    show ClientIndi = "client-individual"

  instance Read AckMode where
    readsPrec _ s = case upString s of
                     "AUTO"              -> [(Auto, "")]
                     "CLIENT"            -> [(Client, "")]
                     "CLIENT-INDIVIDUAL" -> [(ClientIndi, "")]
                     _                   -> error $ "Can't parse AckMode: " ++ s
                   
  infixr >|<, |>, <| 
  -- | append
  (>|<) :: B.ByteString -> B.ByteString -> B.ByteString
  -- | snoc
  (|>)  :: B.ByteString ->   Char       -> B.ByteString
  -- | cons
  (<|)  ::   Char       -> B.ByteString -> B.ByteString
  x >|< y = x `B.append` y
  x <|  y = x `B.cons` y
  x  |> y = x `B.snoc` y

  ----------------------------------------------------------------
  -- | check if 'String' represents a valid 'AckMode'
  ----------------------------------------------------------------
  isValidAck :: String -> Bool
  isValidAck s = upString s `elem` ["AUTO", "CLIENT", "CLIENT-INDIVIDUAL"]

  upString :: String -> String
  upString = map toUpper

  numeric :: String -> Bool
  numeric = all isDigit

  cleanWhite :: String -> String
  cleanWhite = 
    takeWhile (/= ' ') . dropWhile (== ' ')

  getLen :: [Header] -> Either String Int
  getLen hs = 
    case lookup hdrLen hs of
      Nothing -> Right (-1)
      Just l  -> let len = cleanWhite l
                 in if numeric len then Right $ read len 
                      else Left $ "content-length is not numeric: " ++ l

  getAck :: [Header] -> Either String AckMode
  getAck hs = 
    case lookup hdrAck hs of
      Nothing -> Right Auto
      Just a  -> if isValidAck a 
                   then Right $ read a 
                   else Left  $ "Invalid ack header in Subscribe Frame: " 
                                ++ a

  ------------------------------------------------------------------------ 
  -- | convert list of 'Version' to 'String'
  ------------------------------------------------------------------------
  versToVal :: [Version] -> String
  versToVal = foldr addVer "" . map verToVal 
    where addVer v vs = if not $ null vs
                          then v ++ "," ++ vs
                          else v

  ------------------------------------------------------------------------ 
  -- | convert 'Version' to 'String'
  ------------------------------------------------------------------------
  verToVal :: Version -> String
  verToVal (major, minor) = (show major) ++ "." ++ (show minor)

  ------------------------------------------------------------------------ 
  -- | convert 'String' to list of 'Version'
  ------------------------------------------------------------------------
  valToVers :: String -> Maybe [Version]
  valToVers s = case find (== Nothing) vs of
                  Nothing -> Just $ catMaybes vs
                  Just _  -> Nothing
    where ss = splitWhen (== ',') s 
          vs = map valToVer ss

  ------------------------------------------------------------------------ 
  -- | convert 'String' to 'Version'
  ------------------------------------------------------------------------
  valToVer :: String -> Maybe Version
  valToVer v = if numeric major && numeric minor 
                 then Just (read major, read minor)
                 else Nothing
    where major = cleanWhite $ takeWhile (/= '.') v
          minor = cleanWhite $ (drop 1 . dropWhile (/= '.')) v

  ------------------------------------------------------------------------ 
  -- | convert 'HeartBeat' to 'String' 
  ------------------------------------------------------------------------
  beatToVal :: Heart -> String
  beatToVal (x, y) = (show x) ++ "," ++ (show y)

  ------------------------------------------------------------------------ 
  -- | convert 'String' to 'HeartBeat' 
  ------------------------------------------------------------------------
  valToBeat :: String -> Maybe Heart
  valToBeat s = if numeric send && numeric recv
                  then Just (read send, read recv)
                  else Nothing
    where send = (cleanWhite . takeWhile (/= ',')) s
          recv = (cleanWhite . drop 1 . dropWhile (/= ',')) s

  ------------------------------------------------------------------------ 
  -- | convert 'SrvDesc' to 'String' 
  ------------------------------------------------------------------------
  srvToStr :: SrvDesc -> String
  srvToStr (n, v, c) = n ++ "/" ++ v ++ c'
    where c' = if null c then "" else " " ++ c

  ------------------------------------------------------------------------ 
  -- | convert 'String' to 'SrvDesc' 
  ------------------------------------------------------------------------
  strToSrv :: String -> SrvDesc
  strToSrv s = (n, v, c)
    where n = takeWhile (/= '/') s
          v = takeWhile (/= ' ') $ drop (length n + 1) s
          c = drop 1 $ dropWhile (/= ' ') s

  ------------------------------------------------------------------------ 
  -- | remove headers (list of 'String') from list of 'Header'
  ------------------------------------------------------------------------
  rmHdrs :: [Header] -> [String] -> [Header]
  rmHdrs = foldl' rmHdr 

  ------------------------------------------------------------------------ 
  -- | remove header ('String') from list of 'Header'
  ------------------------------------------------------------------------
  rmHdr :: [Header] -> String -> [Header]
  rmHdr [] _ = []
  rmHdr ((k,v):hs) key | k == key  = rmHdr hs key
                       | otherwise = (k,v) : rmHdr hs key

  ------------------------------------------------------------------------
  -- | convert 'AckMode' to 'String'
  ------------------------------------------------------------------------
  ackToVal :: AckMode -> String
  ackToVal = show 

  ------------------------------------------------------------------------
  -- | convert 'String' to 'AckMode'
  ------------------------------------------------------------------------
  valToAck :: String -> Maybe AckMode
  valToAck s = if isValidAck s then Just $ read s else Nothing

  ------------------------------------------------------------------------
  -- | negotiates version - 
  --   if no common version is found,
  --   the function results in version 1.0!
  ------------------------------------------------------------------------
  negoVersion :: [Version] -> [Version] -> Version
  negoVersion bs cs = nego bs' cs
    where bs'  = sortBy desc bs
          desc = flip compare 
          nego []     _    = defVersion
          nego _     []    = defVersion
          nego (v:vs1) vs2 = if v `elem` vs2 then v else nego vs1 vs2

  ------------------------------------------------------------------------
  -- | negotiates heart-beat
  ------------------------------------------------------------------------
  negoBeat :: Heart -> Heart -> Heart
  negoBeat hc hs = 
    let x = if sndC == 0 then 0 else max sndC sndS
        y = if rcvC == 0 then 0 else max rcvC rcvS
    in (x, y)
    where sndC = fst hc
          rcvC = snd hc
          sndS = fst hs
          rcvS = snd hs

  ------------------------------------------------------------------------
  -- | sets the transaction header to an empty string;
  --   this is a useful function for brokers:
  --   when a transaction has been committed,
  --   the 'Send' messages can be handled by the same function
  --   without, accidentally, iterating into a new transaction.
  ------------------------------------------------------------------------
  resetTrans :: Frame -> Frame
  resetTrans f = f {frmTrans = ""}

  ------------------------------------------------------------------------
  -- | converts a 'Frame' into a 'B.ByteString'
  ------------------------------------------------------------------------
  putFrame :: Frame -> B.ByteString
  putFrame BeatFrame = putCommand mkBeat
  putFrame f         = putCommand f >|<
                       putHeaders f >|<
                       putBody    f

  ------------------------------------------------------------------------
  -- | converts a 'Frame' into a 'String'
  ------------------------------------------------------------------------
  toString :: Frame -> String
  toString = U.toString . putFrame

  ------------------------------------------------------------------------
  -- | converts the 'FrameType' into a 'B.ByteString'
  ------------------------------------------------------------------------
  putCommand :: Frame -> B.ByteString
  putCommand f = 
    let s = case typeOf f of
              Connect     -> "CONNECT"
              Connected   -> "CONNECTED"
              Disconnect  -> "DISCONNECT"
              Send        -> "SEND"
              Subscribe   -> "SUBSCRIBE"
              Unsubscribe -> "UNSUBSCRIBE"
              Begin       -> "BEGIN"
              Commit      -> "COMMIT"
              Abort       -> "ABORT"
              Ack         -> "ACK"
              Nack        -> "NACK"
              Message     -> "MESSAGE"
              Receipt     -> "RECEIPT"
              Error       -> "ERROR"
              HeartBeat   -> ""
    in B.pack (s ++ "\n")

  ------------------------------------------------------------------------
  -- Convert headers to ByteString
  ------------------------------------------------------------------------
  putHeaders :: Frame -> B.ByteString
  putHeaders f = 
    let hs = toHeaders f
        s  = B.concat $ map putHeader hs
    in  s |> '\n'

  ------------------------------------------------------------------------
  -- Convert header to ByteString
  ------------------------------------------------------------------------
  putHeader :: Header -> B.ByteString
  putHeader h =
    let k = fst h
        v = snd h
    in U.fromString $ k ++ ":" ++ v ++ "\n"

  ------------------------------------------------------------------------
  -- Convert Frame attributes to headers
  ------------------------------------------------------------------------
  toHeaders :: Frame -> [Header]
  -- Connect Frame -------------------------------------------------------
  toHeaders (ConFrame l p h b v i) = 
    [mkLogHdr l, mkPassHdr p, 
     mkAcVerHdr $ versToVal v, 
     mkBeatHdr  $ beatToVal b,
     mkHostHdr h, mkCliIdHdr i ]
  -- Connected Frame -----------------------------------------------------
  toHeaders (CondFrame s b v d) =
    [mkSesHdr s, 
     mkVerHdr  $ verToVal  v,
     mkBeatHdr $ beatToVal b,
     mkSrvHdr  $ srvToStr  d]
  -- Disconnect Frame -----------------------------------------------------
  toHeaders (DisFrame r) =
    if null r then [] else [mkRecHdr r]
  -- Subscribe Frame ------------------------------------------------------
  toHeaders (SubFrame d a s i r) =
    let ah = if a == Auto then [] else [mkAckHdr (show a)]
        sh = if null s then [] else [mkSelHdr s]
        rh = if null r then [] else [mkRecHdr r]
        ih = if null i then [] else [mkIdHdr i]
    in mkDestHdr d : (ah ++ sh ++ ih ++ rh)
  -- Unsubscribe Frame ----------------------------------------------------
  toHeaders (USubFrame d i r) =
    let ih = if null i then [] else [mkIdHdr i]
        dh = if null d then [] else [mkDestHdr d]
        rh = if null r then [] else [mkRecHdr r]
    in dh ++ ih ++ rh
  -- Send Frame -----------------------------------------------------------
  toHeaders (SndFrame h d t r l m _) = 
    let th = if null t then [] else [mkTrnHdr t]
        rh = if null r then [] else [mkRecHdr r]
        lh = if l <= 0 then [] else [mkLenHdr (show l)]
    in [mkDestHdr d, 
        mkMimeHdr (showType m)] ++ th ++ rh ++ lh ++ h
  -- Begin Frame -----------------------------------------------------------
  toHeaders (BgnFrame  t r) = 
    let rh = if null r then [] else [mkRecHdr r]
    in  [mkTrnHdr t] ++ rh
  -- Commit Frame -----------------------------------------------------------
  toHeaders (CmtFrame  t r) = 
    let rh = if null r then [] else [mkRecHdr r]
    in  [mkTrnHdr t] ++ rh
  -- Abort Frame -----------------------------------------------------------
  toHeaders (AbrtFrame t r) = 
    let rh = if null r then [] else [mkRecHdr r]
    in  [mkTrnHdr t] ++ rh
  -- Ack Frame --------------------------------------------------------------
  toHeaders (AckFrame i s t r) = 
    let sh = if null s then [] else [mkSubHdr s]
        rh = if null r then [] else [mkRecHdr r]
        th = if null t then [] else [mkTrnHdr t]
    in ([mkMIdHdr i] ++ th ++ sh ++ rh)
  -- Nack Frame -------------------------------------------------------------
  toHeaders (NackFrame i s t r) = 
    let sh = if null s then [] else [mkSubHdr s]
        rh = if null r then [] else [mkRecHdr r]
        th = if null t then [] else [mkTrnHdr t]
    in ([mkMIdHdr i] ++ th ++ sh ++ rh)
  -- Message Frame ----------------------------------------------------------
  toHeaders (MsgFrame h s d i l m _)  = 
    let sh = if null s then [] else [mkSubHdr  s]
        dh = if null d then [] else [mkDestHdr d]
        lh = if l <= 0 then [] else [mkLenHdr (show l)]
    in  [mkMIdHdr i,
         mkMimeHdr (showType m)] 
        ++ sh ++ dh ++ lh ++ h
  -- Receipt Frame ----------------------------------------------------------
  toHeaders (RecFrame  r) = [mkRecIdHdr r]
  -- Error Frame ------------------------------------------------------------
  toHeaders (ErrFrame m r l t _) = 
    let mh = if null m then [] else [mkMsgHdr m]
        rh = if null r then [] else [mkRecIdHdr r]
        lh = if l <  0 then [] else [mkLenHdr (show l)]
    in  mh ++ rh ++ lh ++ [mkMimeHdr $ showType t]
  -- Beat Frame --------------------------------------------------------------
  toHeaders BeatFrame = []

  ------------------------------------------------------------------------
  -- get body from frame
  ------------------------------------------------------------------------
  putBody :: Frame -> Body
  putBody f =
    case f of 
      SndFrame _ _ _ _ _ _ b -> b |> '\x00'
      ErrFrame _ _ _ _     b -> b |> '\x00'
      MsgFrame _ _ _ _ _ _ b -> b |> '\x00'
      _                    -> B.pack "\x00"

  ------------------------------------------------------------------------
  -- find a header and return it as string value (with default)
  ------------------------------------------------------------------------
  findStrHdr :: String -> String -> [Header] -> String
  findStrHdr h d hs = case lookup h hs of
                        Nothing -> d
                        Just x  -> x

  ------------------------------------------------------------------------
  -- | make 'Connect' frame
  ------------------------------------------------------------------------
  mkConFrame :: [Header] -> Either String Frame
  mkConFrame hs = 
    let l   = findStrHdr hdrLog   "" hs
        p   = findStrHdr hdrPass  "" hs
        h   = findStrHdr hdrHost  "" hs
        i   = findStrHdr hdrCliId "" hs
        eiB = case lookup hdrBeat hs of
                Nothing -> Right noBeat
                Just x  -> case valToBeat x of
                             Nothing -> Left  $ "Not a valid heart-beat: " ++ x
                             Just b  -> Right b
        eiVs = case lookup hdrAcVer hs of
                        Nothing -> Right []
                        Just v  -> 
                          case valToVers v of
                            Nothing -> Left $ "Not a valid version: " ++ v
                            Just x  -> Right x
     in case eiVs of
          Left  e  -> Left e
          Right vs -> case eiB of
                        Left e  -> Left e
                        Right b -> Right $ ConFrame l p h b vs i

  ------------------------------------------------------------------------
  -- | make 'Connected' frame
  ------------------------------------------------------------------------
  mkCondFrame :: [Header] -> Either String Frame
  mkCondFrame hs =
    let s   = findStrHdr hdrSes "0"       hs
        v   = findStrHdr hdrVer defVerStr hs
        d   = case lookup hdrSrv hs of
                Nothing -> noSrvDesc
                Just x  -> strToSrv x
        eiB = case lookup hdrBeat hs of
                Nothing -> Right noBeat
                Just x  -> case valToBeat x of
                             Nothing -> Left $ "Not a valid heart-beat: " ++ x
                             Just b  -> Right b
    in case valToVer v of
         Nothing -> Left $ "Not a valid version: " ++ v
         Just v' -> case eiB of 
                      Left  e -> Left e
                      Right b -> Right $ CondFrame s b v' d

  ------------------------------------------------------------------------
  -- | make 'Disconnect' frame
  ------------------------------------------------------------------------
  mkDisFrame :: [Header] -> Either String Frame
  mkDisFrame hs = 
    Right $ DisFrame (findStrHdr hdrRec "" hs)

  ------------------------------------------------------------------------
  -- | make 'Send' frame
  ------------------------------------------------------------------------
  mkSndFrame :: [Header] -> Int -> Body -> Either String Frame
  mkSndFrame hs l b =
    case lookup hdrDest hs of
      Nothing -> Left "No destination header in SEND Frame"
      Just d  -> Right $ SndFrame {
                           frmHdrs  = rmHdrs hs [hdrMime, hdrTrn, hdrRec,
                                                 hdrDest, hdrLen],
                           frmDest  = d,
                           frmLen   = l,
                           frmMime  = case lookup hdrMime hs of
                                        Nothing -> defMime
                                        Just t  -> 
                                          case parseMIMEType t of
                                            Nothing -> defMime
                                            Just m  -> m,
                           frmTrans = findStrHdr hdrTrn "" hs,
                           frmRec   = findStrHdr hdrRec "" hs,
                           frmBody  = b
                         }

  ------------------------------------------------------------------------
  -- | make 'Message' frame
  ------------------------------------------------------------------------
  mkMsgFrame :: [Header] -> Int -> Body -> Either String Frame
  mkMsgFrame hs l b =
    case lookup hdrDest hs of
      Nothing -> Left "No destination header in MESSAGE Frame"
      Just d  -> case lookup hdrMId hs of
                   Nothing -> Left "No message id in MESSAGE Frame"
                   Just i  ->
                     Right $ MsgFrame {
                               frmHdrs = rmHdrs hs [hdrSub, hdrMime, 
                                                    hdrLen, hdrDest,
                                                    hdrMId],
                               frmDest = d,
                               frmSub  = findStrHdr hdrSub "" hs,
                               frmId   = i, 
                               frmLen  = l,
                               frmMime = case lookup hdrMime hs of
                                           Nothing -> defMime
                                           Just t  -> 
                                             case parseMIMEType t of
                                               Nothing -> defMime
                                               Just m  -> m,
                               frmBody = b
                             }

  ------------------------------------------------------------------------
  -- | make 'Subscribe' frame
  ------------------------------------------------------------------------
  mkSubFrame :: [Header] -> Either String Frame
  mkSubFrame hs = 
    case lookup hdrDest hs of
      Nothing -> Left "No destination header in Subscribe Frame"
      Just d  -> case getAck hs of
                   Left  e -> Left e
                   Right a -> Right $ SubFrame {
                                        frmDest = d,
                                        frmAck  = a,
                                        frmSel  = findStrHdr hdrSel "" hs,
                                        frmId   = findStrHdr hdrId  "" hs,
                                        frmRec  = findStrHdr hdrRec "" hs}

  ------------------------------------------------------------------------
  -- | make 'Unsubscribe' frame
  ------------------------------------------------------------------------
  mkUSubFrame :: [Header] -> Either String Frame
  mkUSubFrame hs =
    case lookup hdrDest hs of
      Nothing -> case lookup hdrId hs of
                   Nothing -> Left $ "No destination and no id header " ++
                                     "in UnSubscribe Frame"
                   Just i  -> Right $ USubFrame {
                                        frmId   = i,
                                        frmDest = "",
                                        frmRec  = findStrHdr hdrRec "" hs}
      Just d  -> Right $ USubFrame {
                           frmId   = findStrHdr hdrId "" hs,
                           frmDest = d,
                           frmRec  = findStrHdr hdrRec "" hs}

  ------------------------------------------------------------------------
  -- | make 'Begin' frame
  ------------------------------------------------------------------------
  mkBgnFrame :: [Header] -> Either String Frame
  mkBgnFrame hs =
    case lookup hdrTrn hs of
      Nothing -> Left $ "No transation header in Begin Frame"
      Just t  -> Right $ BgnFrame {
                           frmTrans = t,
                           frmRec = findStrHdr hdrRec "" hs}

  ------------------------------------------------------------------------
  -- | make 'Commit' frame
  ------------------------------------------------------------------------
  mkCmtFrame :: [Header] -> Either String Frame
  mkCmtFrame hs =
    case lookup hdrTrn hs of
      Nothing -> Left $ "No transation header in Commit Frame"
      Just t  -> Right $ CmtFrame {
                           frmTrans = t,
                           frmRec = findStrHdr hdrRec "" hs}

  ------------------------------------------------------------------------
  -- | make 'Abort' frame
  ------------------------------------------------------------------------
  mkAbrtFrame :: [Header] -> Either String Frame
  mkAbrtFrame hs =
    case lookup hdrTrn hs of
      Nothing -> Left $ "No transation header in Abort Frame"
      Just t  -> Right $ AbrtFrame {
                           frmTrans = t,
                           frmRec = findStrHdr hdrRec "" hs}

  ------------------------------------------------------------------------
  -- | make 'Ack' frame
  ------------------------------------------------------------------------
  mkAckFrame :: [Header] -> Either String Frame
  mkAckFrame hs =
    case lookup hdrMId hs of
      Nothing -> Left $ "No message-id header in Ack Frame"
      Just i  -> let t = findStrHdr hdrTrn "" hs
                     s = findStrHdr hdrSub "" hs
                     r = findStrHdr hdrRec "" hs
                 in Right AckFrame {
                              frmId    = i,
                              frmSub   = s,
                              frmTrans = t,
                              frmRec   = r}

  ------------------------------------------------------------------------
  -- | make 'Nack' frame
  ------------------------------------------------------------------------
  mkNackFrame :: [Header] -> Either String Frame
  mkNackFrame hs =
    case lookup hdrMId hs of
      Nothing -> Left $ "No message-id header in Ack Frame"
      Just i  -> let t = findStrHdr hdrTrn "" hs
                     s = findStrHdr hdrSub "" hs
                     r = findStrHdr hdrRec "" hs
                 in Right NackFrame {
                              frmId    = i,
                              frmSub   = s,
                              frmTrans = t,
                              frmRec   = r}

  ------------------------------------------------------------------------
  -- | make 'Receipt' frame
  ------------------------------------------------------------------------
  mkRecFrame :: [Header] -> Either String Frame
  mkRecFrame hs =
    case lookup hdrRecId hs of
      Nothing -> Left $ "No receipt-id header in Receipt Frame"
      Just r  -> Right $ RecFrame r

  ------------------------------------------------------------------------
  -- | make 'Error' frame
  ------------------------------------------------------------------------
  mkErrFrame :: [Header] -> Int -> Body -> Either String Frame
  mkErrFrame hs l b =
    Right $ ErrFrame {
              frmMsg  = findStrHdr hdrMsg   "" hs,
              frmRec  = findStrHdr hdrRecId "" hs,
              frmLen  = l,
              frmMime = case lookup hdrMime hs of
                               Nothing -> defMime
                               Just t  -> 
                                 case parseMIMEType t of
                                   Nothing -> defMime
                                   Just x  -> x,
              frmBody = b}

  ------------------------------------------------------------------------
  -- | converts a 'Send' frame into a 'Message' frame;
  --   parameters:
  --   
  --   * message id
  --
  --   * subscription id
  --
  --   * The original 'Send' frame
  ------------------------------------------------------------------------
  sndToMsg :: String -> String -> Frame -> Maybe Frame
  sndToMsg i sub f = case typeOf f of
                       Send ->
                         Just MsgFrame {
                               frmHdrs = frmHdrs f,
                               frmDest = frmDest f,
                               frmSub  = sub, 
                               frmLen  = frmLen  f,
                               frmMime = frmMime f,
                               frmId   = i,
                               frmBody = frmBody f
                             }
                       _ -> Nothing

  ------------------------------------------------------------------------
  -- | converts a 'Connect' frame into a 'Connected' frame,
  --   negotiating heart-beats and version;
  --   parameters:
  --
  --   * server desc
  --
  --   * session id
  --
  --   * caller's bid for heart-beat 
  --
  --   * caller's supported versions
  --
  --   * the original 'Connect' frame
  ------------------------------------------------------------------------
  conToCond :: String -> String -> Heart -> [Version] -> Frame -> Maybe Frame
  conToCond s i b vs f = case typeOf f of
                          Connect ->
                            Just CondFrame {
                                   frmSes  = i,
                                   frmBeat = negoBeat (frmBeat f) b,
                                   frmVer  = negoVersion vs $ frmAcVer f,
                                   frmSrv  = strToSrv s
                                 }
                          _ -> Nothing

  ------------------------------------------------------------------------
  -- | Compliance with protocol version
  ------------------------------------------------------------------------
  complies :: Version -> Frame -> Bool
  complies v f = all (`elm` has) must 
    where must = getHdrs (typeOf f) v
          has  = toHeaders f
          elm  h hs = case lookup h hs of
                        Nothing -> False
                        Just _  -> True

  ------------------------------------------------------------------------
  -- Mandatory headers 
  ------------------------------------------------------------------------
  getHdrs :: FrameType -> Version -> [String]
  getHdrs t v =
    case t of
       Connect     -> case v of
                        (1,0) -> []
                        (1,1) -> ["host", "accept-version"]
                        _     -> []
       Connected   -> case v of
                        (1,0) -> ["session-id"]
                        (1,1) -> ["version"]
                        _     -> []
       Disconnect  -> []
       Subscribe   -> case v of
                        (1,0) -> ["destination"]
                        (1,1) -> ["id", "destination"]
                        _     -> []
       Unsubscribe -> case v of 
                        (1,0) -> ["destination"] -- either dest or id
                        (1,1) -> ["id"] 
                        _     -> []
       Send        -> case v of
                        (1,0) -> ["destination"]
                        (1,1) -> ["destination"]
                        _     -> []
       Message     -> case v of
                        (1,0) -> ["message-id", "destination"]
                        (1,1) -> ["message-id", "subscription", "destination"]
                        _     -> []
       Begin       -> ["transaction"] 
       Commit      -> ["transaction"] 
       Abort       -> ["transaction"] 
       Ack         -> case v of
                        (1,0) -> ["message-id"]
                        (1,1) -> ["message-id", "subscription"]
                        _     -> []
       Nack        -> case v of 
                        (1,1) -> ["message-id", "subscription"]
                        _     -> []
       Error       -> [] 
       Receipt     -> ["receipt-id"] 
       HeartBeat   -> []

