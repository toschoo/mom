module Network.Mom.Stompl.Frame (
                       Frame, Header, Body, AckMode(..), FrameType (..),
                       mkConFrame, mkCondFrame, 
                       mkSubFrame, mkUSubFrame, mkMsgFrame,
                       mkSndFrame, mkDisFrame,  mkErrFrame,
                       mkBgnFrame, mkCmtFrame,  mkAbrtFrame,
                       mkAckFrame, mkRecFrame,
                       sndToMsg,
                       typeOf, putFrame, toString,
                       upString, numeric,
                       getLen, getAck, isValidAck,
                       getLogin, getPasscode, getDest,
                       getLength, getTrans, getReceipt,
                       getSelector, getId, getAcknow,
                       getSession, getMsg, getBody,
                       resetTrans,
                       mkLogHdr, mkPassHdr, mkDestHdr, 
                       mkLenHdr, mkTrnHdr,  mkRecHdr, 
                       mkSelHdr, mkIdHdr,   mkAckHdr, 
                       mkSesHdr, mkMsgHdr, mkMIdHdr,  
                       (|>), (<|), (>|<))
where

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.Char (toUpper, isDigit)
  import           Data.List (find)

  type Header = (String, String)
  type Body   = B.ByteString

  hdrLog, hdrPass, hdrDest, hdrLen, hdrTrn, hdrRec,
    hdrSel, hdrId, hdrAck, hdrSes, hdrMsg, hdrMId :: String
  hdrLog  = "login"
  hdrPass = "passcode"
  hdrDest = "destination"
  hdrLen  = "content-length"
  hdrTrn  = "transaction"
  hdrRec  = "receipt"
  hdrSel  = "selector"
  hdrId   = "id"
  hdrAck  = "ack"
  hdrSes  = "session-id"
  hdrMsg  = "message"
  hdrMId  = "message-id"

  mkHeader :: String -> String -> Header
  mkHeader k v = (k, v)

  mkLogHdr, mkPassHdr, mkDestHdr, mkLenHdr, mkTrnHdr, mkMsgHdr, 
    mkRecHdr, mkSelHdr, mkMIdHdr, mkIdHdr, 
    mkAckHdr, mkSesHdr :: String -> Header
  mkLogHdr = mkHeader hdrLog
  mkPassHdr = mkHeader hdrPass
  mkDestHdr = mkHeader hdrDest
  mkLenHdr  = mkHeader hdrLen
  mkTrnHdr  = mkHeader hdrTrn
  mkRecHdr  = mkHeader hdrRec
  mkSelHdr  = mkHeader hdrSel
  mkIdHdr   = mkHeader hdrId
  mkMIdHdr  = mkHeader hdrMId
  mkAckHdr  = mkHeader hdrAck
  mkSesHdr  = mkHeader hdrSes
  mkMsgHdr  = mkHeader hdrMsg

  data Frame = ConFrame {
                   frmLogin :: String,
                   frmPass  :: String
                 }
               | CondFrame {
                   frmSes   :: String
                 }
               | SubFrame {
                   frmDest  :: String,
                   frmAck   :: AckMode,
                   frmSel   :: String,
                   frmId    :: String
                 }
               | USubFrame {
                   frmDest  :: String,
                   frmId    :: String
                 }
               | SndFrame {
                   frmHdrs  :: [Header],
                   frmDest  :: String,
                   frmTrans :: String,
                   frmRec   :: String,
                   frmLen   :: Int,
                   frmBody  :: Body}
               | DisFrame {
                   frmRec   :: String
                 }
               | BgnFrame {
                   frmTrans :: String
                 }
               | CmtFrame {
                   frmTrans :: String
                 }
               | AckFrame {
                   frmId    :: String,
                   frmDest  :: String,
                   frmTrans :: String
                 }
               | AbrtFrame {
                   frmTrans :: String
                 }
               | MsgFrame {
                   frmHdrs  :: [Header],
                   frmDest  :: String,
                   frmId    :: String,
                   frmLen   :: Int,
                   frmBody  :: Body}
               | RecFrame {
                   frmRec   :: String
                 }
               | ErrFrame {
                   frmMsg  :: String,
                   frmLen  :: Int,
                   frmBody :: Body}
    deriving (Show, Eq)

  getLogin, getPasscode, getSession, getId,
    getDest,  getTrans,    getReceipt, getMsg,
    getSelector :: Frame -> String
  getLength     :: Frame -> Int
  getAcknow     :: Frame -> AckMode
  getBody       :: Frame -> B.ByteString
  getLogin    = frmLogin 
  getPasscode = frmPass
  getSession  = frmSes
  getSelector = frmSel
  getTrans    = frmTrans 
  getMsg      = frmMsg
  getAcknow   = frmAck
  getDest     = frmDest
  getId       = frmId
  getReceipt  = frmRec
  getBody     = frmBody
  getLength   = frmLen

  resetTrans :: Frame -> Frame
  resetTrans f = f {frmTrans = ""}

  data FrameType = Connect   | Connected   | Disconnect | Message |
                   Subscribe | Unsubscribe | Send       | Error   |
                   Begin     | Commit      | Abort      | Ack     | Receipt
    deriving (Show, Read, Eq)

  typeOf :: Frame -> FrameType
  typeOf f = case f of
              (ConFrame  _ _        ) -> Connect
              (CondFrame _          ) -> Connected
              (DisFrame  _          ) -> Disconnect
              (SubFrame  _ _ _ _    ) -> Subscribe
              (USubFrame _ _        ) -> Unsubscribe
              (SndFrame  _ _ _ _ _ _) -> Send
              (BgnFrame  _          )  -> Begin
              (CmtFrame  _          ) -> Commit
              (AbrtFrame _          ) -> Abort
              (AckFrame  _ _ _      ) -> Ack
              (MsgFrame  _ _ _ _ _  ) -> Message
              (RecFrame  _          ) -> Receipt
              (ErrFrame  _ _ _      ) -> Error

  data AckMode = Auto | Client
    deriving (Eq)

  instance Show AckMode where
    show Auto   = "auto"
    show Client = "client"

  instance Read AckMode where
    readsPrec _ s = case upString s of
                     "AUTO"   -> [(Auto, "")]
                     "CLIENT" -> [(Client, "")]
                     _        -> error $ "Can't parse AckMode: " ++ s

  infixr >|<, |>, <| 
  (>|<) :: B.ByteString -> B.ByteString -> B.ByteString
  (|>)  :: B.ByteString ->   Char       -> B.ByteString
  (<|)  ::   Char       -> B.ByteString -> B.ByteString
  x >|< y = x `B.append` y
  x <|  y = x `B.cons` y
  x  |> y = x `B.snoc` y

  isValidAck :: String -> Bool
  isValidAck s = case find (== (upString s)) ["AUTO", "CLIENT"] of
                   Nothing -> False
                   Just _  -> True

  upString :: String -> String
  upString = map toUpper

  numeric :: String -> Bool
  numeric = and . map isDigit

  cleanWhite :: String -> String
  cleanWhite = 
    dropWhile (== ' ') . takeWhile (/= ' ')

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
                   else Left  $ "Invalid ack header in Subscribe Frame: " ++ a

  putFrame :: Frame -> B.ByteString
  putFrame f = 
    putCommand f >|<
    putHeaders f >|<
    putBody    f

  toString :: Frame -> String
  toString = U.toString . putFrame

  putCommand :: Frame -> B.ByteString
  putCommand f = 
    let s = case f of
              ConFrame  _ _         -> "CONNECT"
              CondFrame _           -> "CONNECTED"
              DisFrame  _           -> "DISCONNECT"
              SndFrame  _ _ _ _ _ _ -> "SEND"
              SubFrame  _ _ _ _     -> "SUBSCRIBE"
              USubFrame _ _         -> "UNSUBSCRIBE"
              BgnFrame  _           -> "BEGIN"
              CmtFrame  _           -> "COMMIT"
              AbrtFrame _           -> "ABORT"
              AckFrame  _ _ _       -> "ACK"
              MsgFrame  _ _ _ _ _   -> "MESSAGE"
              RecFrame  _           -> "RECEIPT"
              ErrFrame  _ _ _       -> "ERROR"
    in B.pack (s ++ "\n")

  putHeaders :: Frame -> B.ByteString
  putHeaders f = 
    let hs = toHeaders f
        s  = B.concat $ map putHeader hs
    in s |> '\n'

  putHeader :: Header -> B.ByteString
  putHeader h =
    let k = fst h
        v = snd h
    in B.pack $ k ++ ":" ++ v ++ "\n"

  toHeaders :: Frame -> [Header]
  toHeaders (ConFrame l p) = 
    [mkLogHdr l, mkPassHdr p]
  toHeaders (CondFrame s) =
    [mkSesHdr s]
  toHeaders (DisFrame r) =
    if null r then [] else [mkRecHdr r]
  toHeaders (SubFrame d a s i) =
    let ah = if a == Auto then [] else [mkAckHdr (show a)]
        sh = if null s then [] else [mkSelHdr s]
        ih = if null i then [] else [mkIdHdr i]
    in mkDestHdr d : (ah ++ sh ++ ih)
  toHeaders (USubFrame d i) =
    let ih = if null i then [] else [mkIdHdr i]
        dh = if null i then [] else [mkDestHdr d]
    in dh ++ ih
  toHeaders (SndFrame h _ _ _ _ _) = h
  toHeaders (BgnFrame  t) = [mkTrnHdr t]
  toHeaders (CmtFrame  t) = [mkTrnHdr t]
  toHeaders (AbrtFrame t) = [mkTrnHdr t]
  toHeaders (AckFrame i s t) = 
    let ih = if null i then [] else [mkIdHdr i]
        sh = if null s then [] else [mkDestHdr s]
    in mkTrnHdr t : (ih ++ sh)
  toHeaders (MsgFrame h _ _ _ _)  = h
  toHeaders (RecFrame  r) = [mkRecHdr r]
  toHeaders (ErrFrame m l _) = 
    let mh = if null m then [] else [mkMsgHdr m]
        lh = if l <  0 then [] else [mkLenHdr (show l)]
    in  mh ++ lh 

  putBody :: Frame -> Body
  putBody f =
    case f of 
      SndFrame _ _ _ _ _ b -> b |> '\x00'
      ErrFrame _ _       b -> b |> '\x00'
      MsgFrame _ _ _ _   b -> b |> '\x00'
      _                    -> B.pack "\x00"

  mkConFrame :: [Header] -> Either String Frame
  mkConFrame hs = 
    case lookup hdrLog  hs of
      Nothing -> Left "Missing login header in Connection Frame"
      Just l  -> case lookup hdrPass hs of
                   Nothing -> Left "Missing passcode header in Connection Frame"
                   Just p  -> Right $ ConFrame l p

  mkDisFrame :: [Header] -> Either String Frame
  mkDisFrame hs = 
    let r = case lookup hdrRec hs of
              Nothing -> ""
              Just s  -> s
    in Right $ DisFrame r

  mkSndFrame :: [Header] -> Int -> Body -> Either String Frame
  mkSndFrame hs l b =
    case lookup hdrDest hs of
      Nothing -> Left "No destination header in SEND Frame"
      Just d  -> Right $ SndFrame {
                           frmHdrs = hs,
                           frmDest = d,
                           frmLen  = l,
                           frmTrans = case lookup hdrTrn hs of
                                        Nothing -> ""
                                        Just t  ->  t,
                           frmRec   = case lookup hdrRec hs of
                                        Nothing -> ""
                                        Just r  -> r,
                           frmBody  = b
                         }

  mkSubFrame :: [Header] -> Either String Frame
  mkSubFrame hs = 
    case lookup hdrDest hs of
      Nothing -> Left "No destination header in Subscribe Frame"
      Just d  -> case getAck hs of
                   Left e -> Left e
                   Right a -> Right $ SubFrame {
                                        frmDest = d,
                                        frmAck  = a,
                                        frmSel  = case lookup hdrSel hs of
                                                     Nothing -> ""
                                                     Just s  ->  s,
                                        frmId   = case lookup hdrId hs of
                                                     Nothing -> ""
                                                     Just i  ->  i
                                      }
  mkUSubFrame :: [Header] -> Either String Frame
  mkUSubFrame hs =
    case lookup hdrDest hs of
      Nothing -> case lookup hdrId hs of
                   Nothing -> Left $ "No destination and no id header " ++
                                     "in UnSubscribe Frame"
                   Just i  -> Right $ USubFrame {
                                        frmId   = i,
                                        frmDest = ""}
      Just d  -> Right $ USubFrame {
                           frmId = "",
                           frmDest = d}

  mkBgnFrame :: [Header] -> Either String Frame
  mkBgnFrame hs =
    case lookup hdrTrn hs of
      Nothing -> Left $ "No transation header in Begin Frame"
      Just t  -> Right $ BgnFrame t

  mkCmtFrame :: [Header] -> Either String Frame
  mkCmtFrame hs =
    case lookup hdrTrn hs of
      Nothing -> Left $ "No transation header in Commit Frame"
      Just t  -> Right $ CmtFrame t

  mkAbrtFrame :: [Header] -> Either String Frame
  mkAbrtFrame hs =
    case lookup hdrTrn hs of
      Nothing -> Left $ "No transation header in Abort Frame"
      Just t  -> Right $ AbrtFrame t

  mkAckFrame :: [Header] -> Either String Frame
  mkAckFrame hs =
    case lookup hdrMId hs of
      Nothing -> Left $ "No message-id header in Ack Frame"
      Just i  -> let t = case lookup hdrTrn hs of
                           Nothing  -> ""
                           Just trn -> trn
                     s = case lookup hdrDest hs of -- mandatory!
                           Nothing -> ""
                           Just x  -> x
                 in Right AckFrame {
                              frmId    = i,
                              frmDest  = s,
                              frmTrans = t}

  mkMsgFrame :: [Header] -> Int -> Body -> Either String Frame
  mkMsgFrame hs l b =
    case lookup hdrDest hs of
      Nothing -> Left "No destination header in MESSAGE Frame"
      Just d  -> case lookup hdrMId hs of
                   Nothing -> Left "No message id in MESSAGE Frame"
                   Just i  ->
                     Right $ MsgFrame {
                               frmHdrs = hs,
                               frmDest = d,
                               frmId   = i, 
                               frmLen  = l,
                               frmBody = b
                             }

  mkRecFrame :: [Header] -> Either String Frame
  mkRecFrame hs =
    case lookup hdrRec hs of
      Nothing -> Left $ "No receipt header in Receipt Frame"
      Just r  -> Right $ RecFrame r

  mkCondFrame :: [Header] -> Either String Frame
  mkCondFrame hs =
    case lookup hdrSes hs of
      Nothing -> Left "No session-id header in Connected Frame"
      Just s  -> Right $ CondFrame s

  mkErrFrame :: [Header] -> Int -> Body -> Either String Frame
  mkErrFrame hs l b =
    case lookup hdrMsg hs of
      Nothing -> Left "No message header in Error Frame"
      Just m  -> Right $ ErrFrame {
                           frmMsg  = m,
                           frmLen  = l,
                           frmBody = b}

  sndToMsg :: String -> Frame -> Maybe Frame
  sndToMsg i f = if typeOf f == Send
                   then Just MsgFrame {
                               frmHdrs = frmHdrs f,
                               frmDest = frmDest f,
                               frmLen  = frmLen  f,
                               frmId   = i,
                               frmBody = frmBody f
                             }
                   else Nothing
  

