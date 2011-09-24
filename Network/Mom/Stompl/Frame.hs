module Network.Mom.Stompl.Frame (
                       Frame, Header, Body, Heart, Version,
                       AckMode(..), FrameType (..),
                       mkConFrame, mkCondFrame, 
                       mkSubFrame, mkUSubFrame, mkMsgFrame,
                       mkSndFrame, mkDisFrame,  mkErrFrame,
                       mkBgnFrame, mkCmtFrame,  mkAbrtFrame,
                       mkAckFrame, mkRecFrame,
                       sndToMsg,
                       valToVer, valToVers, verToVal, versToVal,
                       beatToVal, valToBeat,
                       typeOf, putFrame, toString,
                       upString, numeric,
                       getLen, getAck, isValidAck,
                       getLogin, getPasscode, getDest,
                       getLength, getTrans, getReceipt,
                       getSelector, getId, getAcknow,
                       getHost, getVersions, getVersion,
                       getSession, getMsg, getBody, getMime,
                       getBeat, getHeaders,
                       resetTrans,
                       mkLogHdr,   mkPassHdr, mkDestHdr, 
                       mkLenHdr,   mkTrnHdr,  mkRecHdr, 
                       mkSelHdr,   mkIdHdr,   mkAckHdr, 
                       mkSesHdr,   mkMsgHdr,  mkMIdHdr,
                       mkAcVerHdr, mkVerHdr,  mkHostHdr,
                       mkBeatHdr,  mkMimeHdr,
                       (|>), (<|), (>|<))
where

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.Char (toUpper, isDigit)
  import           Data.List (find)
  import           Data.List.Split (splitWhen)
  import           Data.Maybe (catMaybes)
  -- import qualified Codec.MIME.Type as Mi

  type Header = (String, String)
  type Body   = B.ByteString

  type Version = (Int, Int)
  type Heart   = (Int, Int)

  noBeat :: Heart
  noBeat = (0,0)

  defMime :: String
  defMime = "text/plain"

  hdrLog, hdrPass, hdrDest, hdrLen, hdrTrn, hdrRec,
    hdrSel, hdrId, hdrAck, hdrSes, hdrMsg, hdrMId, 
    hdrAcVer, hdrVer, hdrBeat, hdrHost, hdrMime :: String
  hdrLog   = "login"
  hdrPass  = "passcode"
  hdrDest  = "destination"
  hdrLen   = "content-length"
  hdrMime  = "content-type"
  hdrTrn   = "transaction"
  hdrRec   = "receipt"
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

  mkHeader :: String -> String -> Header
  mkHeader k v = (k, v)

  mkLogHdr, mkPassHdr, mkDestHdr, mkLenHdr, mkMimeHdr, mkTrnHdr, mkMsgHdr, 
    mkRecHdr, mkSelHdr, mkMIdHdr, mkIdHdr, 
    mkAckHdr, mkSesHdr, mkAcVerHdr, mkVerHdr, 
    mkHostHdr, mkBeatHdr :: String -> Header
  mkLogHdr   = mkHeader hdrLog
  mkPassHdr  = mkHeader hdrPass
  mkDestHdr  = mkHeader hdrDest
  mkLenHdr   = mkHeader hdrLen
  mkMimeHdr  = mkHeader hdrMime
  mkTrnHdr   = mkHeader hdrTrn
  mkRecHdr   = mkHeader hdrRec
  mkSelHdr   = mkHeader hdrSel
  mkIdHdr    = mkHeader hdrId
  mkMIdHdr   = mkHeader hdrMId
  mkAckHdr   = mkHeader hdrAck
  mkSesHdr   = mkHeader hdrSes
  mkMsgHdr   = mkHeader hdrMsg
  mkVerHdr   = mkHeader hdrVer
  mkAcVerHdr = mkHeader hdrAcVer
  mkHostHdr  = mkHeader hdrHost
  mkBeatHdr  = mkHeader hdrBeat

  data Frame = ConFrame {
                   frmLogin :: String,
                   frmPass  :: String,
                   frmHost  :: String,
                   frmBeat  :: Heart,
                   frmAcVer :: [Version] -- 1.1
                 }
               | CondFrame {
                   frmSes   :: String,
                   frmBeat  :: Heart,
                   frmVer   :: Version -- 1.1
                   -- server name: what for?
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
                   frmMime  :: String, -- MimeType?
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
                   frmMime  :: String, -- MimeType?
                   frmBody  :: Body}
               | RecFrame {
                   frmRec   :: String
                 }
               | ErrFrame {
                   frmMsg  :: String,
                   frmLen  :: Int,
                   frmMime :: String, -- MimeType?
                   frmBody :: Body}
    deriving (Show, Eq)

  getLogin, getPasscode, getSession, getId,
    getDest,  getTrans,    getReceipt, getMsg,
    getSelector, getHost, getMime :: Frame -> String
  getLength     :: Frame -> Int
  getAcknow     :: Frame -> AckMode
  getBody       :: Frame -> B.ByteString
  getVersion    :: Frame -> Version
  getVersions   :: Frame -> [Version]
  getBeat       :: Frame -> Heart
  getHeaders    :: Frame -> [Header]
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
  getMime     = frmMime
  getVersion  = frmVer
  getVersions = frmAcVer
  getHost     = frmHost
  getBeat     = frmBeat
  getHeaders  = frmHdrs

  resetTrans :: Frame -> Frame
  resetTrans f = f {frmTrans = ""}

  data FrameType = Connect   | Connected   | Disconnect | Message |
                   Subscribe | Unsubscribe | Send       | Error   |
                   Begin     | Commit      | Abort      | Ack     | Receipt
    deriving (Show, Read, Eq)

  typeOf :: Frame -> FrameType
  typeOf f = case f of
              (ConFrame  _ _ _ _ _     ) -> Connect
              (CondFrame _ _ _         ) -> Connected
              (DisFrame  _             ) -> Disconnect
              (SubFrame  _ _ _ _       ) -> Subscribe
              (USubFrame _ _           ) -> Unsubscribe
              (SndFrame  _ _ _ _ _ _ _ ) -> Send
              (BgnFrame  _             ) -> Begin
              (CmtFrame  _             ) -> Commit
              (AbrtFrame _             ) -> Abort
              (AckFrame  _ _ _         ) -> Ack
              (MsgFrame  _ _ _ _ _ _   ) -> Message
              (RecFrame  _             ) -> Receipt
              (ErrFrame  _ _ _ _       ) -> Error

  data AckMode = Auto | Client | ClientIndi
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
                   else Left  $ "Invalid ack header in Subscribe Frame: " ++ a

  versToVal :: [Version] -> String
  versToVal = foldr addVer "" . map verToVal 
    where addVer v vs = if not $ null vs
                          then v ++ "," ++ vs
                          else v

  verToVal :: Version -> String
  verToVal (major, minor) = (show major) ++ "." ++ (show minor)

  valToVers :: String -> Maybe [Version]
  valToVers s = case find (== Nothing) vs of
                  Nothing -> Just $ catMaybes vs
                  Just x  -> Nothing
    where ss = splitWhen (== ',') s 
          vs = map valToVer ss

  valToVer :: String -> Maybe Version
  valToVer v = if numeric major && numeric minor 
                 then Just (read major, read minor)
                 else Nothing
    where major = cleanWhite $ takeWhile (/= '.') v
          minor = cleanWhite $ (drop 1 . dropWhile (/= '.')) v

  beatToVal :: Heart -> String
  beatToVal (x, y) = (show x) ++ "," ++ (show y)

  valToBeat :: String -> Maybe Heart
  valToBeat s = if numeric send && numeric recv
                  then Just (read send, read recv)
                  else Nothing
    where send = (cleanWhite . takeWhile (/= ',')) s
          recv = (cleanWhite . drop 1 . dropWhile (/= ',')) s

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
              ConFrame  _ _ _ _ _     -> "CONNECT"
              CondFrame _ _ _         -> "CONNECTED"
              DisFrame  _             -> "DISCONNECT"
              SndFrame  _ _ _ _ _ _ _ -> "SEND"
              SubFrame  _ _ _ _       -> "SUBSCRIBE"
              USubFrame _ _           -> "UNSUBSCRIBE"
              BgnFrame  _             -> "BEGIN"
              CmtFrame  _             -> "COMMIT"
              AbrtFrame _             -> "ABORT"
              AckFrame  _ _ _         -> "ACK"
              MsgFrame  _ _ _ _ _ _   -> "MESSAGE"
              RecFrame  _             -> "RECEIPT"
              ErrFrame  _ _ _ _       -> "ERROR"
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
  toHeaders (ConFrame l p h b v) = 
    [mkLogHdr l, mkPassHdr p, 
     mkAcVerHdr $ versToVal v, 
     mkBeatHdr  $ beatToVal b,
     mkHostHdr h]
  toHeaders (CondFrame s b v) =
    [mkSesHdr s, 
     mkVerHdr  $ verToVal v,
     mkBeatHdr $ beatToVal v]
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
  toHeaders (SndFrame h _ _ _ _ _ _) = h
  toHeaders (BgnFrame  t) = [mkTrnHdr t]
  toHeaders (CmtFrame  t) = [mkTrnHdr t]
  toHeaders (AbrtFrame t) = [mkTrnHdr t]
  toHeaders (AckFrame i s t) = 
    let ih = if null i then [] else [mkIdHdr i]
        sh = if null s then [] else [mkDestHdr s]
    in mkTrnHdr t : (ih ++ sh)
  toHeaders (MsgFrame h _ _ _ _ _)  = h
  toHeaders (RecFrame  r) = [mkRecHdr r]
  toHeaders (ErrFrame m l t _) = 
    let mh = if null m then [] else [mkMsgHdr m]
        th = if null t then [] else [mkMimeHdr t]
        lh = if l <  0 then [] else [mkLenHdr (show l)]
    in  mh ++ lh 

  putBody :: Frame -> Body
  putBody f =
    case f of 
      SndFrame _ _ _ _ _ _ b -> b |> '\x00'
      ErrFrame _ _ _       b -> b |> '\x00'
      MsgFrame _ _ _ _ _   b -> b |> '\x00'
      _                    -> B.pack "\x00"

  mkConFrame :: [Header] -> Either String Frame
  mkConFrame hs = 
    case lookup hdrLog  hs of
      Nothing -> Left "Missing login header in Connection Frame"
      Just l  -> case lookup hdrPass hs of
                   Nothing -> Left "Missing passcode header in Connection Frame"
                   Just p  -> 
                     let eiVs = case lookup hdrAcVer hs of
                                  Nothing -> Right []
                                  Just v  -> 
                                    case valToVers v of
                                      Nothing -> Left $ "Not a valid version: " ++ v
                                      Just x  -> Right x
                         eiB  = case lookup hdrBeat hs of
                                  Nothing -> Right noBeat
                                  Just  b -> case valToBeat b of
                                               Nothing -> Left $ "Not a valid heart-beat: " ++ b
                                               Just x  -> Right x
                         h    = case lookup hdrHost hs of
                                  Nothing -> ""
                                  Just x  -> x 
                     in case eiVs of
                          Left  e  -> Left e
                          Right vs -> case eiB of
                                        Left e  -> Left e
                                        Right b -> Right $ ConFrame l p h b vs 

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
                           frmHdrs  = hs,
                           frmDest  = d,
                           frmLen   = l,
                           frmMime  = case lookup hdrMime hs of
                                        Nothing -> ""
                                        Just t  -> t,
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
                               frmMime = case lookup hdrMime hs of
                                           Nothing -> ""
                                           Just t  -> t,
                               frmBody = b
                             }

  mkRecFrame :: [Header] -> Either String Frame
  mkRecFrame hs =
    case lookup hdrRec hs of
      Nothing -> Left $ "No receipt header in Receipt Frame"
      Just r  -> Right $ RecFrame r

  mkCondFrame :: [Header] -> Either String Frame
  mkCondFrame hs =
    let s   = case lookup hdrSes hs of
                Nothing -> "0"
                Just x  -> x
        v   = case lookup hdrVer hs of
                Nothing -> "1.0"
                Just x  -> x 
        eiB = case lookup hdrBeat hs of
                Nothing -> Right noBeat
                Just x  -> case valToBeat x of
                             Nothing -> Left $ "Not a valid heart-beat: " ++ x
                             Just b  -> Right b
    in case valToVer v of
         Nothing -> Left $ "Not a valid version: " ++ v
         Just v' -> case eiB of 
                      Left  e -> Left e
                      Right b -> Right $ CondFrame s b v'

  mkErrFrame :: [Header] -> Int -> Body -> Either String Frame
  mkErrFrame hs l b =
    case lookup hdrMsg hs of
      Nothing -> Left "No message header in Error Frame"
      Just m  -> Right $ ErrFrame {
                           frmMsg  = m,
                           frmLen  = l,
                           frmMime = case lookup hdrMime hs of
                                       Nothing -> defMime
                                       Just t  -> t,
                           frmBody = b}

  sndToMsg :: String -> Frame -> Maybe Frame
  sndToMsg i f = if typeOf f == Send
                   then Just MsgFrame {
                               frmHdrs = frmHdrs f,
                               frmDest = frmDest f,
                               frmLen  = frmLen  f,
                               frmMime = frmMime f,
                               frmId   = i,
                               frmBody = frmBody f
                             }
                   else Nothing
  

