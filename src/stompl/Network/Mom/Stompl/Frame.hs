module Network.Mom.Stompl.Frame (
                       Frame, Header, Body, Heart, Version,
                       AckMode(..), FrameType (..),
                       mkConFrame, mkCondFrame, 
                       mkSubFrame, mkUSubFrame, mkMsgFrame,
                       mkSndFrame, mkDisFrame,  mkErrFrame,
                       mkBgnFrame, mkCmtFrame,  mkAbrtFrame,
                       mkAckFrame, mkNackFrame, mkRecFrame,
                       mkConnect, mkConnected, 
                       mkSubscribe, mkUnsubscribe, mkMessage,
                       mkSend, mkDisconnect,  mkErr,
                       mkBegin, mkCommit,  mkAbort,
                       mkAck, mkNack, mkReceipt,
                       mkBeat,
                       sndToMsg, conToCond,
                       valToVer, valToVers, verToVal, versToVal,
                       beatToVal, valToBeat,
                       ackToVal, valToAck,
                       strToSrv, srvToStr,
                       typeOf, putFrame, toString, putCommand,
                       upString, numeric,
                       getLen, getAck, isValidAck,
                       getLogin, getPasscode, getDest, getSub,
                       getLength, getTrans, getReceipt,
                       getSelector, getId, getAcknow,
                       getHost, getVersions, getVersion,
                       getSession, getMsg, getBody, getMime,
                       getBeat, getServer, getHeaders,
                       getSrvName, getSrvVer, getSrvCmts,
                       resetTrans,
                       mkLogHdr,   mkPassHdr, mkDestHdr, 
                       mkLenHdr,   mkTrnHdr,  mkRecHdr, 
                       mkSelHdr,   mkIdHdr,   mkAckHdr, 
                       mkSesHdr,   mkMsgHdr,  mkMIdHdr,
                       mkAcVerHdr, mkVerHdr,  mkHostHdr,
                       mkBeatHdr,  mkMimeHdr, mkSrvHdr,
                       mkSubHdr,
                       (|>), (<|), (>|<))
where

  -- Todo:
  -- - conformance to protocol

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.Char (toUpper, isDigit)
  import           Data.List (find, foldl')
  import           Data.List.Split (splitWhen)
  import           Data.Maybe (catMaybes)
  import           Codec.MIME.Type as Mime (showType, Type, nullType)
  import           Codec.MIME.Parse        (parseMIMEType)

  type Header = (String, String)
  type Body   = B.ByteString

  type Version = (Int, Int)
  
  maxVers :: [Version] -> Version
  maxVers = foldr maxVer (1,0)

  maxVer :: Version -> Version -> Version
  maxVer v1 v2 = 
    if major1 > major2        then v1
      else if major1 < major2 then v2
             else if minor1 >= minor2 then v1 
                                      else v2 
    where major1 = fst v1
          minor1 = snd v1
          major2 = fst v2
          minor2 = snd v2

  type Heart   = (Int, Int)

  type SrvDesc = (String, String, String)

  getSrvName :: SrvDesc -> String
  getSrvName (n, _, _) = n

  getSrvVer :: SrvDesc -> String
  getSrvVer  (_, v, _) = v

  getSrvCmts :: SrvDesc -> String
  getSrvCmts (_, _, c) = c

  srvToStr :: SrvDesc -> String
  srvToStr (n, v, c) = n ++ "/" ++ v ++ c'
    where c' = if null c then "" else " " ++ c

  strToSrv :: String -> SrvDesc
  strToSrv s = (n, v, c)
    where n = takeWhile (/= '/') s
          v = takeWhile (/= ' ') $ drop (length n + 1) s
          c = drop 1 $ dropWhile (/= ' ') s

  noBeat :: Heart
  noBeat = (0,0)

  defMime :: Mime.Type
  defMime =  Mime.nullType

  defVerStr :: String
  defVerStr = "1.1"

  defVersion :: Version
  defVersion = (1, 1)

  noSrvDesc :: SrvDesc
  noSrvDesc = ("","","")

  hdrLog, hdrPass, hdrDest, hdrSub, hdrLen, hdrTrn, hdrRec, hdrRecId,
    hdrSel, hdrId, hdrAck, hdrSes, hdrMsg, hdrMId, hdrSrv,
    hdrAcVer, hdrVer, hdrBeat, hdrHost, hdrMime :: String
  hdrLog   = "login"
  hdrPass  = "passcode"
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

  mkLogHdr, mkPassHdr, mkDestHdr, mkLenHdr, mkMimeHdr, mkTrnHdr, mkMsgHdr, 
    mkRecHdr, mkRecIdHdr, mkSelHdr, mkMIdHdr, mkIdHdr, mkSubHdr, mkSrvHdr,
    mkAckHdr, mkSesHdr, mkAcVerHdr, mkVerHdr, 
    mkHostHdr, mkBeatHdr :: String -> Header
  mkLogHdr   = mkHeader hdrLog
  mkPassHdr  = mkHeader hdrPass
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
                   frmVer   :: Version, -- 1.1
                   frmSrv   :: SrvDesc
                   -- server name: what for?
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
                   frmLen  :: Int,
                   frmMime :: Mime.Type,
                   frmBody :: Body}
               | BeatFrame
    deriving (Show, Eq)

  mkConnect :: String -> String -> String -> Heart -> [Version] -> Frame
  mkConnect usr pwd hst beat vers =
    ConFrame {
       frmLogin = usr,
       frmPass  = pwd,
       frmHost  = hst,
       frmBeat  = beat,
       frmAcVer = vers}

  mkConnected :: String -> Heart -> Version -> SrvDesc -> Frame
  mkConnected ses beat ver srv =
    CondFrame {
      frmSes  = ses,
      frmBeat = beat,
      frmVer  = ver, 
      frmSrv  = srv}

  mkSubscribe :: String -> AckMode -> String -> String -> String -> Frame
  mkSubscribe dst ack sel sid rc =
    SubFrame {
      frmDest = dst,
      frmAck  = ack,
      frmSel  = sel,
      frmId   = sid,
      frmRec  = rc}

  mkUnsubscribe :: String -> String -> String -> Frame
  mkUnsubscribe dst sid rc =
    USubFrame {
      frmDest = dst,
      frmId   = sid,
      frmRec  = rc}
 
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

  mkDisconnect :: String -> Frame
  mkDisconnect rec = DisFrame rec

  mkReceipt :: String -> Frame
  mkReceipt rec = RecFrame rec

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

  mkErr :: String -> Mime.Type -> Int -> Body -> Frame
  mkErr mid mime len bdy =
    ErrFrame {
      frmMsg  = mid,
      frmLen  = len,
      frmMime = mime,
      frmBody = bdy}

  mkBeat :: Frame
  mkBeat = BeatFrame

  mkBegin  :: String -> String -> Frame
  mkBegin = BgnFrame

  mkCommit :: String -> String -> Frame
  mkCommit = CmtFrame

  mkAbort  :: String -> String -> Frame
  mkAbort = AbrtFrame

  mkAck :: String -> String -> String -> String -> Frame
  mkAck mid sid trn rc = AckFrame {
                           frmId    = mid,
                           frmSub   = sid,
                           frmTrans = trn,
                           frmRec   = rc}

  mkNack :: String -> String -> String -> String -> Frame
  mkNack mid sid trn rc = NackFrame {
                            frmId    = mid,
                            frmSub   = sid,
                            frmTrans = trn,
                            frmRec   = rc}

  getLogin, getPasscode, getSession, getId,
    getDest,  getTrans,  getSub, getReceipt, getMsg,
    getSelector, getHost :: Frame -> String
  getLength     :: Frame -> Int
  getAcknow     :: Frame -> AckMode
  getBody       :: Frame -> B.ByteString
  getVersion    :: Frame -> Version
  getVersions   :: Frame -> [Version]
  getBeat       :: Frame -> Heart
  getServer     :: Frame -> SrvDesc
  getHeaders    :: Frame -> [Header]
  getMime       :: Frame -> Mime.Type
  getLogin    = frmLogin 
  getPasscode = frmPass
  getSession  = frmSes
  getServer   = frmSrv
  getSelector = frmSel
  getTrans    = frmTrans 
  getMsg      = frmMsg
  getAcknow   = frmAck
  getDest     = frmDest
  getSub      = frmSub
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
                   Subscribe | Unsubscribe | Send       | Error   | Receipt |
                   Begin     | Commit      | Abort      | Ack     | Nack    |
                   HeartBeat
    deriving (Show, Read, Eq)

  typeOf :: Frame -> FrameType
  typeOf f = case f of
              (ConFrame  _ _ _ _ _     ) -> Connect
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
              (ErrFrame  _ _ _ _       ) -> Error
              (BeatFrame               ) -> HeartBeat

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

  valToAck :: String -> Maybe AckMode
  valToAck s = if isValidAck s
                 then Just $ read s
                 else Nothing

  ackToVal :: AckMode -> String
  ackToVal = show 
                   

  infixr >|<, |>, <| 
  (>|<) :: B.ByteString -> B.ByteString -> B.ByteString
  (|>)  :: B.ByteString ->   Char       -> B.ByteString
  (<|)  ::   Char       -> B.ByteString -> B.ByteString
  x >|< y = x `B.append` y
  x <|  y = x `B.cons` y
  x  |> y = x `B.snoc` y

  isValidAck :: String -> Bool
  isValidAck s = case find (== (upString s)) 
                      ["AUTO", "CLIENT", "CLIENT-INDIVIDUAL"] of
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
                  Just _  -> Nothing
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

  rmHdrs :: [Header] -> [String] -> [Header]
  rmHdrs = foldl' rmHdr 

  rmHdr :: [Header] -> String -> [Header]
  rmHdr [] _ = []
  rmHdr ((k,v):hs) key | k == key  = rmHdr hs key
                       | otherwise = (k,v) : rmHdr hs key

  putFrame :: Frame -> B.ByteString
  putFrame BeatFrame = putCommand mkBeat
  putFrame f         = putCommand f >|<
                       putHeaders f >|<
                       putBody    f

  toString :: Frame -> String
  toString = U.toString . putFrame

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

  putHeaders :: Frame -> B.ByteString
  putHeaders f = 
    let hs = toHeaders f
        s  = B.concat $ map putHeader hs
    in  s |> '\n'

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
  toHeaders (CondFrame s b v d) =
    [mkSesHdr s, 
     mkVerHdr  $ verToVal  v,
     mkBeatHdr $ beatToVal b,
     mkSrvHdr  $ srvToStr  d]
  toHeaders (DisFrame r) =
    if null r then [] else [mkRecHdr r]
  toHeaders (SubFrame d a s i r) =
    let ah = if a == Auto then [] else [mkAckHdr (show a)]
        sh = if null s then [] else [mkSelHdr s]
        rh = if null r then [] else [mkRecHdr r]
        ih = if null i then [] else [mkIdHdr i]
    in mkDestHdr d : (ah ++ sh ++ ih ++ rh)
  toHeaders (USubFrame d i r) =
    let ih = if null i then [] else [mkIdHdr i]
        dh = if null d then [] else [mkDestHdr d]
        rh = if null r then [] else [mkRecHdr r]
    in dh ++ ih ++ rh
  toHeaders (SndFrame h d t r l m _) = 
    let th = if null t then [] else [mkTrnHdr t]
        rh = if null r then [] else [mkRecHdr r]
        lh = if l <= 0 then [] else [mkLenHdr (show l)]
    in [mkDestHdr d, 
        mkMimeHdr (showType m)] ++ th ++ rh ++ lh ++ h
  toHeaders (BgnFrame  t r) = 
    let rh = if null r then [] else [mkRecHdr r]
    in  [mkTrnHdr t] ++ rh
  toHeaders (CmtFrame  t r) = 
    let rh = if null r then [] else [mkRecHdr r]
    in  [mkTrnHdr t] ++ rh
  toHeaders (AbrtFrame t r) = 
    let rh = if null r then [] else [mkRecHdr r]
    in  [mkTrnHdr t] ++ rh
  toHeaders (AckFrame i s t r) = 
    let sh = if null s then [] else [mkSubHdr s]
        rh = if null r then [] else [mkRecHdr r]
        th = if null t then [] else [mkTrnHdr t]
    in ([mkMIdHdr i] ++ th ++ sh ++ rh)
  toHeaders (NackFrame i s t r) = 
    let sh = if null s then [] else [mkSubHdr s]
        rh = if null r then [] else [mkRecHdr r]
        th = if null t then [] else [mkTrnHdr t]
    in ([mkMIdHdr i] ++ th ++ sh ++ rh)
  toHeaders (MsgFrame h s d i l m _)  = 
    let sh = if null s then [] else [mkSubHdr  s]
        dh = if null d then [] else [mkDestHdr d]
        lh = if l <= 0 then [] else [mkLenHdr (show l)]
    in  [mkMIdHdr i,
         mkMimeHdr (showType m)] 
        ++ sh ++ dh ++ lh ++ h
  toHeaders (RecFrame  r) = [mkRecIdHdr r]
  toHeaders (ErrFrame m l t _) = 
    let mh = if null m then [] else [mkMsgHdr m]
        lh = if l <  0 then [] else [mkLenHdr (show l)]
    in  mh ++ lh ++ [mkMimeHdr $ showType t]
  toHeaders BeatFrame = []

  putBody :: Frame -> Body
  putBody f =
    case f of 
      SndFrame _ _ _ _ _ _ b -> b |> '\x00'
      ErrFrame _ _ _       b -> b |> '\x00'
      MsgFrame _ _ _ _ _ _ b -> b |> '\x00'
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
                   Left  e -> Left e
                   Right a -> Right $ SubFrame {
                                        frmDest = d,
                                        frmAck  = a,
                                        frmSel  = case lookup hdrSel hs of
                                                     Nothing -> ""
                                                     Just s  ->  s,
                                        frmId   = case lookup hdrId hs of
                                                     Nothing -> ""
                                                     Just i  ->  i,
                                        frmRec  = case lookup hdrRec hs of
                                                   Nothing -> ""
                                                   Just r  -> r} 
  mkUSubFrame :: [Header] -> Either String Frame
  mkUSubFrame hs =
    case lookup hdrDest hs of
      Nothing -> case lookup hdrId hs of
                   Nothing -> Left $ "No destination and no id header " ++
                                     "in UnSubscribe Frame"
                   Just i  -> Right $ USubFrame {
                                        frmId   = i,
                                        frmDest = "",
                                        frmRec  = case lookup hdrRec hs of
                                                   Nothing -> ""
                                                   Just r  -> r} 
      Just d  -> Right $ USubFrame {
                           frmId   = case lookup hdrId hs of
                                       Nothing -> ""
                                       Just i  -> i,
                           frmDest = d,
                           frmRec  = case lookup hdrRec hs of
                                      Nothing -> ""
                                      Just r  -> r} 

  mkBgnFrame :: [Header] -> Either String Frame
  mkBgnFrame hs =
    case lookup hdrTrn hs of
      Nothing -> Left $ "No transation header in Begin Frame"
      Just t  -> Right $ BgnFrame {
                           frmTrans = t,
                           frmRec = case lookup hdrRec hs of
                                      Nothing -> ""
                                      Just r  -> r} 

  mkCmtFrame :: [Header] -> Either String Frame
  mkCmtFrame hs =
    case lookup hdrTrn hs of
      Nothing -> Left $ "No transation header in Commit Frame"
      Just t  -> Right $ CmtFrame {
                           frmTrans = t,
                           frmRec = case lookup hdrRec hs of
                                      Nothing -> ""
                                      Just r  -> r} 

  mkAbrtFrame :: [Header] -> Either String Frame
  mkAbrtFrame hs =
    case lookup hdrTrn hs of
      Nothing -> Left $ "No transation header in Abort Frame"
      Just t  -> Right $ AbrtFrame {
                           frmTrans = t,
                           frmRec = case lookup hdrRec hs of
                                      Nothing -> ""
                                      Just r  -> r} 

  mkAckFrame :: [Header] -> Either String Frame
  mkAckFrame hs =
    case lookup hdrMId hs of
      Nothing -> Left $ "No message-id header in Ack Frame"
      Just i  -> let t = case lookup hdrTrn hs of
                           Nothing  -> ""
                           Just trn -> trn
                     s = case lookup hdrSub hs of -- mandatory!
                           Nothing -> ""
                           Just x  -> x
                 in Right AckFrame {
                              frmId    = i,
                              frmSub   = s,
                              frmTrans = t,
                              frmRec = case lookup hdrRec hs of
                                         Nothing -> ""
                                         Just r  -> r} 

  mkNackFrame :: [Header] -> Either String Frame
  mkNackFrame hs =
    case lookup hdrMId hs of
      Nothing -> Left $ "No message-id header in Ack Frame"
      Just i  -> let t = case lookup hdrTrn hs of
                           Nothing  -> ""
                           Just trn -> trn
                     s = case lookup hdrSub hs of -- mandatory!
                           Nothing -> ""
                           Just x  -> x
                 in Right NackFrame {
                              frmId    = i,
                              frmSub   = s,
                              frmTrans = t,
                              frmRec = case lookup hdrRec hs of
                                         Nothing -> ""
                                         Just r  -> r} 

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
                               frmSub  = case lookup hdrSub hs of
                                           Nothing -> ""
                                           Just s  -> s,
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

  mkRecFrame :: [Header] -> Either String Frame
  mkRecFrame hs =
    case lookup hdrRecId hs of
      Nothing -> Left $ "No receipt header in Receipt Frame"
      Just r  -> Right $ RecFrame r

  mkCondFrame :: [Header] -> Either String Frame
  mkCondFrame hs =
    let s   = case lookup hdrSes hs of
                Nothing -> "0"
                Just x  -> x
        v   = case lookup hdrVer hs of
                Nothing -> defVerStr
                Just x  -> x 
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

  mkErrFrame :: [Header] -> Int -> Body -> Either String Frame
  mkErrFrame hs l b =
    case lookup hdrMsg hs of
      Nothing -> Left "No message header in Error Frame"
      Just m  -> Right $ ErrFrame {
                           frmMsg  = m,
                           frmLen  = l,
                           frmMime = case lookup hdrMime hs of
                                       Nothing -> defMime
                                       Just t  -> 
                                         case parseMIMEType t of
                                           Nothing -> defMime
                                           Just x  -> x,
                           frmBody = b}

  sndToMsg :: String -> Frame -> Maybe Frame
  sndToMsg i f = case typeOf f of
                   Send ->
                     Just MsgFrame {
                               frmHdrs = frmHdrs f,
                               frmDest = frmDest f,
                               frmSub  = "",
                               frmLen  = frmLen  f,
                               frmMime = frmMime f,
                               frmId   = i,
                               frmBody = frmBody f
                             }
                   _ -> Nothing

  conToCond :: String -> String -> Heart -> Frame -> Maybe Frame
  conToCond s i b f = case typeOf f of
                        Connect ->
                          Just CondFrame {
                                 frmSes  = i,
                                 frmBeat = negoBeat (frmBeat f) b,
                                 frmVer  = negoVer $ frmAcVer f,
                                 frmSrv  = strToSrv s
                               }
                        _ -> Nothing

  negoVer :: [Version] -> Version
  negoVer vs = maxVer defVersion v
    where v = maxVers vs

  negoBeat :: Heart -> Heart -> Heart
  negoBeat hc hs = 
    let x = if sndC == 0 then 0 else max sndC sndS
        y = if rcvC == 0 then 0 else max rcvC rcvS
    in (x, y)
    where sndC = fst hc
          rcvC = snd hc
          sndS = fst hs
          rcvS = snd hs
    
  

