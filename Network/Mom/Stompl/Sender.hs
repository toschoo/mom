module Sender {- (
                            startSender,
                            registerCon, unRegisterCon,
                            bookFrame
                          ) -}
where
  
  import           Types  
  import           Config
  import           Logger
  import           Network.Mom.Stompl.Frame  as F

  import qualified Network.Socket            as S
  import qualified Network.Socket.ByteString as SB

  import           Control.Concurrent
  import           Control.Monad.State
  import           Control.Applicative ((<$>))

  import           Data.List  (insert, delete, find)
  import qualified Data.ByteString           as B

  type TxId  = String
  type SubId = String

  data Transaction = Tx {
                       txId     :: TxId,
                       txExt    :: String,
                       txCid    :: Int,
                       txFrames :: [Frame] -- better a Sequence
                     }
    deriving (Show)

  eqTx :: TxId -> Transaction -> Bool
  eqTx tid trn = txId trn == tid

  instance Eq Transaction where
    x == y = txId x `eqTx` y

  instance Ord Transaction where
    compare t1 t2 
      | txId t1 == txId t2 = EQ
      | txId t1 >= txId t2 = GT
      | otherwise          = LT

  getTx :: TxId -> [Transaction] -> Maybe Transaction
  getTx tid = find (eqTx tid)

  mkTx :: Int -> TxId -> [Frame] -> Transaction
  mkTx cid tid fs = Tx {
                      txId     = mkTxId cid tid,
                      txExt    = tid,
                      txCid    = cid,
                      txFrames = fs
                    }

  mkTxId :: Int -> String -> TxId
  mkTxId  cid tid = (show cid) ++ ":" ++ tid

  parseTrn :: TxId -> (Int, String)
  parseTrn tx = (read cid, tid)
    where cid = takeWhile (/= ':') tx
          tid = tail $ dropWhile (/= ':') tx

  insertTrn :: Transaction -> [Transaction] -> [Transaction]
  insertTrn tx ts = case find (== tx) ts of
                      Nothing -> insert tx ts
                      Just _  ->           ts

  data Queue = Queue {
                 qName :: String,
                 qMsgs :: [MsgStore],
                 qSubs :: [SubId]
               }
    deriving (Show)

  instance Eq Queue where
    q1 == q2 = qName q1 == qName q2

  instance Ord Queue where
    compare q1 q2 
      | qName q1 == qName q2 = EQ
      | qName q1 >= qName q2 = GT
      | otherwise            = LT

  eqQueue :: String -> Queue -> Bool
  eqQueue n q = n == qName q

  getQueue :: String -> [Queue] -> Maybe Queue
  getQueue n = find (eqQueue n)

  mkQueue :: String -> [SubId] -> Queue
  mkQueue n subs = Queue {
                     qName = n,
                     qSubs = subs,
                     qMsgs = []}

  addMsgToQ :: String -> MsgStore -> Connection -> SendState -> Sender ()
  addMsgToQ n m c st = do
    b <- get
    case getQueue n $ bookQs b of
      Nothing -> do
        logS INFO $ "Unknown Queue " ++ n ++
                    ". Discarding Message."
      Just q  -> do
        let cid = conId c
        case find (== m) $ qMsgs q of
          Nothing -> do
            let m' = m {strPending = [(cid, st)]}
            let q' = q {qMsgs = m' : qMsgs q}
            put b {bookQs = insert q' $ delete q $ bookQs b}
          Just mx -> do
            let mx' = mx {strPending = (cid, st) : strPending mx}
            let q' = q {qMsgs = insert mx' $ delete mx $ qMsgs q}
            put b {bookQs = insert q' $ delete q $ bookQs b}

  addSubToQ :: SubId -> String -> [Queue] -> [Queue]
  addSubToQ sid n qs =
    case getQueue n qs of
      Nothing -> qs
      Just q  -> 
        let q' = q {qSubs = insert sid $ qSubs q} 
        in  (insert q' . delete q) qs

  remSubFromQ :: SubId -> String -> [Queue] -> [Queue]
  remSubFromQ sid n qs = 
    case getQueue n qs of
      Nothing -> qs
      Just q  -> case find (== sid) $ qSubs q of
                   Nothing -> qs
                   Just s  -> 
                     let q' = q {qSubs = delete s $ qSubs q}
                     in if null $ qSubs q' 
                        then delete q qs
                        else (insert q' . delete q) qs

  data Subscription = Subscription {
                        subId    :: SubId, -- = ConId ++ extId ++ Queue
                        subExt   :: String,
                        subCid   :: Int,
                        subQueue :: String,
                        subMode  :: AckMode
                      }
    deriving (Show)

  instance Eq Subscription where
    s1 == s2 = subId s1 == subId s2

  instance Ord Subscription where
    compare s1 s2 
      | subId s1 == subId s2 = EQ
      | subId s1 >= subId s2 = GT
      | otherwise            = LT

  eqSub :: String -> Subscription -> Bool
  eqSub sid s = subId s == sid

  getSub :: String -> [Subscription] -> Maybe Subscription
  getSub sid = find (eqSub sid)

  getSubById :: Int -> String -> [Subscription] -> Maybe Subscription
  getSubById cid ext = find (\s -> cid == subCid s && ext == subExt s)

  mkSub :: Int -> String -> String -> AckMode -> Subscription
  mkSub cid ext q m = Subscription {
                        subId    = mkSubId cid ext q,
                        subExt   = ext,
                        subCid   = cid,
                        subQueue = q,
                        subMode  = m}

  mkSubId :: Int -> String -> String -> String
  mkSubId cid ext q = q ++ ":" ++ (show cid) ++ ":" ++ ext 

  parseSubId :: String -> (String, Int, String)
  parseSubId s = 
    let que = getQ   s
        cid = getCid que s
        ext = getExt cid que s
    in (que, read cid, ext)
    where getQ       = takeWhile (/= ':') 
          getCid q   = drop (length q + 1) . takeWhile (/= ':') 
          getExt c q = drop   (length c + length q + 2) 

  getSubscribers :: String -> [Subscription] -> [Subscription]
  getSubscribers que = 
    takeWhile (\s -> eq s) . dropWhile (\s -> uneq s)
    where uneq   = not . eq 
          eq   s = subQueue s == que

  getSubsOfCon :: Int -> [Subscription] -> [Subscription]
  getSubsOfCon cid = filter (\s -> cid == subCid s) 

  getQueueFromSub :: SubId -> Book -> Maybe Queue
  getQueueFromSub sid b =
    case getSub sid $ bookSubs b of
      Nothing -> Nothing
      Just s  -> case getQueue (subQueue s) $ bookQs b of
                   Nothing -> Nothing
                   Just q  -> Just q

  data MsgStore = MsgStore {
                    strId      :: String,
                    strMsg     :: B.ByteString,
                    strPending :: [(Int, SendState)]}
    deriving (Show)

  instance Eq MsgStore where
    (==) = eqStore

  instance Ord MsgStore where
    compare m1 m2 
      | strId m1 == strId m2 = EQ
      | strId m1 >= strId m2 = GT
      | otherwise          = LT

  mkStore :: B.ByteString -> Frame -> [(cid, SendState)] -> MsgStore
  mkStore m f _  = MsgStore {
                     strId      = case typeOf f of
                                    F.Message -> getId f
                                    _         -> "",
                     strMsg     = m,
                     strPending = []}

  setState :: Int -> SendState -> MsgStore -> MsgStore
  setState cid s m = case lookup cid $ strPending m of
                       Nothing  -> m 
                       Just old -> 
                         m {strPending =
                             (cid, s) : (delete (cid, old) $ strPending m)}

  updMsgState :: Int -> String -> String -> Sender ()
  updMsgState cid mid n = do
    b <- get
    case getQueue n $ bookQs b of
      Nothing -> logS WARNING $ "Queue " ++ n ++ " not found"
      Just q  -> 
        case getStoreFromQ mid q of
          Nothing -> logS WARNING $ "Queue " ++ n ++ " has no pending messages"
          Just m  -> 
            case lookup cid $ strPending m of
              Nothing -> logS WARNING $ "Queue "             ++ n ++ 
                                        " has no pending message" ++
                                        " for connection " ++ (show cid)
              Just Sent -> delMsg  Sent cid q m
              Just st   -> setSent st   cid q m

  setSent :: SendState -> Int -> Queue -> MsgStore -> Sender ()
  setSent st cid q m = do
    b <- get
    let m' = m {strPending = (cid, Sent) : (delete (cid, st) $ strPending m)}
    let q' = q {qMsgs = insert m' $ delete m $ qMsgs q}
    put b {bookQs = insert q' $ delete q $ bookQs b}

  delMsg :: SendState -> Int -> Queue -> MsgStore -> Sender ()
  delMsg st cid q m = do
    b <- get
    let m' = m {strPending = delete (cid, st) $ strPending m}
    if null $ strPending m'
      then do 
        let q' = q {qMsgs = delete m $ qMsgs q}
        let b' = case lookup (strId m) $ bookMsgs b of
                   Nothing -> b
                   Just s  -> b {bookMsgs = delete (strId m, s) $ bookMsgs b}
        put b' {bookQs = insert q' $ delete q $ bookQs b}
      else do 
        let q' = q {qMsgs = insert m' $ delete m $ qMsgs q}
        put b {bookQs = insert q' $ delete q $ bookQs b}

  getStoreFromQ :: String -> Queue -> Maybe MsgStore
  getStoreFromQ mid q = find (\m -> strId m == mid) $ qMsgs q 

  eqStore :: MsgStore -> MsgStore -> Bool
  eqStore x y =  let xi = strId x
                     yi = strId y
                 in (not . null) xi &&
                    (not . null) yi && xi == yi

  data SendState = Pending | Sent | Acked 
    deriving (Eq, Show)

  data Connection = Connection {
                       conId      :: Int,
                       conSock    :: S.Socket,
                       conState   :: SockState,
                       conPending :: [MsgStore]
                     }

  instance Show Connection where
    show c = "Connection {" ++ 
             "  " ++ (show $ conId c)   ++  ", " ++
             "  " ++ (show $ conState c) ++ ", " ++
             "  " ++ (show $ conPending c) ++ "}"

  instance Eq Connection where
    c1 == c2 = conId c1 == conId c2

  instance Ord Connection where
    c1 >= c2 = conId c1 >= conId c2

  eqCon :: Int -> Connection -> Bool
  eqCon cid c = conId c == cid

  getCon :: Int -> [Connection] -> Maybe Connection
  getCon cid = find (eqCon cid)

  mkCon :: Int -> S.Socket -> Connection
  mkCon cid sock = Connection cid sock Up []

  data Book = Book {
                bookCfg   :: Config,
                bookLog   :: String,
                bookCount :: Int,
                bookCons  :: [Connection],
                bookQs    :: [Queue],
                bookSubs  :: [Subscription],
                bookMsgs  :: [(String, String)],
                bookTrans :: [Transaction]
              }

  mkBook :: Config -> Book
  mkBook c = Book {
               bookCfg   = c,
               bookLog   = (getName c) ++ "." ++ "Sender",
               bookCount = 1,
               bookCons  = [],
               bookQs    = [],
               bookSubs  = [],
               bookMsgs  = [],
               bookTrans = []}

  type Sender = StateT Book IO 

  changeBook :: String -> Sender () 
  changeBook name = do
    logS DEBUG $ "Changing Broker name to '" ++ name ++ "'"
    b <- get
    let b' = b {bookLog = name ++ "." ++ "Sender",
                bookCfg = setName name $ bookCfg b}
    put b'

  addMsg :: String -> String -> Sender ()
  addMsg mid n = do
    b <- get
    put b {bookMsgs = (mid, n) : bookMsgs b}

  insQueue :: String -> [SubId] -> Sender ()
  insQueue n subs = do
    b <- get
    put b {bookQs = insert (mkQueue n subs) $ bookQs b}

  insSub :: Int -> String -> String -> AckMode -> Sender ()
  insSub cid ext queue m = do
    b <- get
    let sid  = mkSubId cid ext queue
    let subs = insert (mkSub cid ext queue m) $ bookSubs b
    let qs   = case getQueue queue $ bookQs b of
                  Nothing -> insert (mkQueue queue [sid]) $ bookQs b
                  Just _  -> addSubToQ sid queue $ bookQs b
    put b {bookSubs = subs,
           bookQs   = qs}

  insTx :: Transaction -> Sender ()
  insTx tx = do
    b <- get 
    put b {bookTrans = insertTrn tx $ bookTrans b}

  delTx :: Transaction -> Sender ()
  delTx tx = do
    b <- get
    put b {bookTrans = delete tx $ bookTrans b}

  addTx :: Int -> Frame -> Sender ()
  addTx cid f = do
    let tid = mkTxId cid $ getTrans f
    b <- get
    let ts = bookTrans b
    case getTx tid ts of
      Nothing -> do
        logS INFO $ "Unknown Tx in Frame: " ++ tid
        -- send error frame
      Just tx -> do
        let tx' = tx {txFrames = resetTrans f : txFrames tx}
        put b {bookTrans = insertTrn tx' $ delete tx $ bookTrans b}

  delSubsOfCon :: Int -> Sender ()
  delSubsOfCon cid = do
    b <- get
    let ss = getSubsOfCon cid $ bookSubs b
    mapM_ delSub ss

  delSub :: Subscription -> Sender ()
  delSub s = do
    b <- get
    let subs = delete s $ bookSubs b
    let qs   = remSubFromQ (subId s) (subQueue s) $ bookQs b
    put b {bookSubs = subs,
           bookQs   = qs}

  delSubs :: String -> Sender ()
  delSubs sid = do
    b <- get
    put b {bookSubs = filter (not . eqSub sid) $ bookSubs b}

  updQueue :: Queue -> Sender ()
  updQueue q = do
    b <- get
    put b {bookQs = insert q $ delete q $ bookQs b}

  updSub :: Subscription -> Sender ()
  updSub s = do
    b <- get
    put b {bookSubs = insert s $ delete s $ bookSubs b}

  insCon :: Int -> S.Socket -> Sender ()
  insCon cid s = do
    b <- get
    put b {bookCons = (mkCon cid s) : bookCons b}
            
  updCon :: Connection -> Sender ()
  updCon c = do
    b <- get
    put b {bookCons = insert c $ delete c $ bookCons b}

  delCon :: Connection -> Sender ()
  delCon c = do
    b <- get
    put b {bookCons = delete c $ bookCons b}

  removeCon :: Int -> Sender ()
  removeCon cid = do
    b <- get
    case getCon cid $ bookCons b of
      Nothing -> return ()
      Just c  -> do
        delCon c
        delSubsOfCon cid

  getChannel :: Sender (Chan SubMsg)
  getChannel = do
    b <- get
    return $ getSndChan $ bookCfg b

  getCount :: Sender Int
  getCount = do
    b <- get
    let i = if bookCount b == 999999999
              then 1
              else 1 + bookCount b
    put b {bookCount = i}
    return i

  registerCon :: Chan SubMsg -> Int -> S.Socket -> IO ()
  registerCon ch cid s = 
    writeChan ch $ RegMsg cid s

  unRegisterCon :: Chan SubMsg -> Int -> IO ()
  unRegisterCon ch cid = 
    writeChan ch $ UnRegMsg cid 

  bookFrame :: Chan SubMsg -> Int -> Frame -> IO ()
  bookFrame ch cid f = 
    writeChan ch $ FrameMsg cid f

  logS :: Priority -> String -> Sender ()
  logS p s = do
    b <- get
    liftIO $ logX (getLogChan $ bookCfg b) (bookLog b) p s 

  logIO :: Book -> Priority -> String -> IO ()
  logIO b p s = do
    logX (getLogChan $ bookCfg b) (bookLog b) p s 

  startSender :: Config -> IO ()
  startSender c = do
    evalStateT runSender $ mkBook c

  runSender :: Sender ()
  runSender = do
    logS INFO "Sender up and running..."
    forever $ do
      m <- waitRequest
      logS DEBUG "request..."
      handleRequest m
      return ()

  waitRequest :: Sender SubMsg
  waitRequest = do
    ch <- getChannel
    liftIO $ readChan ch

  handleRequest :: SubMsg -> Sender ()
  handleRequest msg = do
    case msg of
      FrameMsg cid f    -> handleFrame cid f
      SockMsg  cid s st -> return ()
      RegMsg   cid s    -> insCon cid s
      UnRegMsg cid      -> removeCon cid 
      CfgSndMsg name    -> changeBook name

  handleFrame :: Int -> Frame -> Sender ()
  handleFrame cid f = 
    case typeOf f of
      F.Connect     -> return () -- ignore
      F.Connected   -> return () -- ignore
      F.Message     -> return () -- forward
      F.Receipt     -> return () -- forward
      F.Disconnect  -> return () -- send receipt
      F.Subscribe   -> handleSub    cid f
      F.Unsubscribe -> handleUnSub  cid f
      F.Begin       -> handleBegin  cid f
      F.Commit      -> handleCommit cid f
      F.Abort       -> handleAbort  cid f
      F.Ack         -> handleAck    cid f
      F.Send        -> handleSend   cid f
      F.Error       -> handleError  cid f

  handleBegin :: Int -> Frame -> Sender ()
  handleBegin cid f = do
    let tid = getTrans f
    logS DEBUG $ "Handle Begin: " ++ tid
    insTx $ mkTx cid tid []

  handleCommit :: Int -> Frame -> Sender ()
  handleCommit cid f = do
    let tid = mkTxId cid $ getTrans f
    logS DEBUG $ "Handle Commit for Tx " ++ tid
    b <- get
    case getTx tid $ bookTrans b of
      Nothing -> do
        logS INFO $ "Unknown Tx on commit: " ++ tid
        -- send error frame
      Just tx -> do
        logS DEBUG $ "Commiting " ++ tid
        mapM_ (handleFrame cid) $ reverse $ txFrames tx
        delTx tx

  handleAbort :: Int -> Frame -> Sender ()
  handleAbort cid f = do
    let tid = mkTxId cid $ getTrans f
    logS DEBUG $ "Handle Abort for Tx " ++ tid
    b <- get
    case getTx tid $ bookTrans b of
      Nothing -> do
        logS INFO $ "Unknown Tx on abort: " ++ tid
        -- send error frame
      Just tx -> do
        logS DEBUG $ "Aborting " ++ tid
        delTx tx

  handleAck :: Int -> Frame -> Sender ()
  handleAck cid f = do
    if null $ getTrans f 
      then doAck cid f
      else addTx cid f

  doAck :: Int -> Frame -> Sender ()
  doAck cid f = do
    logS DEBUG "Handle Ack"
    let mid = getId   f
    b <- get
    case (if null $ getDest f 
            then lookup mid $ bookMsgs b
            else Just $ getDest f) of
      Nothing -> logS INFO $ "Unknown message " ++ mid
      Just n  -> updMsgState cid mid n

  -- should we ensure unique subscriptions per
  -- cid / queue or
  -- cid / id / queue ?
  handleSub :: Int -> Frame -> Sender ()
  handleSub cid f = do
    logS DEBUG "Handle Subscription"
    b <- get
    let cons = bookCons b
    case getCon cid cons of
      Nothing -> return ()
      Just _  -> do
        let q  = getDest f
        insSub cid (getId f) (getDest f) (getAcknow f)
        logS DEBUG $ show $ bookQs     b
        -- logS DEBUG $ show $ bookSubs   b
        -- logS DEBUG $ show $ bookCons   b

  handleUnSub :: Int -> Frame -> Sender ()
  handleUnSub cid f = do
    logS DEBUG "Handle Unsubscribe"
    b <- get
    let cons = bookCons b
    case getCon cid cons of
      Nothing -> return ()
      Just _  -> do
        let i = getId   f
        let subSearch = if null i 
                          then getSub (mkSubId cid (getId f) (getDest f))
                          else getSubById cid i
        case subSearch $ bookSubs b of
          Nothing -> do
            logS WARNING $ "Subscription " ++ (mkSubId cid (getId f) (getDest f)) ++ " not found!"
            return ()
          Just s  -> do
            delSub s
            -- logS DEBUG $ show $ bookSubs b
            -- logS DEBUG $ show $ bookCons b

  -- send receipt
  handleSend :: Int -> Frame -> Sender ()
  handleSend cid f = do
    logS DEBUG ("Sending " ++ (show $ typeOf f) ++ " Frame")
    if null $ getTrans f 
      then doSend cid f
      else addTx  cid f

  doSend :: Int -> Frame -> Sender ()
  doSend cid f = do
    b <- get
    let cons = bookCons b
    case getCon cid cons of
      Nothing -> return ()
      Just _  -> do
        let n    = getDest f
        let subs = getSubscribers n $ bookSubs b
        let cc   = dropNothing $ map (\s -> (getCon (subCid s) cons, Just s)) subs
        mid <- show <$> getCount
        case sndToMsg mid f of
          Nothing -> logS ERROR "Can't convert Send to Message"
          Just m  -> do
            addMsg mid n 
            mapM_ (\c-> sendFrame n c (getTrans f) m) cc

  handleError :: Int -> Frame -> Sender ()
  handleError cid f = do
    b <- get
    case getCon cid $ bookCons b of
      Nothing -> return ()
      Just c  -> sendFrame "" (c, Nothing) "" f

  sendFrame :: String -> (Connection, Maybe Subscription) -> String -> Frame -> Sender ()
  sendFrame n (c, mbS) trn f = do
    let m  = putFrame f
    if (conState c == Down) ||
       (not $ null $ conPending c) 
      then addMsgx n m Pending
      else do
        let s = conSock c
        b <- get
        l <- liftIO $ catch (SB.send s m)
                            (onError b s)
        if l /= B.length m
          then do
            logS ERROR "Can't send Frame"
            updCon c {conState = Down}
            addMsgx n m Pending
          else case mbS of
                 Nothing -> return ()
                 Just x  -> 
                   if subMode x == Client
                     then addMsgx n m Sent
                     else return () 
    where onError b s e = do
            logIO b WARNING (show e)
            return 0
          addMsgx nm m st = do
            let ms = mkStore m f [(conId c, st)]
            if null nm
              then updCon c {conPending = ms : conPending c} 
              else addMsgToQ nm ms c st
