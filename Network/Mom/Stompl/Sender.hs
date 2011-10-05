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
  import           Data.Maybe (catMaybes)
  import qualified Data.ByteString           as B

  type TxId  = String
  type SubId = String

  ------------------------------------------------------------------------
  -- Transaction
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- Queue 
  ------------------------------------------------------------------------
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

  addMsgToQ :: String -> MsgStore -> Connection -> String -> SendState -> Sender ()
  addMsgToQ n m c sid st = do
    b <- get
    case getQueue n $ bookQs b of
      Nothing -> do
        logS INFO $ "Unknown Queue " ++ n ++
                    ". Discarding Message."
      Just q  -> do
        let cid = conId c
        case find (== m) $ qMsgs q of
          Nothing -> do
            let m' = m {strPending = [(cid, mkPnd sid st)]}
            let q' = q {qMsgs = m' : qMsgs q}
            put b {bookQs = insert q' $ delete q $ bookQs b}
          Just mx -> do
            let mx' = mx {strPending = (cid, mkPnd sid st) : strPending mx}
            let q' = q {qMsgs = insert mx' $ delete mx $ qMsgs q}
            put b {bookQs = insert q' $ delete q $ bookQs b}

  msgFilter :: Int -> SubId -> SendState -> [MsgStore] -> [MsgStore]
  msgFilter cid sid st ms = 
    case filCon cid ms of
      []  -> []
      cms -> case filSub sid cms of
              []  -> []
              sms -> filState st sms
    where findCon   c x = (              fst) x == c
          findSub   s x = (getPndSub   . snd) x == s
          findState s x = (getPndState . snd) x == s
          findMsg f x   = case find f $ strPending x of
                          Nothing -> Nothing
                          Just y  -> Just x
          filterM f     = catMaybes . foldr (\x y -> findMsg f x : y) [] 
          filCon  c xs  = filterM (findCon c) xs
          filSub  s xs  = filterM (findSub s) xs
          filState s xs = filterM (findState s) xs

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

  ------------------------------------------------------------------------
  -- Subscription 
  ------------------------------------------------------------------------
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

  getSubscription :: SubId -> [Subscription] -> Maybe Subscription
  getSubscription sid = find (eqSub sid)

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
    case getSubscription sid $ bookSubs b of
      Nothing -> Nothing
      Just s  -> case getQueue (subQueue s) $ bookQs b of
                   Nothing -> Nothing
                   Just q  -> Just q

  getSubMode :: SubId -> Sender AckMode
  getSubMode sid = do
    b <- get
    case find (eqSub sid) $ bookSubs b of
      Nothing -> return Auto
      Just s  -> return $ subMode s

  ------------------------------------------------------------------------
  -- MessageStore
  ------------------------------------------------------------------------
  type PndMsg = (SubId, SendState)

  getPndSub :: PndMsg -> SubId
  getPndSub = fst

  getPndState :: PndMsg -> SendState
  getPndState = snd

  mkPnd :: SubId -> SendState -> PndMsg
  mkPnd sid st = (sid, st)

  data MsgStore = MsgStore {
                    strId      :: String,
                    strMsg     :: B.ByteString,
                    strPending :: [(Int, PndMsg)]}
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

  setState :: Int -> String -> SendState -> MsgStore -> MsgStore
  setState cid sid s m = case lookup cid $ strPending m of
                           Nothing  -> m 
                           Just old -> 
                             m {strPending =
                                 (cid, mkPnd sid s) : (delete (cid, old) $ strPending m)}

  updMsgState :: Int -> String -> String -> SubId -> Sender ()
  updMsgState cid mid n sid = do
    b <- get
    case getQueue n $ bookQs b of
      Nothing -> logS WARNING $ "Queue " ++ n ++ " not found"
      Just q  -> 
        case getStoreFromQ mid q of
          Nothing -> logS WARNING $ "Queue " ++ n ++ " has no pending messages"
          Just m  -> do
            case lookup cid $ strPending m of
              Nothing -> logS WARNING $ "Queue "             ++ n ++ 
                                        " has no pending message" ++
                                        " for connection " ++ (show cid)
              Just (sub, Sent) -> 
                if sub == sid then delMsg  Sent cid q sid m else return ()
              Just (sub, st  ) -> 
                if sub == sid then setSent st   cid q sid m else return ()

  setSent :: SendState -> Int -> Queue -> SubId -> MsgStore -> Sender ()
  setSent st cid q sid m = do
    b <- get
    let m' = m {strPending = (cid, mkPnd sid Sent) : (delete (cid, mkPnd sid st) $ strPending m)}
    let q' = q {qMsgs = insert m' $ delete m $ qMsgs q}
    put b {bookQs = insert q' $ delete q $ bookQs b}

  delMsg :: SendState -> Int -> Queue -> SubId -> MsgStore -> Sender ()
  delMsg st cid q sid m = do
    b <- get
    let m' = m {strPending = delete (cid, mkPnd sid st) $ strPending m}
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

  ------------------------------------------------------------------------
  -- Connection
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- State
  ------------------------------------------------------------------------
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

  getCount :: Sender Int
  getCount = do
    b <- get
    let i = if bookCount b == 999999999
              then 1
              else 1 + bookCount b
    put b {bookCount = i}
    return i

  registerCon :: (SubMsg -> IO()) -> Int -> S.Socket -> IO ()
  registerCon wSnd cid s = 
    wSnd $ RegMsg cid s 

  unRegisterCon :: (SubMsg -> IO ()) -> Int -> IO ()
  unRegisterCon wSnd cid = 
    wSnd $ UnRegMsg cid 

  bookFrame :: (SubMsg -> IO ()) -> Int -> Frame -> IO ()
  bookFrame wSnd cid f = 
    wSnd $ FrameMsg cid f

  logS :: Priority -> String -> Sender ()
  logS p s = do
    b <- get
    liftIO $ logX (writeLog $ bookCfg b) (bookLog b) p s 

  logIO :: Book -> Priority -> String -> IO ()
  logIO b p s = do
    logX (writeLog $ bookCfg b) (bookLog b) p s 

  ------------------------------------------------------------------------
  -- Start and run Sender
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- Handling requests
  ------------------------------------------------------------------------
  waitRequest :: Sender SubMsg
  waitRequest = do
    b <- get
    let brk = bookCfg b
    liftIO $ readSender brk

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

  ------------------------------------------------------------------------
  -- Begin Frame
  ------------------------------------------------------------------------
  handleBegin :: Int -> Frame -> Sender ()
  handleBegin cid f = do
    let tid = getTrans f
    logS DEBUG $ "Handle Begin: " ++ tid
    insTx $ mkTx cid tid []

  ------------------------------------------------------------------------
  -- Commit Frame
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- Abort Frame
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- Ack Frame
  ------------------------------------------------------------------------
  handleAck :: Int -> Frame -> Sender ()
  handleAck cid f = do
    if null $ getTrans f 
      then doAck cid f
      else addTx cid f

  doAck :: Int -> Frame -> Sender ()
  doAck cid f = do
    logS DEBUG "Handle Ack"
    let mid = getId f
    b <- get
    case lookup mid $ bookMsgs b of
      Nothing -> logS INFO $ "Unknown message " ++ mid
      Just n  -> do
        let sid = mkSubId cid (getSub f) n
        am <- getSubMode sid
        case am of
          Client     -> updMsgOlder b   mid n sid
          ClientIndi -> updMsgState cid mid n sid
          _          -> liftIO $ putStrLn "AckMode Auto!" -- return ()
     where updMsgOlder b mid n sid = 
              case getQueue n $ bookQs b of
                Nothing -> return ()
                Just q  -> do
                  let ms  = dropWhile (\x -> strId x /= mid) $ qMsgs q
                  let ms' = msgFilter cid sid Sent ms
                  mapM_ (\x -> updMsgState cid (strId x) n sid) ms' 

  ------------------------------------------------------------------------
  -- Subscribe Frame
  ------------------------------------------------------------------------
  -- should we ensure unique subscriptions per
  -- cid / queue or
  -- cid / id / queue ?
  handleSub :: Int -> Frame -> Sender ()
  handleSub cid f = do
    logS DEBUG "Handle Subscription"
    b <- get
    let cons = bookCons b
    case getCon cid cons of
      Nothing -> return () -- error & disconnect
      Just _  -> do
        let q  = getDest f
        insSub cid (getId f) (getDest f) (getAcknow f)
        logS DEBUG $ show $ bookQs     b
        -- logS DEBUG $ show $ bookSubs   b
        -- logS DEBUG $ show $ bookCons   b

  ------------------------------------------------------------------------
  -- Unsubscribe Frame
  ------------------------------------------------------------------------
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
                          then getSubscription 
                               (mkSubId cid (getId f) (getDest f))
                          else getSubById cid i
        case subSearch $ bookSubs b of
          Nothing -> do
            logS WARNING $ "Subscription " ++ (mkSubId cid (getId f) (getDest f)) ++ " not found!"
            return ()
          Just s  -> do
            delSub s
            -- logS DEBUG $ show $ bookSubs b
            -- logS DEBUG $ show $ bookCons b

  ------------------------------------------------------------------------
  -- Send Frame
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- Error Frame
  ------------------------------------------------------------------------
  handleError :: Int -> Frame -> Sender ()
  handleError cid f = do
    b <- get
    case getCon cid $ bookCons b of
      Nothing -> return ()
      Just c  -> sendFrame "" (c, Nothing) "" f

  ------------------------------------------------------------------------
  -- Actually send a Frame
  ------------------------------------------------------------------------
  sendFrame :: String -> (Connection, Maybe Subscription) -> String -> Frame -> Sender ()
  sendFrame n (c, mbS) trn f = do
    let m  = putFrame f
    let sid = case mbS of
                Nothing -> ""
                Just x  -> subId x
    if (conState c == Down) ||
       (not $ null $ conPending c) 
      then addMsgx n m sid Pending
      else do
        let s = conSock c
        b <- get
        l <- liftIO $ catch (SB.send s m)
                            (onError b s)
        if l /= B.length m
          then do
            logS ERROR "Can't send Frame"
            updCon c {conState = Down}
            addMsgx n m sid Pending
          else case mbS of
                 Nothing -> return ()
                 Just x  -> 
                   if subMode x /= Auto
                     then addMsgx n m sid Sent
                     else return () 
    where onError b s e = do
            logIO b WARNING (show e)
            return 0
          addMsgx nm m sid st = do
            let ms = mkStore m f [(conId c, st)]
            if null nm
              then updCon c {conPending = ms : conPending c} 
              else addMsgToQ nm ms c sid st
