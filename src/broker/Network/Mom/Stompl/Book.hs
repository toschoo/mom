{-# Language CPP #-}
module Book ( 
         -- * Book 
         Book,
         mkBook,
         bookCfg, bookLog, 
         countBook,
         -- * Connection
         Connection, 
         eqCon, mkCon, getCon,
         addPndToCon, pndOnCon, getSock,
         addCon, remCon, getConId,
         updSockState, sockUp,
         -- * Queue
         Queue, 
         eqQueue, mkQueue, getQueue,
         qMsgs, addQueue, remQueue,
         getMsgFromQ,
         addMsgToQ, msgFilter,
         addMsgToQNoSub,
         addSubToQ, remSubFromQ,
         -- * Subscription
         Subscription,
         SubId, 
         mkSubId, parseSubId,
         addSub, remSub, remSubs,
         eqSub, getSub, mkSub,
         getQueueFromSub,
         getSubscribers, getSubsOfCon,
         getSubId, getSubConId,
         getSubById, getModeOfSub, getQofSub,
         -- * Transaction
         Transaction,
         TxId, 
         eqTx, getTx, mkTx, insertTrn, addTx, remTx,
         mkTxId, parseTrn, addTxFrame, txFrames,
         -- * MsgStore
         MsgStore,
         SendState(..), PndMsg,
         eqStore, getStoreFromQ,
         getQofMsg,
         addBookMsg, remBookMsg,
         getMsgId, getMsgString,
         remMsg, setSent, mkStore,
         updMsgPnd, setState, subMsg
#ifdef TEST
         , bookCons, bookTrans, bookQs, bookSubs,
         conPending, qSubs, MsgStore(..),
         mkPnd
#endif
         )

where

  import Config
  import Types

  import qualified Network.Mom.Stompl.Frame as F

  import           Data.List  (insert, delete, find, findIndex)
  import           Data.Maybe (catMaybes)
  import qualified Data.ByteString           as B
  import qualified Network.Socket            as S

  type TxId  = String
  type SubId = String

  ------------------------------------------------------------------------
  -- Book
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

  countBook :: Book -> (Int, Book)
  countBook b = let i = if bookCount b == 999999999
                          then 1
                          else 1 + bookCount b
                in (i, b {bookCount = i})

  ------------------------------------------------------------------------
  -- Transaction
  ------------------------------------------------------------------------
  data Transaction = Tx {
                       txId     :: TxId,
                       txExt    :: String,
                       txCid    :: Int,
                       txFrames :: [F.Frame] -- better a Sequence
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

  getTx :: TxId -> Book -> Maybe Transaction
  getTx tid = find (eqTx tid) . bookTrans

  addTx :: Transaction -> Book -> Book
  addTx tx b = b {bookTrans = insertTrn tx $ bookTrans b}

  remTx :: Transaction -> Book -> Book
  remTx tx b = b {bookTrans = delete tx $ bookTrans b}

  mkTx :: Int -> String -> [F.Frame] -> Transaction
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

  addTxFrame :: Int -> F.Frame -> Book -> Maybe Book
  addTxFrame cid f b = 
    case getTx tid b of
      Nothing -> Nothing
      Just tx -> let tx' = tx {txFrames = f : txFrames tx}
                 in  Just b {bookTrans = 
                               insertTrn tx' $ delete tx $ bookTrans b}
   where tid = mkTxId cid $ F.getTrans f

  ------------------------------------------------------------------------
  -- Queue 
  ------------------------------------------------------------------------
  data Pattern = PtoP | NtoM
    deriving (Show) 

  patternFromName :: String -> Pattern
  patternFromName qn = 
    let pre = takeWhile (/= '/') qn
        qs  = ["Q", "QUEUE"]
    in case find (== F.upString pre) qs of
         Nothing -> NtoM
         Just _  -> PtoP

  data Queue = Queue {
                 qName :: String,
                 qType :: Pattern,
                 qMsgs :: [MsgStore], -- List of References?
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

  getQueue :: String -> Book -> Maybe Queue
  getQueue n = find (eqQueue n) . bookQs

  addQueue :: Queue -> Book -> Book
  addQueue q b = b {bookQs = insert q $ bookQs b}

  remQueue :: Queue -> Book -> Book
  remQueue q b = b {bookQs = delete q $ bookQs b}

  mkQueue :: String -> [SubId] -> Queue
  mkQueue n subs = Queue {
                     qName = n,
                     qType = patternFromName n,
                     qSubs = subs,
                     qMsgs = []}

  addMsgToQ :: MsgStore -> Connection -> String -> SendState -> Queue -> Queue
  addMsgToQ m c sid st q = 
    let cid = conId c
    in case find (== m) $ qMsgs q of
         Nothing -> 
           let m' = m {strPending = [(cid, mkPnd sid st)]}
           in  q {qMsgs = m' : qMsgs q}
         Just mx -> 
           let mx' = mx {strPending = (cid, mkPnd sid st) : strPending mx}
           in  q  {qMsgs = subMsg mx mx' $ qMsgs q}

  addMsgToQNoSub :: MsgStore -> Queue -> Queue
  addMsgToQNoSub m q = q {qMsgs = m : qMsgs q}

  addSubToMsg :: Int -> SubId -> Queue -> Queue
  addSubToMsg cid sid q =
    let ms  = qMsgs q
        ms' = map (addPnd cid sid) ms
    in q {qMsgs = ms'}
 
  addPnd :: Int -> SubId -> MsgStore -> MsgStore
  addPnd cid sid m = m {strPending = (cid, mkPnd sid Pending) : strPending m}

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
                            Just _  -> Just x
          filterM f     = catMaybes . foldr (\x y -> findMsg f x : y) [] 
          filCon  c xs  = filterM (findCon c) xs
          filSub  s xs  = filterM (findSub s) xs
          filState s xs = filterM (findState s) xs

  addSub :: Subscription -> Book -> Book
  addSub s b = 
    let sid  = subId s
        n    = subQueue s
        subs = insert s $ bookSubs b
        qs   = case getQueue n b of
                  Nothing -> insert (mkQueue n [sid]) $ bookQs b
                  Just _  -> addSubToQ s n $ bookQs b
    in b {bookSubs = subs, 
          bookQs   = qs}

  remSub :: Subscription -> Book -> Book
  remSub s b = 
    let subs = delete s $ bookSubs b
        qs   = remSubFromQ (getSubId s) (getQofSub s) $ bookQs b
    in b {bookSubs = subs,
          bookQs   = qs}

  remSubs :: SubId -> Book -> Book
  remSubs sid b = b {bookSubs = filSubs b}
    where filSubs = filter (not . eqSub sid) . bookSubs

  addSubToQ :: Subscription -> String -> [Queue] -> [Queue]
  addSubToQ s n qs = 
    case find (eqQueue n) qs of
      Nothing -> qs
      Just q  -> 
        let q' = addSubToMsg cid sid $ q {qSubs = insert sid $ qSubs q} 
        in  (insert q' . delete q) qs
    where sid = subId  s
          cid = subCid s

  remSubFromQ :: SubId -> String -> [Queue] -> [Queue]
  remSubFromQ sid n qs = 
    case find (eqQueue n) qs of
      Nothing -> qs
      Just q  -> case find (== sid) $ qSubs q of
                   Nothing -> qs
                   Just s  -> 
                     let q' = q {qSubs = delete s $ qSubs q}
                     in  insert q' $ delete q qs

  getQofMsg :: String -> Book -> Maybe String
  getQofMsg mid b = lookup mid $ bookMsgs b

  addBookMsg :: String -> String -> Book -> Book
  addBookMsg mid qn b = b {bookMsgs = (mid, qn) : bookMsgs b}

  remBookMsg :: String -> Book -> Book
  remBookMsg mid b = case lookup mid $ bookMsgs b of
                       Nothing -> b
                       Just qn -> let msgs = delete (mid, qn) $ bookMsgs b
                                  in  b {bookMsgs = msgs}

  ------------------------------------------------------------------------
  -- Subscription 
  ------------------------------------------------------------------------
  data Subscription = Subscription {
                        subId    :: SubId, -- = ConId ++ extId ++ Queue
                        subExt   :: String,
                        subCid   :: Int,
                        subQueue :: String,  -- Reference?
                        subMode  :: F.AckMode
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

  getSubId :: Subscription -> SubId
  getSubId = subId

  getSubConId :: Subscription -> Int
  getSubConId = subCid

  getSub :: SubId -> Book -> Maybe Subscription
  getSub sid = find (eqSub sid) . bookSubs

  getSubById :: Int -> String -> Book -> Maybe Subscription
  getSubById cid ext = find (\s -> cid == subCid s && ext == subExt s) . bookSubs

  mkSub :: Int -> String -> String -> F.AckMode -> Subscription
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

  getSubscribers :: String -> Book -> [Subscription]
  getSubscribers que = 
    takeWhile (\s -> eq s) . dropWhile (\s -> uneq s) . bookSubs
    where uneq   = not . eq 
          eq   s = subQueue s == que

  getSubsOfCon :: Int -> Book -> [Subscription]
  getSubsOfCon cid = filter (\s -> cid == subCid s) . bookSubs

  getQueueFromSub :: SubId -> Book -> Maybe Queue
  getQueueFromSub sid b =
    case getSub sid b of
      Nothing -> Nothing
      Just s  -> case getQueue (subQueue s) b of
                   Nothing -> Nothing
                   Just q  -> Just q

  getModeOfSub :: Subscription -> F.AckMode
  getModeOfSub = subMode

  getQofSub :: Subscription -> String
  getQofSub = subQueue

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
                    strPending :: [(Int, PndMsg)],
                    strOld     :: Bool}
    deriving (Show)

  instance Eq MsgStore where
    m1 == m2 = eqStore (strId m1) m2

  instance Ord MsgStore where
    compare m1 m2 
      | strId m1 == strId m2 = EQ
      | strId m1 >= strId m2 = GT
      | otherwise          = LT

  mkStore :: B.ByteString -> F.Frame -> [(cid, SendState)] -> MsgStore
  mkStore m f _  = MsgStore {
                     strId      = case F.typeOf f of
                                    F.Message -> F.getId f
                                    _         -> "",
                     strMsg     = m,
                     strPending = [],
                     strOld     = False}

  getMsgId :: MsgStore -> String
  getMsgId = strId

  getMsgString :: MsgStore -> B.ByteString
  getMsgString = strMsg

  getMsgFromQ :: String -> String -> Book -> Maybe MsgStore
  getMsgFromQ mid qn b = 
    case getQueue qn b of
      Nothing -> Nothing
      Just q  -> find (eqStore mid) $ qMsgs q

  updMsgPnd :: Int -> String -> SubId -> Queue -> Queue
  updMsgPnd cid mid sid q = 
    case getStoreFromQ mid q of
      Nothing -> q
      Just m  -> 
        case lookup cid $ strPending m of
          Nothing -> q
          Just (sub, Sent) -> 
            if sub == sid 
              then remMsg Sent cid sid m q
              else q
          Just (sub, st  ) -> 
            if sub == sid 
              then setSent st cid sid m q 
              else q

  setState :: Int -> String -> SendState -> MsgStore -> MsgStore
  setState cid sid s m = case lookup cid $ strPending m of
                           Nothing  -> m 
                           Just old -> 
                             m {strPending =
                                 (cid, mkPnd sid s) : (delete (cid, old) $ strPending m)}

  setSent :: SendState -> Int -> SubId -> MsgStore -> Queue -> Queue
  setSent st cid sid m q = 
    let m' = m {strPending = (cid, mkPnd sid Sent) : (delete (cid, mkPnd sid st) $ strPending m)}
    in  q {qMsgs = subMsg m m' $ qMsgs q}

  remMsg :: SendState -> Int -> SubId -> MsgStore -> Queue -> Queue
  remMsg st cid sid m q = 
    let m' = m {strPending = delete (cid, mkPnd sid st) $ strPending m}
        q' = q {qMsgs = delete m $ qMsgs q}
    in if null $ strPending m'
         then q' 
         else q' {qMsgs = subMsg m m' $ qMsgs q}

  subMsg :: MsgStore -> MsgStore -> [MsgStore] -> [MsgStore]
  subMsg m m' ms = 
    case findIndex (\x -> strId x == strId m) ms of
      Nothing -> ms
      Just i  -> take i ms ++ [m'] ++ drop (i+1) ms 

  getStoreFromQ :: String -> Queue -> Maybe MsgStore
  getStoreFromQ mid q = find (\m -> strId m == mid) $ qMsgs q 

  eqStore :: String -> MsgStore -> Bool
  eqStore mid1 m =  let mid2 = strId m
                    in (not . null) mid1 &&
                       (not . null) mid2 && mid1 == mid2

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

  getCon :: Int -> Book -> Maybe Connection
  getCon cid = find (eqCon cid) . bookCons

  mkCon :: Int -> S.Socket -> Connection
  mkCon cid sock = Connection cid sock Up []

  addCon :: Connection -> Book -> Book
  addCon c b = b {bookCons = c : bookCons b}

  remCon :: Connection -> Book -> Book
  remCon c b = b {bookCons = delete c $ bookCons b}

  addPndToCon :: MsgStore -> Connection -> Connection
  addPndToCon m c = c {conPending = m : conPending c}

  pndOnCon :: Connection -> Bool
  pndOnCon = null . conPending

  updSockState :: SockState -> Connection -> Connection
  updSockState st c = c{conState = st}

  sockUp :: Connection -> Bool
  sockUp c = conState c == Up

  getConId :: Connection -> Int
  getConId = conId

  getSock :: Connection -> S.Socket
  getSock = conSock
 
