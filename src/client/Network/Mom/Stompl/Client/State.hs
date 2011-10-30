{-# OPTIONS -fglasgow-exts -fno-cse #-}
module State (
         msgContent, numeric, ms,
         Connection(..),
         Transaction(..),
         Topt(..), hasTopt, tmo,
         TxState(..),
         Receipt,
         mkConnection,
         addCon, updCon, rmCon, getCon,
         addSub, addDest, getSub, getDest,
         rmSub, rmDest,
         mkTrn,
         addTx,  getTx, rmTx, rmThisTx,
         updTx, updCurTx, getCurTx,
         updTxState,
         txPendingAck, txReceipts,
         addAck, rmAck, addRec, rmRec,
         forceRmRec,
         checkReceipt)
where

  import qualified Protocol as P
  import           Factory  

  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Client.Exception

  import           Control.Concurrent 
  import           Control.Exception (throwIO)

  import           System.IO.Unsafe

  import           Data.List (find, deleteBy, delete)
  import           Data.Char (isDigit)

  ------------------------------------------------------------------------
  -- | Returns the content of the message in the format 
  --   produced by an in-bound converter
  ------------------------------------------------------------------------
  msgContent :: P.Message a -> a
  msgContent = P.msgCont

  numeric :: String -> Bool
  numeric = and . map isDigit

  ms :: Int -> Int
  ms u = 1000 * u

  eq :: Eq a => (a, b) -> (a, b) -> Bool
  eq x y = fst x == fst y

  -- | Just a nicer word for 'Rec'
  type Receipt = Rec

  ------------------------------------------------------------------------
  -- Connection
  ------------------------------------------------------------------------
  data Connection = Connection {
                      conCon   :: P.Connection,
                      conOwner :: ThreadId,
                      conSubs  :: [SubEntry],
                      conDests :: [DestEntry], 
                      conThrds :: [ThreadEntry],
                      conErrs  :: [F.Frame],
                      conRecs  :: [Receipt],
                      conAcks  :: [String]}

  mkConnection :: P.Connection -> ThreadId -> Connection
  mkConnection c myself = Connection c myself [] [] [] [] [] []

  addAckToCon :: String -> Connection -> Connection
  addAckToCon mid c = c {conAcks = mid : conAcks c} 

  rmAckFromCon :: String -> Connection -> Connection
  rmAckFromCon mid c = c {conAcks = delete mid $ conAcks c}

  addRecToCon :: Receipt -> Connection -> Connection
  addRecToCon r c = c {conRecs = r : conRecs c}

  rmRecFromCon :: Receipt -> Connection -> Connection
  rmRecFromCon r c = c {conRecs = delete r $ conRecs c}

  checkReceiptCon :: Receipt -> Connection -> Bool
  checkReceiptCon r c = case find (== r) $ conRecs c of
                         Nothing -> True
                         Just _  -> False

  ------------------------------------------------------------------------
  -- Sub, Dest, Thread and Tx Entry
  ------------------------------------------------------------------------
  type SubEntry    = (Sub, Chan F.Frame)
  type DestEntry   = (String, Chan F.Frame)
  type ThreadEntry = (ThreadId, [TxEntry])
  type TxEntry     = (Tx, Transaction)

  addSubToCon :: SubEntry -> Connection -> Connection
  addSubToCon s c = c {conSubs = s : conSubs c}

  getSub :: Sub -> Connection -> Maybe (Chan F.Frame)
  getSub sid c = lookup sid (conSubs c)

  rmSubFromCon :: SubEntry -> Connection -> Connection
  rmSubFromCon s c = c {conSubs = ss} 
    where ss = deleteBy eq s (conSubs c)

  addDestToCon :: DestEntry -> Connection -> Connection
  addDestToCon d c = c {conDests = d : conDests c}

  getDest :: String -> Connection -> Maybe (Chan F.Frame)
  getDest dst c = lookup dst (conDests c)

  rmDestFromCon :: DestEntry -> Connection -> Connection
  rmDestFromCon d c = c {conDests = ds}
    where ds = deleteBy eq d (conDests c)

  txPendingAck :: Transaction -> Bool
  txPendingAck t = if (txAbrtAck t)
                     then if (null $ txAcks t) then False else True
                     else False

  txReceipts :: Transaction -> Bool
  txReceipts t = if (txAbrtRc t) 
                   then if (null $ txRecs t) then False else True
                   else False

  ------------------------------------------------------------------------
  -- Connection Entry
  ------------------------------------------------------------------------
  type ConEntry = (Con, Connection)

  updCon :: ConEntry -> [ConEntry] -> [ConEntry]
  updCon c cs = c : deleteBy eq c cs
  
  ------------------------------------------------------------------------
  -- Transaction 
  ------------------------------------------------------------------------
  data Transaction = Trn {
                       txState   :: TxState,
                       txTmo     :: Int,
                       txAbrtAck :: Bool,
                       txAbrtRc  :: Bool,
                       txAcks    :: [String],
                       txRecs    :: [Receipt]
                     }

  mkTrn :: [Topt] -> Transaction
  mkTrn os = Trn {
               txState   = TxStarted,
               txTmo     = tmo os,
               txAbrtAck = hasTopt OAbortMissingAcks os,
               txAbrtRc  = hasTopt OWithReceipts     os,
               txAcks    = [],
               txRecs    = []
             }

  ------------------------------------------------------------------------
  -- | Options passed to a transaction.
  ------------------------------------------------------------------------
  data Topt = 
            -- | The timeout in milliseconds (not microseconds!)
            --   to wait for pending receipts.
            --   If no timeout or a timeout /<= 0/, but 
            --   'OWithReceipts' is given,
            --   the transaction will immediately terminate
            --   with 'TxException';
            --   otherwise it will wait until all pending
            --   ineractions with the broker have terminated
            --   or the timeout has expired - whatever comes first.
            --   If the timeout expires first, 'TxException' is raised. 
            OTimeout Int 
            -- | If this option is given,
            --   interactions of the transaction with the broker,
            --   /i.e./ starting and ending the transaction,
            --   will request a receipt from the broker.
            --   If receipts are pending, when the transaction
            --   is ready to terminate and 'OTimeout' with
            --   a value /> 0/ is given, the transaction will
            --   wait for pending receipts.
            | OWithReceipts 
            -- | If a message was received from a 
            --   queue with 'OMode' option other 
            --   than 'F.Auto' and this message has not been
            --   acknowledged when the transaction is ready
            --   to terminate, the /ack/ is /missing/.
            --   With this option, the transaction 
            --   will not commit with missing /acks/,
            --   but abort and raise 'TxException'.
            | OAbortMissingAcks 
    deriving (Eq, Show)

  hasTopt :: Topt -> [Topt] -> Bool
  hasTopt o os = case find (== o) os of
                   Nothing -> False
                   Just _  -> True

  tmo :: [Topt] -> Int
  tmo os = case find isTimeout os of
             Just (OTimeout i) -> i
             _                 -> 0
    where isTimeout o = case o of
                          OTimeout _ -> True
                          _          -> False

  data TxState = TxStarted | TxEnded
    deriving (Eq, Show)

  setTxState :: TxState -> Transaction -> Transaction
  setTxState st t = t {txState = st}

  addAckToTx :: String -> Transaction -> Transaction
  addAckToTx mid t = t {txAcks = mid : txAcks t}

  rmAckFromTx :: String -> Transaction -> Transaction
  rmAckFromTx mid t = t {txAcks = delete mid $ txAcks t}

  addRecToTx :: Receipt -> Transaction -> Transaction
  addRecToTx r t = t {txRecs = r : txRecs t}

  rmRecFromTx :: Receipt -> Transaction -> Transaction
  rmRecFromTx r t = t {txRecs = delete r $ txRecs t}

  checkReceiptTx :: Receipt -> Transaction -> Bool
  checkReceiptTx r t = case find (== r) $ txRecs t of
                         Nothing -> True
                         Just _  -> False

  ------------------------------------------------------------------------
  -- State 
  ------------------------------------------------------------------------
  {-# NOINLINE con #-}
  con :: MVar [ConEntry]
  con = unsafePerformIO $ newMVar []
 
  addCon :: ConEntry -> IO ()
  addCon c = modifyMVar_ con $ \cs -> return (c:cs)

  getCon :: Con -> IO Connection
  getCon cid = withCon cid $ \(_, c) -> return (c, c) 

  rmCon :: Con -> IO ()
  rmCon cid = modifyMVar_ con $ \cs -> do
    case lookup cid cs of
      Nothing -> return cs
      Just c  -> 
        return $ deleteBy eq (cid, c) cs

  withCon :: Con -> (ConEntry -> IO (Connection, a)) -> IO a
  withCon cid op = modifyMVar con (\cs -> 
     case lookup cid cs of
       Nothing   -> 
         throwIO $ ConnectException $
                 "No such Connection: " ++ (show cid)
       Just c    -> do
         (c', x) <- op (cid, c)
         let cs' = updCon (cid, c') cs
         return (cs', x))

  addSub :: Con -> SubEntry -> IO ()
  addSub cid s  = withCon cid $ \(_, c) -> return (addSubToCon s c, ())

  addDest :: Con -> DestEntry -> IO ()
  addDest cid d = withCon cid $ \(_, c) -> return (addDestToCon d c, ())

  rmSub :: Con -> Sub -> IO ()
  rmSub cid sid = withCon cid rm
    where rm (_, c) = case getSub sid c of 
                        Nothing -> return (c, ())
                        Just ch -> return (rmSubFromCon (sid, ch) c, ())

  rmDest :: Con -> String -> IO ()
  rmDest cid dst = withCon cid rm
    where rm (_, c) = case getDest dst c of 
                        Nothing -> return (c, ())
                        Just ch -> return (rmDestFromCon (dst, ch) c, ())

  addTx :: TxEntry -> Con -> IO ()
  addTx t cid = withCon cid $ \(_, c) -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> 
        return (c {conThrds = [(tid, [t])]}, ())
      Just ts -> 
        return (c {conThrds = addTx2Thrds t tid (conThrds c) ts}, ())
    where addTx2Thrds tx tid ts trns = 
            (tid, tx : trns) : deleteBy eq (tid, trns) ts

  getTx :: Tx -> Connection -> IO (Maybe Transaction)
  getTx tx c = do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return Nothing
      Just ts -> return $ lookup tx ts

  updTx :: Tx -> Con -> (Transaction -> Transaction) -> IO ()
  updTx tx cid f = withCon cid $ \(_, c) -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return (c, ())
      Just ts -> 
        case lookup tx ts of
          Nothing -> return (c, ())
          Just t  -> 
            let t' = f t
            in  return (c {conThrds = 
                             updTxInThrds (tx, t') tid (conThrds c) ts}, 
                        ())
    where updTxInThrds t tid ts trns =
            (tid, t : deleteBy eq t trns) : deleteBy eq (tid, trns) ts

  updTxState :: Tx -> Con -> TxState -> IO ()
  updTxState tx cid st = updTx tx cid (setTxState st)

  getCurTx :: Connection -> IO (Maybe TxEntry)
  getCurTx c = do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return Nothing
      Just ts -> if null ts then return Nothing
                   else return $ Just $ head ts

  updCurTx :: (Transaction -> Transaction) ->
              (Connection  -> Connection)  ->
              ConEntry -> IO (Connection, ())
  updCurTx onTx onCon (_, c) = do
    tid  <- myThreadId
    case lookup tid $ conThrds c of
      Nothing -> return (onCon c, ())
      Just ts -> if null ts 
                   then return (onCon c, ())
                   else do
                      let (tx, t) = head ts
                      let t'      = onTx t 
                      let ts'     = (tx, t') : tail ts
                      let c'      = c {conThrds = 
                                         (tid, ts') : (
                                         deleteBy eq (tid, ts) $ conThrds c)}
                      return (c', ())

  addAck :: Con -> String -> IO ()
  addAck cid mid = do
    let toTx  = addAckToTx  mid
    let toCon = addAckToCon mid
    withCon cid $ updCurTx toTx toCon

  rmAck :: Con -> String -> IO ()
  rmAck cid mid = do
    let fromTx  = rmAckFromTx  mid
    let fromCon = rmAckFromCon mid
    withCon cid $ updCurTx fromTx fromCon

  addRec :: Con -> Receipt -> IO ()
  addRec cid r = do
    let toTx  = addRecToTx  r
    let toCon = addRecToCon r
    withCon cid $ updCurTx toTx toCon

  rmRec :: Con -> Receipt -> IO ()
  rmRec cid r = do
    let fromTx  = rmRecFromTx  r
    let fromCon = rmRecFromCon r
    withCon cid $ updCurTx fromTx fromCon

  forceRmRec :: Con -> Receipt -> IO ()
  forceRmRec cid r = withCon cid doRmRec 
    where doRmRec (_, c) = do
            case find (== r) $ conRecs c of
              Just _  -> return (rmRecFromCon r c, ())
              Nothing -> 
                let thrds = map rmRecFromThrd $ conThrds c
                in  return (c {conThrds = thrds}, ()) 
          rmRecFromThrd   (thrd, ts) = (thrd, map rmRecFromTxEntry ts)
          rmRecFromTxEntry (tid, tx) = (tid,      rmRecFromTx    r tx)

  checkCurTx :: (Transaction -> Bool) ->
                (Connection  -> Bool) -> 
                Con -> IO Bool
  checkCurTx onTx onCon cid = do
    c   <- getCon cid
    tid <- myThreadId
    case lookup tid $ conThrds c of
      Nothing -> return $ onCon c
      Just ts -> if null ts then return $ onCon c
                   else return $ onTx $ (snd . head) ts

  checkReceipt :: Con -> Receipt -> IO Bool
  checkReceipt cid r = do
    let onTx  = checkReceiptTx r
    let onCon = checkReceiptCon r
    checkCurTx onTx onCon cid

  rmThisTx :: Tx -> Con -> IO ()
  rmThisTx tx cid = withCon cid $ \(_, c) -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return (c, ())
      Just ts -> 
        if null ts 
          then return (c {conThrds = deleteBy eq (tid, []) $ conThrds c}, ())
          else 
            case lookup tx ts of
              Nothing -> return (c, ())
              Just t  -> do
                let ts' = deleteBy eq (tx, t) ts
                if null ts' 
                  then return (c {conThrds = 
                                   deleteBy eq (tid, ts) $ conThrds c},  ())
                  else return (c {conThrds = (tid, ts') : (
                                   deleteBy eq (tid, ts) $ conThrds c)}, ())

  rmTx :: Con -> IO ()
  rmTx cid = withCon cid $ \(_, c) -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return (c, ())
      Just ts -> 
        if null ts 
          then return (c {conThrds = deleteBy eq (tid, []) $ conThrds c}, ())
          else do
            let ts' = tail ts
            if null ts' 
              then return (c {conThrds = 
                               deleteBy eq (tid, ts) $ conThrds c},  ())
              else return (c {conThrds = (tid, ts') : (
                               deleteBy eq (tid, ts) $ conThrds c)}, ())

