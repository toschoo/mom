{-# OPTIONS -fglasgow-exts -fno-cse #-}
module State (
         msgContent, numeric, ms,
         Connection(..),
         Transaction(..),
         Topt(..), hasTopt, tmo,
         TxState(..),
         Receipt,
         mkConnection,
         logSend, logReceive,
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
  import           Data.Time.Clock

  ------------------------------------------------------------------------
  -- | Returns the content of the message in the format 
  --   produced by an in-bound converter
  ------------------------------------------------------------------------
  msgContent :: P.Message a -> a
  msgContent = P.msgCont

  ------------------------------------------------------------------------
  -- some helpers
  ------------------------------------------------------------------------
  numeric :: String -> Bool
  numeric = all isDigit

  ------------------------------------------------------------------------
  -- convert milliseconds to microseconds 
  ------------------------------------------------------------------------
  ms :: Int -> Int
  ms u = 1000 * u

  ------------------------------------------------------------------------
  -- compare two tuples by the fst
  ------------------------------------------------------------------------
  eq :: Eq a => (a, b) -> (a, b) -> Bool
  eq x y = fst x == fst y

  -- | Just a nicer word for 'Rec'
  type Receipt = Rec

  ------------------------------------------------------------------------
  -- Connection
  ------------------------------------------------------------------------
  data Connection = Connection {
                      conCon     :: P.Connection,
                      conOwner   :: ThreadId,
                      conHisBeat :: UTCTime,
                      conMyBeat  :: UTCTime, 
                      conSubs    :: [SubEntry],
                      conDests   :: [DestEntry], 
                      conThrds   :: [ThreadEntry],
                      conErrs    :: [F.Frame],
                      conRecs    :: [Receipt],
                      conAcks    :: [P.MsgId]}

  mkConnection :: P.Connection -> ThreadId -> UTCTime -> Connection
  mkConnection c myself t = Connection c myself t t [] [] [] [] [] []

  addAckToCon :: P.MsgId -> Connection -> Connection
  addAckToCon mid c = c {conAcks = mid : conAcks c} 

  rmAckFromCon :: P.MsgId -> Connection -> Connection
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

  setHisTime :: UTCTime -> Connection -> Connection
  setHisTime t c = c {conHisBeat = t}

  setMyTime  :: UTCTime -> Connection -> Connection
  setMyTime t c = c {conMyBeat = t}

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
                       txAcks    :: [P.MsgId],
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
            --   to wait for /pending receipts/.
            --   If receipts are pending, when the transaction
            --   is ready to terminate,
            --   and no timeout or a timeout /<= 0/ is given, 
            --   and the option 'OWithReceipts' 
            --   was passed to 'withTransaction',
            --   the transaction will be aborted with 'TxException';
            --   otherwise it will wait until all pending
            --   ineractions with the broker have terminated
            --   or the timeout has expired - whatever comes first.
            --   If the timeout expires first, 'TxException' is raised. 
            OTimeout Int 
            -- | This option has two effects:
            --   1) Internal interactions of the transaction
            --      with the broker will request receipts;
            --   2) before ending the transaction,
            --      the library will check for receipts
            --      that have not yet been confirmed by the broker.
            --
            --   If receipts are pending, when the transaction
            --   is ready to terminate and 'OTimeout' with
            --   a value /> 0/ is given, the transaction will
            --   wait for pending receipts; otherwise
            --   the transaction will be aborted with 'TxException'.
            --   Note that it, usually, does not make sense to use
            --   this options without 'OTimeout',
            --   since the risk to lose a receipt is very high.
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

  addAckToTx :: P.MsgId -> Transaction -> Transaction
  addAckToTx mid t = t {txAcks = mid : txAcks t}

  rmAckFromTx :: P.MsgId -> Transaction -> Transaction
  rmAckFromTx mid t = t {txAcks = delete mid $ txAcks t}

  addRecToTx :: Receipt -> Transaction -> Transaction
  addRecToTx r t = t {txRecs = r : txRecs t}

  rmRecFromTx :: Receipt -> Transaction -> Transaction
  rmRecFromTx r t = t {txRecs = delete r $ txRecs t}

  checkReceiptTx :: Receipt -> Transaction -> Bool
  checkReceiptTx r t = case find (== r) $ txRecs t of
                         Nothing -> True
                         Just _  -> False

  txPendingAck :: Transaction -> Bool
  txPendingAck t = txAbrtAck t && not (null $ txAcks t)

  txReceipts :: Transaction -> Bool
  txReceipts t = txAbrtRc t && not (null $ txRecs t) 

  ------------------------------------------------------------------------
  -- State 
  ------------------------------------------------------------------------
  {-# NOINLINE con #-}
  con :: MVar [ConEntry]
  con = unsafePerformIO $ newMVar []
 
  ------------------------------------------------------------------------
  -- Add connection to state
  ------------------------------------------------------------------------
  addCon :: ConEntry -> IO ()
  addCon c = modifyMVar_ con $ \cs -> return (c:cs)

  ------------------------------------------------------------------------
  -- get connection from state
  ------------------------------------------------------------------------
  getCon :: Con -> IO Connection
  getCon cid = withCon cid $ \(_, c) -> return (c, c) 

  ------------------------------------------------------------------------
  -- remove connection from state
  ------------------------------------------------------------------------
  rmCon :: Con -> IO ()
  rmCon cid = modifyMVar_ con $ \cs -> 
    case lookup cid cs of
      Nothing -> return cs
      Just c  -> 
        return $ deleteBy eq (cid, c) cs

  ------------------------------------------------------------------------
  -- Apply an action that may change a connection to the state
  ------------------------------------------------------------------------
  withCon :: Con -> (ConEntry -> IO (Connection, a)) -> IO a
  withCon cid op = modifyMVar con (\cs -> 
     case lookup cid cs of
       Nothing   -> 
         throwIO $ ConnectException $
                 "No such Connection: " ++ show cid
       Just c    -> do
         (c', x) <- op (cid, c)
         let cs' = updCon (cid, c') cs
         return (cs', x))

  ------------------------------------------------------------------------
  -- Log heart-beats
  ------------------------------------------------------------------------
  logTime :: Con -> (UTCTime -> Connection -> Connection) -> IO ()
  logTime cid f = 
    getCurrentTime >>= \t -> withCon cid (\(_, c) -> return (f t c, ()))

  logSend :: Con -> IO ()
  logSend cid = logTime cid setMyTime

  logReceive :: Con -> IO ()
  logReceive cid = logTime cid setHisTime

  ------------------------------------------------------------------------
  -- add and remove sub and dest
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- add transaction to connection
  -- Note: transactions are kept per threadId
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- get transaction from connection
  -- Note: transactions are kept per threadId
  ------------------------------------------------------------------------
  getTx :: Tx -> Connection -> IO (Maybe Transaction)
  getTx tx c = do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return Nothing
      Just ts -> return $ lookup tx ts

  ------------------------------------------------------------------------
  -- apply an action that may change a transaction to the state
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- update transaction state
  ------------------------------------------------------------------------
  updTxState :: Tx -> Con -> TxState -> IO ()
  updTxState tx cid st = updTx tx cid (setTxState st)

  ------------------------------------------------------------------------
  -- get current transaction for thread
  ------------------------------------------------------------------------
  getCurTx :: Connection -> IO (Maybe TxEntry)
  getCurTx c = do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return Nothing
      Just ts -> if null ts then return Nothing
                   else return $ Just $ head ts

  ------------------------------------------------------------------------
  -- apply a change of the current transaction 
  -- or the connection (if there is no transaction) to the state
  ------------------------------------------------------------------------
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
                                         (tid, ts') : 
                                         deleteBy eq (tid, ts) (conThrds c)}
                      return (c', ())

  ------------------------------------------------------------------------
  -- add a pending ack either to the current transaction
  -- or - if there is no transaction - to the connection
  ------------------------------------------------------------------------
  addAck :: Con -> P.MsgId -> IO ()
  addAck cid mid = do
    let toTx  = addAckToTx  mid
    let toCon = addAckToCon mid
    withCon cid $ updCurTx toTx toCon

  ------------------------------------------------------------------------
  -- remove a pending ack either from the current transaction
  -- or - if there is no transaction - from the connection
  ------------------------------------------------------------------------
  rmAck :: Con -> P.MsgId -> IO ()
  rmAck cid mid = do
    let fromTx  = rmAckFromTx  mid
    let fromCon = rmAckFromCon mid
    withCon cid $ updCurTx fromTx fromCon

  ------------------------------------------------------------------------
  -- add a pending receipt either to the current transaction
  -- or - if there is no transaction - to the connection
  ------------------------------------------------------------------------
  addRec :: Con -> Receipt -> IO ()
  addRec cid r = do
    let toTx  = addRecToTx  r
    let toCon = addRecToCon r
    withCon cid $ updCurTx toTx toCon

  ------------------------------------------------------------------------
  -- remove a pending receipt either from the current transaction
  -- or - if there is no transaction - from the connection
  ------------------------------------------------------------------------
  rmRec :: Con -> Receipt -> IO ()
  rmRec cid r = do
    let fromTx  = rmRecFromTx  r
    let fromCon = rmRecFromCon r
    withCon cid $ updCurTx fromTx fromCon

  ------------------------------------------------------------------------
  -- search for a receipt either in connection or transactions.
  -- this is used by the listener that is not in the thread list
  ------------------------------------------------------------------------
  forceRmRec :: Con -> Receipt -> IO ()
  forceRmRec cid r = withCon cid doRmRec 
    where doRmRec (_, c) = 
            case find (== r) $ conRecs c of
              Just _  -> return (rmRecFromCon r c, ())
              Nothing -> 
                let thrds = map rmRecFromThrd $ conThrds c
                in  return (c {conThrds = thrds}, ()) 
          rmRecFromThrd   (thrd, ts) = (thrd, map rmRecFromTxEntry ts)
          rmRecFromTxEntry (tid, tx) = (tid,      rmRecFromTx    r tx)

  ------------------------------------------------------------------------
  -- check a condition either on the current transaction or
  -- - if there is not transaction - on the connection
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- check a receipt
  ------------------------------------------------------------------------
  checkReceipt :: Con -> Receipt -> IO Bool
  checkReceipt cid r = do
    let onTx  = checkReceiptTx r
    let onCon = checkReceiptCon r
    checkCurTx onTx onCon cid

  ------------------------------------------------------------------------
  -- remove a specific transaction
  ------------------------------------------------------------------------
  rmThisTx :: Tx -> Con -> IO ()
  rmThisTx tx cid = withCon cid $ \(_, c) -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return (c, ())
      Just ts -> 
        if null ts 
          then return (c {conThrds = deleteBy eq (tid, []) (conThrds c)}, ())
          else 
            case lookup tx ts of
              Nothing -> return (c, ())
              Just t  -> do
                let ts' = deleteBy eq (tx, t) ts
                if null ts' 
                  then return (c {conThrds = 
                                   deleteBy eq (tid, ts) (conThrds c)},  ())
                  else return (c {conThrds = (tid, ts') : 
                                   deleteBy eq (tid, ts) (conThrds c)}, ())

  ------------------------------------------------------------------------
  -- remove the current transaction
  ------------------------------------------------------------------------
  rmTx :: Con -> IO ()
  rmTx cid = withCon cid $ \(_, c) -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return (c, ())
      Just ts -> 
        if null ts 
          then return (c {conThrds = deleteBy eq (tid, []) (conThrds c)}, ())
          else do
            let ts' = tail ts
            if null ts' 
              then return (c {conThrds = 
                               deleteBy eq (tid, ts) (conThrds c)},  ())
              else return (c {conThrds = (tid, ts') : 
                               deleteBy eq (tid, ts) (conThrds c)}, ())
