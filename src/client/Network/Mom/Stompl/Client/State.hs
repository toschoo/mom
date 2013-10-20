{-# Language BangPatterns #-}
module State (
         msgContent, numeric, ms,
         Connection(..),
         Copt(..),
         oHeartBeat, oMaxRecv,
         oAuth, oCliId,
         Transaction(..),
         Topt(..), hasTopt, tmo,
         TxState(..),
         Receipt,
         mkConnection,
         logSend, logReceive,
         addCon, rmCon, getCon,
         addSub, addDest, getSub, getDest,
         rmSub, rmDest,
         mkTrn,
         addTx,  getTx, rmTx, rmThisTx,
         getCurTx,
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
                      conId      :: Con,
                      conCon     :: P.Connection,
                      conOwner   :: ThreadId,
                      conHisBeat :: UTCTime,
                      conMyBeat  :: UTCTime, 
                      conWait    :: Int,
                      conSubs    :: [SubEntry],
                      conDests   :: [DestEntry], 
                      conThrds   :: [ThreadEntry],
                      conErrs    :: [F.Frame],
                      conRecs    :: [Receipt],
                      conAcks    :: [P.MsgId]}

  instance Eq Connection where
    c1 == c2 = conId c1 == conId c2

  -------------------------------------------------------------------------
  -- | Options passed to a connection
  -------------------------------------------------------------------------
  data Copt = 
    -- | Tells the connection to wait /n/ milliseconds for the 'Receipt' 
    --   sent with 'F.Disconnect' at the end of the session.
    --   The /Stomp/ protocol advises to request a receipt 
    --   and to wait for it before actually closing the 
    --   socket. Many brokers, however, do not 
    --   implement this feature (or implement it inappropriately,
    --   closing the connection immediately after having sent
    --   the receipt).
    --   'withConnection', for this reason, ignores 
    --   the receipt by default and simply closes the socket
    --   after having sent the 'F.Disconnect' frame.
    --   If your broker shows a correct behaviour, 
    --   it is advisable to use this option.
    OWaitBroker Int |

    -- | The maximum size of TCP/IP packets.
    --   Indirectly, this options also defines the
    --   maximum message size which is /10 * maxReceive/.
    --   By default, the maximum packet size is 1024 bytes.
    OMaxRecv    Int |

    -- | This option defines the client\'s bid
    --   for negotiating heart beats (see 'F.HeartBeat'). 
    --   By default, no heart beats are sent or accepted
    OHeartBeat  (F.Heart) |

    -- | Authentication: user and password
    OAuth String String |

    -- | Identification: specifies the JMS Client ID for persistant connections
    OClientId String

    deriving (Eq, Show)

  ------------------------------------------------------------------------
  -- Same constructor
  ------------------------------------------------------------------------
  is :: Copt -> Copt -> Bool
  is (OWaitBroker _) (OWaitBroker _) = True
  is (OMaxRecv    _) (OMaxRecv    _) = True
  is (OHeartBeat  _) (OHeartBeat  _) = True
  is (OAuth     _ _) (OAuth     _ _) = True
  is (OClientId   _) (OClientId  _)  = True
  is _               _               = False

  noWait :: Int
  noWait = 0

  stdRecv :: Int
  stdRecv = 1024

  noBeat :: F.Heart
  noBeat = (0,0)

  noAuth :: (String, String)
  noAuth = ("","")

  noCliId :: String
  noCliId = ""

  oWaitBroker :: [Copt] -> Int
  oWaitBroker os = case find (is $ OWaitBroker 0) os of
                     Just (OWaitBroker d) -> d
                     _   -> noWait

  oMaxRecv :: [Copt] -> Int
  oMaxRecv os = case find (is $ OMaxRecv 0) os of
                  Just (OMaxRecv i) -> i
                  _ -> stdRecv

  oHeartBeat :: [Copt] -> F.Heart
  oHeartBeat os = case find (is $ OHeartBeat (0,0)) os of
                    Just (OHeartBeat b) -> b
                    _ -> noBeat

  oAuth :: [Copt] -> (String, String)
  oAuth os = case find (is $ OAuth "" "") os of
               Just (OAuth u p) -> (u, p)
               _   -> noAuth

  oCliId :: [Copt] -> String
  oCliId os = case find (is $ OClientId "") os of
               Just (OClientId i) -> i
               _   -> noCliId

  findCon :: Con -> [Connection] -> Maybe Connection
  findCon cid = find (\c -> conId c == cid)

  mkConnection :: Con -> P.Connection -> ThreadId -> UTCTime -> [Copt] -> Connection
  mkConnection cid c myself t os = Connection cid c myself t t (oWaitBroker os)
                                              [] [] [] [] [] []

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
  -- Sub, Dest and Thread Entry
  ------------------------------------------------------------------------
  type SubEntry    = (Sub, Chan F.Frame)
  type DestEntry   = (String, Chan F.Frame)
  type ThreadEntry = (ThreadId, [Transaction])

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

  updCon :: Connection -> [Connection] -> [Connection]
  updCon c cs = let !cs' = delete c cs in c:cs' 
  
  ------------------------------------------------------------------------
  -- Transaction 
  ------------------------------------------------------------------------
  data Transaction = Trn {
                       txId      :: Tx,
                       txState   :: TxState,
                       txTmo     :: Int,
                       txAbrtAck :: Bool,
                       txAbrtRc  :: Bool,
                       txAcks    :: [P.MsgId],
                       txRecs    :: [Receipt]
                     }

  instance Eq Transaction where
    t1 == t2 = txId t1 == txId t2

  findTx :: Tx -> [Transaction] -> Maybe Transaction
  findTx tx = find (\x -> txId x == tx) 

  mkTrn :: Tx -> [Topt] -> Transaction
  mkTrn tx os = Trn {
                  txId      = tx,
                  txState   = TxStarted,
                  txTmo     = tmo os,
                  txAbrtAck = hasTopt OAbortMissingAcks os,
                  txAbrtRc  = hasTopt OWithReceipts     os,
                  txAcks    = [],
                  txRecs    = []}

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
            --      that have not yet been confirmed by the broker
            --      (including receipts requested by user calls
            --       such as /writeQ/ or /ackWith/).
            --
            --   If receipts are pending, when the transaction
            --   is ready to terminate and 'OTimeout' with
            --   a value /> 0/ is given, the transaction will
            --   wait for pending receipts; otherwise
            --   the transaction will be aborted with 'TxException'.
            --   Note that it, usually, does not make sense to use
            --   this options without 'OTimeout',
            --   since it is in all probability that a receipt 
            --   has not yet been confirmed when the transaction terminates.
            | OWithReceipts 
            -- | If a message has been received from a 
            --   queue with 'OMode' option other 
            --   than 'F.Auto' and this message has not yet been
            --   acknowledged when the transaction is ready
            --   to terminate, the /ack/ is /missing/.
            --   With this option, the transaction 
            --   will not commit with missing /acks/,
            --   but abort and raise 'TxException'.
            | OAbortMissingAcks 
    deriving (Eq, Show)

  hasTopt :: Topt -> [Topt] -> Bool
  hasTopt o os = o `elem` os 

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
  checkReceiptTx r = notElem r . txRecs 

  txPendingAck :: Transaction -> Bool
  txPendingAck t = txAbrtAck t && not (null $ txAcks t)

  txReceipts :: Transaction -> Bool
  txReceipts t = txAbrtRc t && not (null $ txRecs t) 

  ------------------------------------------------------------------------
  -- State 
  ------------------------------------------------------------------------
  {-# NOINLINE con #-}
  con :: MVar [Connection]
  con = unsafePerformIO $ newMVar []
 
  ------------------------------------------------------------------------
  -- Add connection to state
  ------------------------------------------------------------------------
  addCon :: Connection -> IO ()
  addCon c = modifyMVar_ con $ \cs -> return (c:cs)

  ------------------------------------------------------------------------
  -- get connection from state
  ------------------------------------------------------------------------
  getCon :: Con -> IO Connection
  getCon cid = withCon cid $ \c -> return (c, c) 

  ------------------------------------------------------------------------
  -- remove connection from state
  ------------------------------------------------------------------------
  rmCon :: Con -> IO ()
  rmCon cid = modifyMVar_ con $ \cs -> 
    case findCon cid cs of
      Nothing -> return cs
      Just c  -> 
        return $ delete c cs

  ------------------------------------------------------------------------
  -- Apply an action that may change a connection to the state
  ------------------------------------------------------------------------
  withCon :: Con -> (Connection -> IO (Connection, a)) -> IO a
  withCon cid op = modifyMVar con (\cs -> 
     case findCon cid cs of
       Nothing   -> 
         throwIO $ ConnectException $
                 "No such Connection: " ++ show cid
       Just c    -> do
         (c', x) <- op c
         let cs' = updCon c' cs
         return (cs', x))

  ------------------------------------------------------------------------
  -- Log heart-beats
  ------------------------------------------------------------------------
  logTime :: Con -> (UTCTime -> Connection -> Connection) -> IO ()
  logTime cid f = 
    getCurrentTime >>= \t -> withCon cid (\c -> return (f t c, ()))

  logSend :: Con -> IO ()
  logSend cid = logTime cid setMyTime

  logReceive :: Con -> IO ()
  logReceive cid = logTime cid setHisTime

  ------------------------------------------------------------------------
  -- add and remove sub and dest
  ------------------------------------------------------------------------
  addSub :: Con -> SubEntry -> IO ()
  addSub cid s  = withCon cid $ \c -> return (addSubToCon s c, ())

  addDest :: Con -> DestEntry -> IO ()
  addDest cid d = withCon cid $ \c -> return (addDestToCon d c, ())

  rmSub :: Con -> Sub -> IO ()
  rmSub cid sid = withCon cid rm
    where rm c = case getSub sid c of 
                   Nothing -> return (c, ())
                   Just ch -> return (rmSubFromCon (sid, ch) c, ())

  rmDest :: Con -> String -> IO ()
  rmDest cid dst = withCon cid rm
    where rm c = case getDest dst c of 
                   Nothing -> return (c, ())
                   Just ch -> return (rmDestFromCon (dst, ch) c, ())

  ------------------------------------------------------------------------
  -- add transaction to connection
  -- Note: transactions are kept per threadId
  ------------------------------------------------------------------------
  addTx :: Transaction -> Con -> IO ()
  addTx t cid = withCon cid $ \c -> do
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
      Just ts -> return $ findTx tx ts

  ------------------------------------------------------------------------
  -- apply an action that may change a transaction to the state
  ------------------------------------------------------------------------
  updTx :: Tx -> Con -> (Transaction -> Transaction) -> IO ()
  updTx tx cid f = withCon cid $ \c -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return (c, ())
      Just ts -> 
        case findTx tx ts of
          Nothing -> return (c, ())
          Just t  -> 
            let !t' = f t
            in  return (c {conThrds = 
                             updTxInThrds t' tid (conThrds c) ts}, 
                        ())
    where updTxInThrds t tid ts trns =
            let !trns' = delete t trns
                !ts'   = deleteBy eq (tid, trns) ts
             in (tid, t : trns') : ts'

  ------------------------------------------------------------------------
  -- update transaction state
  ------------------------------------------------------------------------
  updTxState :: Tx -> Con -> TxState -> IO ()
  updTxState tx cid st = updTx tx cid (setTxState st)

  ------------------------------------------------------------------------
  -- get current transaction for thread
  ------------------------------------------------------------------------
  getCurTx :: Connection -> IO (Maybe Tx)
  getCurTx c = do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return Nothing
      Just ts -> if null ts then return Nothing
                   else return $ Just $ (txId . head) ts

  ------------------------------------------------------------------------
  -- apply a change of the current transaction 
  -- or the connection (if there is no transaction) to the state
  ------------------------------------------------------------------------
  updCurTx :: (Transaction -> Transaction) ->
              (Connection  -> Connection)  ->
              Connection -> IO (Connection, ())
  updCurTx onTx onCon c = do
    tid  <- myThreadId
    case lookup tid $ conThrds c of
      Nothing -> return (onCon c, ())
      Just ts -> if null ts 
                   then return (onCon c, ())
                   else do
                      let t   = head ts
                      let t'  = onTx t 
                      let ts' = t' : tail ts
                      let c'  = c {conThrds = 
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
  -- this is used by the listener (which is not in the thread list)
  ------------------------------------------------------------------------
  forceRmRec :: Con -> Receipt -> IO ()
  forceRmRec cid r = withCon cid doRmRec 
    where doRmRec c = 
            case find (== r) $ conRecs c of
              Just _  -> return (rmRecFromCon r c, ())
              Nothing -> 
                let thrds = map rmRecFromThrd $ conThrds c
                in  return (c {conThrds = thrds}, ()) 
          rmRecFromThrd   (thrd, ts) = (thrd, map (rmRecFromTx r) ts)

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
                   else return $ onTx $ head ts

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
  rmThisTx tx cid = withCon cid $ \c -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return (c, ())
      Just ts -> 
        if null ts 
          then return (c {conThrds = deleteBy eq (tid, []) (conThrds c)}, ())
          else 
            case findTx tx ts of
              Nothing -> return (c, ())
              Just t  -> do
                let ts' = delete t ts
                if null ts' 
                  then return (c {conThrds = 
                                   deleteBy eq (tid, ts) (conThrds c)},  ())
                  else return (c {conThrds = (tid, ts') : 
                                   deleteBy eq (tid, ts) (conThrds c)}, ())

  ------------------------------------------------------------------------
  -- remove the current transaction
  ------------------------------------------------------------------------
  rmTx :: Con -> IO ()
  rmTx cid = withCon cid $ \c -> do
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

