{-# Language BangPatterns,CPP #-}
module Network.Mom.Stompl.Client.State (
         msgContent, numeric, ms,
         Connection(..), mkConnection,
         connected, getVersion, 
         EHandler,
         getEH, 
         Copt(..),
         oHeartBeat, oMaxRecv,
         oAuth, oCliId, oStomp, oTmo, oTLS,oEH,
         Transaction(..),
         Topt(..), hasTopt, tmo,
         TxState(..),
         Receipt,
         Message(..), mkMessage, MsgId(..),
         Subscription(..), mkSub,
         logSend, logReceive,
         addCon, rmCon, getCon, withCon, updCon,
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

  import qualified Network.Mom.Stompl.Client.Factory  as Fac

  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Client.Exception

  import           Control.Concurrent 
  import           Control.Exception (throwIO)

  import           System.IO.Unsafe

  import           Data.List (find)
  import           Data.Char (isDigit)
  import           Data.Time.Clock

  import           Data.Conduit.Network.TLS (TLSClientConfig, 
                                             tlsClientConfig,
                                             tlsClientUseTLS)

  import qualified Data.ByteString.Char8 as B

  import           Codec.MIME.Type as Mime (Type) 

  ------------------------------------------------------------------------
  -- | Returns the content of the message in the format 
  --   produced by an in-bound converter
  ------------------------------------------------------------------------
  msgContent :: Message a -> a
  msgContent = msgCont

  ------------------------------------------------------------------------
  -- some helpers
  ------------------------------------------------------------------------
  numeric :: String -> Bool
  numeric = all isDigit

  ------------------------------------------------------------------------
  -- strict deletes
  ------------------------------------------------------------------------
  delete' :: Eq a => a -> [a] -> [a]
  delete' = deleteBy' (==) 

  deleteBy' :: (a -> a -> Bool) -> a -> [a] -> [a]
  deleteBy' _ _ [] = []
  deleteBy' f p (x:xs) | f p x     = xs
                       | otherwise = let !xs' = deleteBy' f p xs
                                      in  x : xs'

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
  type Receipt = Fac.Rec

  ------------------------------------------------------------------------
  -- | Action executed when an Error Frame is received;
  --   the typical use case is logging the text of the Error Frame.
  ------------------------------------------------------------------------
  type EHandler = Fac.Con -> F.Frame -> IO ()

  ------------------------------------------------------------------------
  -- Connection
  ------------------------------------------------------------------------
  data Connection = Connection {
                      conId      :: Fac.Con,        -- Con handle
                      conAddr    :: String,         -- the broker's IP address
                      conPort    :: Int,            -- the broker's port
                      conMax     :: Int,            -- max receive
                      conUsr     :: String,         -- user
                      conPwd     :: String,         -- passcode
                      conCli     :: String,         -- client-id
                      conSrv     :: String,         -- server description
                      conSes     :: String,         -- session identifier 
                      conVers    :: [F.Version],    -- accepted versions
                      conBeat    :: F.Heart,        -- the heart beat
                      conChn     :: Chan F.Frame,   -- sender channel
                      conBrk     :: Bool,
                      conOwner   :: ThreadId,       -- thread that created 
                                                    -- the connection
                      conEH      :: Maybe EHandler, -- Error Handler
                      conHisBeat :: UTCTime,        -- broker's next beat
                      conMyBeat  :: UTCTime,        -- our next beat
                      conWait    :: Int,            -- wait for receipt
                                                    -- before terminating
                      conWaitE   :: Int,            -- wait on error handling
                      conSubs    :: [SubEntry],     -- subscriptions
                      conDests   :: [DestEntry],    -- destinations
                      conThrds   :: [ThreadEntry],  -- threads with transactions
                      conRecs    :: [Receipt],      -- expected receipts 
                      conAcks    :: [MsgId]}        -- expected acks 

  instance Eq Connection where
    c1 == c2 = conId c1 == conId c2

  -------------------------------------------------------------------------
  -- Make a connection. Quite ugly.
  -------------------------------------------------------------------------
  mkConnection :: Fac.Con -> String -> Int         -> 
                             Int    -> String      -> String   -> 
                             String -> [F.Version] -> F.Heart  -> 
                             Chan F.Frame          -> ThreadId -> 
                             UTCTime               -> [Copt]   -> Connection
  mkConnection cid host port mx usr pwd ci vs hs chn myself t os = 
    Connection cid host port mx usr pwd ci "" "" vs hs chn False 
                   myself (oEH os) t t 
                          (oWaitBroker os) 
                          (oWaitError  os) [] [] [] [] []

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

    -- | Wait /n/ milliseconds 
    --   after the connection has been closed by the broker
    --   to give the library some time to process
    --   the error message (if one has been sent).
    --   
    OWaitError  Int |

    -- | The maximum size of TCP/IP packets.
    --   This option is currently ignored.
    --   Instead, 'Data.Conduit.Network' defines
    --   the packet size (currently hard-wired 4KB).
    --   The maximum message size is 1024 times this value,
    --   /i.e./ 4MB.
    OMaxRecv    Int |

    -- | This option defines the client\'s bid
    --   for negotiating heartbeats providing 
    --   an accepted lower and upper bound
    --   expessed as milliseconds between heartbeats.
    --   By default, no heart beats are sent or accepted
    OHeartBeat  (F.Heart) |

    -- | Authentication: user and password
    OAuth String String |

    -- | Identification: specifies the JMS Client ID for persistent connections
    OClientId String |

    -- | With this option set, "connect" will use 
    --   a "STOMP" frame instead of a "CONNECT" frame
    OStomp |

    -- | Connection timeout in milliseconds;
    --   if the broker does not respond to a connect request
    --   within this time frame, a 'ConnectException' is thrown.
    --   If the value is <= 0, the program will wait forever.
    OTmo Int |

    -- | 'TLSClientConfig'
    --        (see 'Data.Conduit.Network.TLS' for details)
    --   for TLS connections.
    --   If the option is not given, 
    --      a plain TCP/IP connection is used.
    OTLS TLSClientConfig |

    -- | Action to handle Error frames;
    --   if the option is not given,
    --   an exception is raised on arrival of an error frame.
    --   If it is given, one should also pass a value
    --   for OWaitError to give the error handler time
    --   to execute.
    OEH EHandler

  instance Show Copt where
    show (OWaitBroker i) = "OWaitBroker" ++ show i
    show (OWaitError  i) = "OWaitError " ++ show i
    show (OMaxRecv    i) = "OMaxRecv "   ++ show i
    show (OHeartBeat  h) = "OHeartBeat " ++ show h
    show (OAuth     u p) = "OAuth " ++ u ++ "/" ++ p
    show (OClientId   u) = "OClientId "  ++ u
    show  OStomp         = "OStomp"  
    show (OTmo        i) = "OTmo"        ++ show i
    show (OTLS        c) = "OTLS      "  ++ show (tlsClientUseTLS c)
    show (OEH         _) = "OEHandler "  

  instance Eq Copt where
    (OWaitBroker i1) == (OWaitBroker i2) = i1 == i2
    (OWaitError  i1) == (OWaitError  i2) = i1 == i2
    (OMaxRecv    i1) == (OMaxRecv    i2) = i1 == i2 
    (OHeartBeat  h1) == (OHeartBeat  h2) = h1 == h2
    (OAuth    u1 p1) == (OAuth    u2 p2) = u1 == u2 && p1 == p2
    (OClientId   u1) == (OClientId   u2) = u1 == u2
    (OTmo        i1) == (OTmo        i2) = i1 == i2
    (OTLS        c1) == (OTLS        c2) = tlsClientUseTLS c1 && 
                                           tlsClientUseTLS c2
    (OEH          _) == (OEH          _) = True
    OStomp           == OStomp           = True
    _                == _                = False

  ------------------------------------------------------------------------
  -- Same constructor
  ------------------------------------------------------------------------
  is :: Copt -> Copt -> Bool
  is (OWaitBroker _) (OWaitBroker _) = True
  is (OWaitError  _) (OWaitError  _) = True
  is (OMaxRecv    _) (OMaxRecv    _) = True
  is (OHeartBeat  _) (OHeartBeat  _) = True
  is (OAuth     _ _) (OAuth     _ _) = True
  is (OClientId   _) (OClientId  _)  = True
  is (OStomp       ) (OStomp      )  = True
  is (OTmo _       ) (OTmo _      )  = True
  is (OTLS      _  ) (OTLS      _ )  = True
  is (OEH       _  ) (OEH       _ )  = True
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

  oWaitError  :: [Copt] -> Int
  oWaitError  os = case find (is $ OWaitError  0) os of
                     Just (OWaitError d) -> d
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

  oStomp :: [Copt] -> Bool
  oStomp os = case find (is OStomp) os of
                Just _  -> True
                Nothing -> False

  oTmo :: [Copt] -> Int
  oTmo os = case find (is $ OTmo 0) os of
              Just (OTmo i) -> i
              _             -> 0

  oTLS :: String -> Int -> [Copt] -> TLSClientConfig
  oTLS h p os = case find (is $ OTLS dcfg) os of
                     Just (OTLS cfg) -> cfg
                     _               -> dcfg
    where dcfg = (tlsClientConfig p $ B.pack h){tlsClientUseTLS=False}

  oEH :: [Copt] -> Maybe EHandler
  oEH os = case find (is $ OEH deh) os of
             Just (OEH eh) -> Just eh
             _             -> Nothing
    where deh _ _ = return ()

  findCon :: Fac.Con -> [Connection] -> Maybe Connection
  findCon cid = find (\c -> conId c == cid)

  addAckToCon :: MsgId -> Connection -> Connection
  addAckToCon mid c = c {conAcks = mid : conAcks c} 

  rmAckFromCon :: MsgId -> Connection -> Connection
  rmAckFromCon mid c = c {conAcks = delete' mid $ conAcks c}

  addRecToCon :: Receipt -> Connection -> Connection
  addRecToCon r c = c {conRecs = r : conRecs c}

  rmRecFromCon :: Receipt -> Connection -> Connection
  rmRecFromCon r c = c {conRecs = delete' r $ conRecs c}

  checkReceiptCon :: Receipt -> Connection -> Bool
  checkReceiptCon r c = case find (== r) $ conRecs c of
                         Nothing -> True
                         Just _  -> False

  ---------------------------------------------------------------------
  -- Connection interfaces
  ---------------------------------------------------------------------
  connected :: Connection -> Bool
  connected = conBrk 

  getVersion :: Connection -> F.Version
  getVersion c = if null (conVers c) 
                   then defVersion
                   else head $ conVers c

  getEH :: Connection -> Maybe EHandler
  getEH = conEH 

  ------------------------------------------------------------------------
  -- Sub, Dest and Thread Entry
  ------------------------------------------------------------------------
  type SubEntry    = (Fac.Sub, Chan F.Frame)
  type DestEntry   = (String, Chan F.Frame)
  type ThreadEntry = (ThreadId, [Transaction])

  addSubToCon :: SubEntry -> Connection -> Connection
  addSubToCon s c = c {conSubs = s : conSubs c}

  getSub :: Fac.Sub -> Connection -> Maybe (Chan F.Frame)
  getSub sid c = lookup sid (conSubs c)

  rmSubFromCon :: SubEntry -> Connection -> Connection
  rmSubFromCon s c = c {conSubs = ss} 
    where !ss = deleteBy' eq s (conSubs c)

  addDestToCon :: DestEntry -> Connection -> Connection
  addDestToCon d c = c {conDests = d : conDests c}

  getDest :: String -> Connection -> Maybe (Chan F.Frame)
  getDest dst c = lookup dst (conDests c)

  rmDestFromCon :: DestEntry -> Connection -> Connection
  rmDestFromCon d c = c {conDests = ds}
    where !ds = deleteBy' eq d (conDests c)

  setHisTime :: UTCTime -> Connection -> Connection
  setHisTime t c = c {conHisBeat = t}

  setMyTime  :: UTCTime -> Connection -> Connection
  setMyTime t c = c {conMyBeat = t}

  _updCon :: Connection -> [Connection] -> [Connection]
  _updCon c cs = let !c' = delete' c cs in c:c'
  
  ------------------------------------------------------------------------
  -- Transaction 
  ------------------------------------------------------------------------
  data Transaction = Trn {
                       txId      :: Fac.Tx,
                       txState   :: TxState,
                       txTmo     :: Int,
                       txAbrtAck :: Bool,
                       txAbrtRc  :: Bool,
                       txAcks    :: [MsgId],
                       txRecs    :: [Receipt]
                     }

  instance Eq Transaction where
    t1 == t2 = txId t1 == txId t2

  findTx :: Fac.Tx -> [Transaction] -> Maybe Transaction
  findTx tx = find (\x -> txId x == tx) 

  mkTrn :: Fac.Tx -> [Topt] -> Transaction
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
            --   Note that it usually does not make sense to use
            --   this option without 'OTimeout',
            --   since, in all probability, there will be receipts 
            --   that have not yet been confirmed 
            --   when the transaction terminates.
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

  addAckToTx :: MsgId -> Transaction -> Transaction
  addAckToTx mid t = t {txAcks = mid : txAcks t}

  rmAckFromTx :: MsgId -> Transaction -> Transaction
  rmAckFromTx mid t = t {txAcks = delete' mid $ txAcks t}

  addRecToTx :: Receipt -> Transaction -> Transaction
  addRecToTx r t = t {txRecs = r : txRecs t}

  rmRecFromTx :: Receipt -> Transaction -> Transaction
  rmRecFromTx r t = t {txRecs = delete' r $ txRecs t}

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
  getCon :: Fac.Con -> IO Connection
  getCon cid = withCon cid $ \c -> return (c, c) 

  ------------------------------------------------------------------------
  -- update connection 
  ------------------------------------------------------------------------
  updCon :: Fac.Con -> Connection -> IO ()
  updCon cid c = withCon cid $ \_ -> return (c, ()) 

  ------------------------------------------------------------------------
  -- remove connection from state
  ------------------------------------------------------------------------
  rmCon :: Fac.Con -> IO ()
  rmCon cid = modifyMVar_ con $ \cs -> 
    case findCon cid cs of
      Nothing -> return cs
      Just c  -> return $ delete' c cs

  ------------------------------------------------------------------------
  -- Apply an action that may change a connection to the state
  ------------------------------------------------------------------------
  withCon :: Fac.Con -> (Connection -> IO (Connection, a)) -> IO a
  withCon cid op = modifyMVar con (\cs -> 
     case findCon cid cs of
       Nothing   -> 
         throwIO $ ConnectException $
                 "No such Connection: " ++ show cid
       Just c    -> do
         (c', x) <- op c
         let cs' = _updCon c' cs
         return (cs', x))

  ------------------------------------------------------------------------
  -- Log heart-beats
  ------------------------------------------------------------------------
  logTime :: Fac.Con -> (UTCTime -> Connection -> Connection) -> IO ()
  logTime cid f = 
    getCurrentTime >>= \t -> withCon cid (\c -> return (f t c, ()))

  logSend :: Fac.Con -> IO ()
  logSend cid = logTime cid setMyTime

  logReceive :: Fac.Con -> IO ()
  logReceive cid = logTime cid setHisTime

  ------------------------------------------------------------------------
  -- add and remove sub and dest
  ------------------------------------------------------------------------
  addSub :: Fac.Con -> SubEntry -> IO ()
  addSub cid s  = withCon cid $ \c -> return (addSubToCon s c, ())

  addDest :: Fac.Con -> DestEntry -> IO ()
  addDest cid d = withCon cid $ \c -> return (addDestToCon d c, ())

  rmSub :: Fac.Con -> Fac.Sub -> IO ()
  rmSub cid sid = withCon cid rm
    where rm c = case getSub sid c of 
                   Nothing -> return (c, ())
                   Just ch -> return (rmSubFromCon (sid, ch) c, ())

  rmDest :: Fac.Con -> String -> IO ()
  rmDest cid dst = withCon cid rm
    where rm c = case getDest dst c of 
                   Nothing -> return (c, ())
                   Just ch -> return (rmDestFromCon (dst, ch) c, ())

  ------------------------------------------------------------------------
  -- add transaction to connection
  -- Note: transactions are kept per threadId
  ------------------------------------------------------------------------
  addTx :: Transaction -> Fac.Con -> IO ()
  addTx t cid = withCon cid $ \c -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> 
        return (c {conThrds = [(tid, [t])]}, ())
      Just ts -> 
        return (c {conThrds = addTx2Thrds t tid (conThrds c) ts}, ())
    where addTx2Thrds tx tid ts trns = 
            (tid, tx : trns) : deleteBy' eq (tid, trns) ts

  ------------------------------------------------------------------------
  -- get transaction from connection
  -- Note: transactions are kept per threadId
  ------------------------------------------------------------------------
  getTx :: Fac.Tx -> Connection -> IO (Maybe Transaction)
  getTx tx c = do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return Nothing
      Just ts -> return $ findTx tx ts

  ------------------------------------------------------------------------
  -- apply an action that may change a transaction to the state
  ------------------------------------------------------------------------
  updTx :: Fac.Tx -> Fac.Con -> (Transaction -> Transaction) -> IO ()
  updTx tx cid f = withCon cid $ \c -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return (c, ())
      Just ts -> 
        case findTx tx ts of
          Nothing -> return (c, ())
          Just t  -> 
            let t' = f t
            in  return (c {conThrds = 
                             updTxInThrds t' tid (conThrds c) ts}, 
                        ())
    where updTxInThrds t tid ts trns =
            let !trns' = delete' t trns
                !ts'   = deleteBy' eq (tid, trns) ts
             in (tid, t : trns') : ts'

  ------------------------------------------------------------------------
  -- update transaction state
  ------------------------------------------------------------------------
  updTxState :: Fac.Tx -> Fac.Con -> TxState -> IO ()
  updTxState tx cid st = updTx tx cid (setTxState st)

  ------------------------------------------------------------------------
  -- get current transaction for thread
  ------------------------------------------------------------------------
  getCurTx :: Connection -> IO (Maybe Fac.Tx)
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
                                         deleteBy' eq (tid, ts) (conThrds c)}
                      return (c', ())

  ------------------------------------------------------------------------
  -- add a pending ack either to the current transaction
  -- or - if there is no transaction - to the connection
  ------------------------------------------------------------------------
  addAck :: Fac.Con -> MsgId -> IO ()
  addAck cid mid = do
    let toTx  = addAckToTx  mid
    let toCon = addAckToCon mid
    withCon cid $ updCurTx toTx toCon

  ------------------------------------------------------------------------
  -- remove a pending ack either from the current transaction
  -- or - if there is no transaction - from the connection
  ------------------------------------------------------------------------
  rmAck :: Fac.Con -> MsgId -> IO ()
  rmAck cid mid = do
    let fromTx  = rmAckFromTx  mid
    let fromCon = rmAckFromCon mid
    withCon cid $ updCurTx fromTx fromCon

  ------------------------------------------------------------------------
  -- add a pending receipt either to the current transaction
  -- or - if there is no transaction - to the connection
  ------------------------------------------------------------------------
  addRec :: Fac.Con -> Receipt -> IO ()
  addRec cid r = do
    let toTx  = addRecToTx  r
    let toCon = addRecToCon r
    withCon cid $ updCurTx toTx toCon

  ------------------------------------------------------------------------
  -- remove a pending receipt either from the current transaction
  -- or - if there is no transaction - from the connection
  ------------------------------------------------------------------------
  rmRec :: Fac.Con -> Receipt -> IO ()
  rmRec cid r = do
    let fromTx  = rmRecFromTx  r
    let fromCon = rmRecFromCon r
    withCon cid $ updCurTx fromTx fromCon

  ------------------------------------------------------------------------
  -- search for a receipt either in connection or transactions.
  -- this is used by the listener (which is not in the thread list)
  ------------------------------------------------------------------------
  forceRmRec :: Fac.Con -> Receipt -> IO ()
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
                Fac.Con -> IO Bool
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
  checkReceipt :: Fac.Con -> Receipt -> IO Bool
  checkReceipt cid r = do
    let onTx  = checkReceiptTx r
    let onCon = checkReceiptCon r
    checkCurTx onTx onCon cid

  ------------------------------------------------------------------------
  -- remove a specific transaction
  ------------------------------------------------------------------------
  rmThisTx :: Fac.Tx -> Fac.Con -> IO ()
  rmThisTx tx cid = withCon cid $ \c -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return (c, ())
      Just ts -> 
        if null ts 
          then return (c {conThrds = deleteBy' eq (tid, []) (conThrds c)}, ())
          else 
            case findTx tx ts of
              Nothing -> return (c, ())
              Just t  -> do
                let ts' = delete' t ts
                if null ts' 
                  then return (c {conThrds = 
                                   deleteBy' eq (tid, ts) (conThrds c)},  ())
                  else return (c {conThrds = (tid, ts') : 
                                   deleteBy' eq (tid, ts) (conThrds c)}, ())

  ------------------------------------------------------------------------
  -- remove the current transaction
  ------------------------------------------------------------------------
  rmTx :: Fac.Con -> IO ()
  rmTx cid = withCon cid $ \c -> do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return (c, ())
      Just ts -> 
        if null ts 
          then return (c {conThrds = deleteBy' eq (tid, []) (conThrds c)}, ())
          else do
            let ts' = tail ts
            if null ts' 
              then return (c {conThrds = 
                               deleteBy' eq (tid, ts) (conThrds c)},  ())
              else return (c {conThrds = (tid, ts') : 
                               deleteBy' eq (tid, ts) (conThrds c)}, ())

  ---------------------------------------------------------------------
  -- Default version, when broker does not send a version
  ---------------------------------------------------------------------
  defVersion :: F.Version
  defVersion = (1,2)

  ---------------------------------------------------------------------
  -- Subscribe abstraction
  ---------------------------------------------------------------------
  data Subscription = Subscription {
                        subId   :: Fac.Sub,   -- subscribe identifier
                        subName :: String,    -- queue name
                        subMode :: F.AckMode  -- ack mode
                      }
    deriving (Show)

  mkSub :: Fac.Sub -> String -> F.AckMode -> Subscription
  mkSub sid qn am = Subscription {
                      subId   = sid,
                      subName = qn,
                      subMode = am}

  ---------------------------------------------------------------------
  -- | Message Identifier
  ---------------------------------------------------------------------
  data MsgId = MsgId String | NoMsg
    deriving (Eq)

  instance Show MsgId where
    show (MsgId s) = s
    show (NoMsg)   = ""

  ------------------------------------------------------------------------
  -- | Any content received from a queue
  --   is wrapped in a message.
  --   It is, in particular, the return value of /readQ/.
  ------------------------------------------------------------------------
  data Message a = Msg {
                     -- | The message Identifier
                     msgId   :: MsgId,
                     -- | The subscription
                     msgSub  :: Fac.Sub,
                     -- | The destination
                     msgDest :: String,
                     -- | The Ack identifier
                     msgAck  :: String,
                     -- | The Stomp headers
                     --   that came with the message
                     msgHdrs :: [F.Header],
                     -- | The /MIME/ type of the content
                     msgType :: Mime.Type,
                     -- | The length of the 
                     --   encoded content
                     msgLen  :: Int,
                     -- | The transaction, in which 
                     --   the message was received
                     msgTx   :: Fac.Tx,
                     -- | The encoded content             
                     msgRaw  :: B.ByteString,
                     -- | The content             
                     msgCont :: a}
  
  ---------------------------------------------------------------------
  -- Create a message
  ---------------------------------------------------------------------
  mkMessage :: MsgId -> Fac.Sub -> String -> String ->
               Mime.Type -> Int -> Fac.Tx -> 
               B.ByteString -> a -> Message a
  mkMessage mid sub dst ak typ len tx raw cont = Msg {
                                             msgId   = mid,
                                             msgSub  = sub,
                                             msgDest = dst,
                                             msgAck  = ak,
                                             msgHdrs = [], 
                                             msgType = typ, 
                                             msgLen  = len, 
                                             msgTx   = tx,
                                             msgRaw  = raw,
                                             msgCont = cont}

