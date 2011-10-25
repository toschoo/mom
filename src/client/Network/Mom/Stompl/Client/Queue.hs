{-# OPTIONS -fglasgow-exts -fno-cse #-}
module Network.Mom.Stompl.Client.Queue (
                   P.Message, P.mkMessage,
                   P.msgRaw, msgContent,
                   P.msgType, P.msgLen, P.msgHdrs,
                   Queue, Qopt(..), Converter(..),
                   Factory.Con, 
                   Factory.Rec(..), Receipt,
                   withConnection, 
                   withConnection_, 
                   newQueue, readQ, 
                   writeQ, writeQWith,
                   waitReceipt,
                   withTransaction,
                   withTransaction_,
                   Topt(..), abort,
                   getPendingAcks, ack)
where

  ----------------------------------------------------------------
  -- todo
  -- - forceTx
  -- - nack
  -- - harmonise interface (cid -> tx)
  -- - Heartbeat
  -- - test/check for deadlocks
  ----------------------------------------------------------------

  import qualified Socket   as S
  import qualified Protocol as P
  import           Factory  

  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Client.Exception

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U

  import           Control.Concurrent 
  import           Control.Applicative ((<$>))
  import           Control.Monad
  import           Control.Exception (bracket, finally, 
                                      throwIO, SomeException)
  import qualified Control.Exception as Ex (catch)

  import           System.IO.Unsafe

  import           Data.List (find, deleteBy, delete, insert)
  import           Data.Char (isDigit)

  import           Codec.MIME.Type as Mime (Type)

  msgContent :: P.Message a -> a
  msgContent = P.msgCont

  type Receipt = Rec

  data Connection = Connection {
                      conCon   :: P.Connection,
                      conOwner :: ThreadId,
                      conSubs  :: [SubEntry],
                      conDests :: [DestEntry], -- needs connection in key!
                      conThrds :: [ThreadEntry],
                      conErrs  :: [F.Frame],
                      conRecs  :: [Receipt],
                      conAcks  :: [String]}

  type ConEntry = (Con, Connection)

  mkConnection :: P.Connection -> ThreadId -> Connection
  mkConnection c myself = Connection c myself [] [] [] [] [] []

  eq :: Eq a => (a, b) -> (a, b) -> Bool
  eq x y = fst x == fst y

  eqCon :: Con -> ConEntry -> Bool
  eqCon cid c = fst c == cid 

  updCon :: (Con, Connection) -> [ConEntry] -> [ConEntry]
  updCon c cs = c : deleteBy eq c cs

  {-# NOINLINE con #-}
  con :: MVar [ConEntry]
  con = unsafePerformIO $ newMVar []
 
  addCon :: ConEntry -> IO ()
  addCon c = modifyMVar_ con $ \cs -> return (c:cs)

  withCon :: (ConEntry -> IO (Connection, a)) -> Con -> IO a
  withCon op cid = modifyMVar con (\cs -> 
     case lookup cid cs of
       Nothing   -> 
         throwIO $ ConnectException $
                 "No such Connection: " ++ (show cid)
       Just c    -> do
         (c', x) <- op (cid, c)
         let cs' = updCon (cid, c') cs
         return (cs', x))

  getCon :: Con -> IO Connection
  getCon cid = withCon (\c -> return (snd c, snd c)) cid

  rmCon :: Con -> IO ()
  rmCon cid = modifyMVar_ con $ \cs -> do
    case lookup cid cs of
      Nothing -> return cs
      Just c  -> 
        return $ deleteBy eq (cid, c) cs

  type SubEntry = (Sub, Chan F.Frame)

  addSub :: Con -> SubEntry -> IO ()
  addSub cid s = withCon act cid
    where act (_, c) = return $ (addSubToCon s c, ())

  addSubToCon :: SubEntry -> Connection -> Connection
  addSubToCon s c = c {conSubs = s : conSubs c}

  getSub :: Sub -> Connection -> Maybe (Chan F.Frame)
  getSub sid c = lookup sid (conSubs c)

  rmSub :: SubEntry -> Connection -> Connection
  rmSub s c = c {conSubs = ss} 
    where ss = deleteBy eq s (conSubs c)

  type DestEntry = (String, Chan F.Frame)

  addDest :: Con -> DestEntry -> IO ()
  addDest cid d = withCon act cid
    where act (_, c) = return $ (addDestToCon d c, ())

  addDestToCon :: DestEntry -> Connection -> Connection
  addDestToCon d c = c {conDests = d : conDests c}

  getDest :: String -> Connection -> Maybe (Chan F.Frame)
  getDest dst c = lookup dst (conDests c)

  rmDest :: DestEntry -> Connection -> Connection
  rmDest d c = c {conDests = ds}
    where ds = deleteBy eq d (conDests c)

  type ThreadEntry = (ThreadId, [TxEntry])
  
  data Transaction = Trn {
                       txState   :: TxState,
                       txTmo     :: Int,
                       txAbrtAck :: Bool,
                       txAbrtRc  :: Bool,
                       txWithRc  :: Bool,
                       txAcks    :: [String],
                       txRecs    :: [Receipt]
                     }

  data TxState = TxStarted | TxEnded
    deriving (Eq, Show)

  mkTrn :: [Topt] -> Transaction
  mkTrn os = Trn {
               txState   = TxStarted,
               txTmo     = tmo os,
               txAbrtAck = hasTopt os OAbortMissingAcks,
               txAbrtRc  = hasTopt os OWithReceipts,
               txAcks    = [],
               txRecs    = []
             }

  data Topt = OTimeout Int | OWithReceipts | OAbortMissingAcks 
    deriving (Eq, Show)

  hasTopt :: [Topt] -> Topt -> Bool
  hasTopt os o = case find (== o) os of
                   Nothing -> False
                   Just _  -> True

  tmo :: [Topt] -> Int
  tmo os = case find isTimeout os of
             Just (OTimeout i) -> i
             _                 -> 0
    where isTimeout o = case o of
                          OTimeout _ -> True
                          _          -> False

  type TxEntry = (Tx, Transaction)

  addTx :: TxEntry -> Con -> IO ()
  addTx t cid = withCon (addTx' t) cid

  addTx' :: TxEntry -> ConEntry -> IO (Connection, ())
  addTx' t (cid, c) = do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> 
        return (c {conThrds = [(tid, [t])]}, ())
      Just ts -> 
        return (c {conThrds = addTx2Thrds t tid (conThrds c) ts}, ())
    where addTx2Thrds tx tid ts trns = 
            (tid, tx : trns) : deleteBy eq (tid, trns) ts

  updTx :: Tx -> Con -> (Transaction -> Transaction) -> IO ()
  updTx tx cid f = withCon (updTx' tx f) cid

  updTx' :: Tx -> (Transaction -> Transaction) -> ConEntry -> IO (Connection, ())
  updTx' tx f (cid, c) = do
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

  setTxState :: TxState -> Transaction -> Transaction
  setTxState st t = t {txState = st}

  updTxState :: Tx -> Con -> TxState -> IO ()
  updTxState tx cid st = updTx tx cid (setTxState st)

  getTx :: Tx -> Connection -> IO (Maybe Transaction)
  getTx tx c = do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing -> return Nothing
      Just ts -> return $ lookup tx ts

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
  updCurTx onTx onCon (cid, c) = do
    tid  <- myThreadId
    case lookup tid $ conThrds c of
      Nothing -> return $ (onCon c, ())
      Just ts -> if null ts 
                   then return (onCon c, ())
                   else do
                      let (tx, t) = head ts
                      let t'  = onTx t 
                      let ts' = (tx, t') : tail ts
                      let c'  = c {conThrds = 
                                     (tid, ts') : (
                                     deleteBy eq (tid, ts) $ conThrds c)}
                      return (c', ())

  addAckToTx :: String -> Transaction -> Transaction
  addAckToTx mid t = t {txAcks = mid : txAcks t}

  addAckToCon :: String -> Connection -> Connection
  addAckToCon mid c = c {conAcks = mid : conAcks c} 

  rmAckFromTx :: String -> Transaction -> Transaction
  rmAckFromTx mid t = t {txAcks = delete mid $ txAcks t}

  rmAckFromCon :: String -> Connection -> Connection
  rmAckFromCon mid c = c {conAcks = delete mid $ conAcks c}

  addAck :: Con -> String -> IO ()
  addAck cid mid = do
    let toTx  = addAckToTx  mid
    let toCon = addAckToCon mid
    withCon (updCurTx toTx toCon) cid

  rmAck :: Con -> String -> IO ()
  rmAck cid mid = do
    let fromTx  = rmAckFromTx  mid
    let fromCon = rmAckFromCon mid
    withCon (updCurTx fromTx fromCon) cid

  addRecToTx :: Receipt -> Transaction -> Transaction
  addRecToTx r t = t {txRecs = r : txRecs t}

  addRecToCon :: Receipt -> Connection -> Connection
  addRecToCon r c = c {conRecs = r : conRecs c}

  rmRecFromTx :: Receipt -> Transaction -> Transaction
  rmRecFromTx r t = t {txRecs = delete r $ txRecs t}

  rmRecFromCon :: Receipt -> Connection -> Connection
  rmRecFromCon r c = c {conRecs = delete r $ conRecs c}

  addRec :: Con -> Receipt -> IO ()
  addRec cid r = do
    let toTx  = addRecToTx  r
    let toCon = addRecToCon r
    withCon (updCurTx toTx toCon) cid

  rmRec :: Con -> Receipt -> IO ()
  rmRec cid r = do
    let fromTx  = rmRecFromTx  r
    let fromCon = rmRecFromCon r
    withCon (updCurTx fromTx fromCon) cid

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

  checkReceiptTx :: Receipt -> Transaction -> Bool
  checkReceiptTx r t = case find (== r) $ txRecs t of
                         Nothing -> True
                         Just _  -> False

  checkReceiptCon :: Receipt -> Connection -> Bool
  checkReceiptCon r c = case find (== r) $ conRecs c of
                         Nothing -> True
                         Just _  -> False

  checkReceipt :: Con -> Receipt -> IO Bool
  checkReceipt cid r = do
    let onTx  = checkReceiptTx r
    let onCon = checkReceiptCon r
    checkCurTx onTx onCon cid

  rmThisTx :: Tx -> Con -> IO ()
  rmThisTx tx cid = withCon (rmThisTx' tx) cid 
  
  rmThisTx' :: Tx -> ConEntry -> IO (Connection, ())
  rmThisTx' tx (cid, c) = do
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
  rmTx cid = withCon rmTx' cid 

  rmTx' :: ConEntry -> IO (Connection, ())
  rmTx' (cid, c) = do
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

  -- == State ===========================================================

  vers :: [F.Version]
  vers = [(1,0), (1,1)]

  withConnection_ :: String -> Int -> Int -> String -> String -> F.Heart -> 
                     (Con -> IO ()) -> IO ()
  withConnection_ host port mx usr pwd beat act = 
    withConnection host port mx usr pwd beat act >>= (\_ -> return ())

  withConnection :: String -> Int -> Int -> String -> String -> F.Heart -> 
                    (Con -> IO a) -> IO a
  withConnection host port mx usr pwd beat act = do
    me  <- myThreadId
    cid <- mkUniqueConId
    c   <- P.connect host port mx usr pwd vers beat
    if not $ P.connected c 
      then throwIO $ ConnectException $ P.getErr c
      else bracket (do addCon (cid, mkConnection c me)
                       forkIO $ listen cid)
                   -- if an exception is raised in the post-action
                   -- we at least will remove the connection
                   -- from our state -- and then reraise 
                   (\l -> do 
                       Ex.catch (do killThread l
                                    -- unsubscribe all queues?
                                    _ <- P.disconnect c ""
                                    rmCon cid)
                                (\e -> do rmCon cid
                                          throwIO (e::SomeException)))
                   (\_ -> act cid)
    
  data Queue a = SendQ {
                   qCon  :: Con,
                   qDest :: String,
                   qName :: String,
                   qRec  :: Bool,
                   qWait :: Bool,
                   qTx   :: Bool,
                   qTo   :: OutBoundF a}
               | RecvQ {
                   qCon  :: Con,
                   qSub  :: Sub,
                   qDest :: String,
                   qName :: String,
                   qMode :: F.AckMode,
                   qAuto :: Bool, -- library creates Ack
                   qFrom :: InBoundF a}

  instance Eq (Queue a) where
    q1 == q2 = qName q1 == qName q2

  data QType = SendQT | RecvQT
    deriving (Eq)

  typeOf :: Queue a -> QType
  typeOf (SendQ _ _ _ _ _ _ _) = SendQT
  typeOf (RecvQ _ _ _ _ _ _ _) = RecvQT

  -- OForceTx
  data Qopt = OSend | OReceive | OWithReceipt | OWaitReceipt | 
              OMode F.AckMode  | OAck         | OForceTx
    deriving (Show, Read, Eq) 

  hasQopt :: Qopt -> [Qopt] -> Bool
  hasQopt o os = case find (== o) os of
                   Nothing -> False
                   Just _  -> True

  ackMode :: [Qopt] -> F.AckMode
  ackMode os = case find isMode os of
                 Just (OMode x) -> x
                 _              -> F.Auto
    where isMode x = case x of
                       OMode m -> True
                       _       -> False
  type OutBoundF a = a -> IO B.ByteString
  type InBoundF  a = Mime.Type -> Int -> [F.Header] -> B.ByteString -> IO a
  data Converter a = OutBound (OutBoundF a)
                    | InBound (InBoundF  a)

  newQueue :: Con -> String -> String -> [Qopt] -> [F.Header] -> 
              Converter a -> IO (Queue a)
  newQueue cid qn dst os hs conv = do
    c <- getCon cid
    if not $ P.connected (conCon c)
      then throwIO $ ConnectException $ 
                 "Not connected (" ++ (show cid) ++ ")"
      else 
        if hasQopt OSend os 
          then 
            case conv of
              (OutBound f) -> newSendQ cid qn dst os f
              _            -> throwIO $ QueueException $
                                    "InBound Converter for SendQ (" ++ 
                                    (show qn) ++ ")"
          else 
            if hasQopt OReceive os
              then 
                case conv of
                  (InBound f) -> newRecvQ cid c qn dst os hs f
                  _           -> throwIO $ QueueException $ 
                                       "OutBound Converter for RecvQ (" ++
                                          (show qn) ++ ")"
              else throwIO $ QueueException $
                         "No direction indicated (" ++ (show qn) ++ ")"

  newSendQ :: Con -> String -> String -> [Qopt] -> 
              OutBoundF a -> IO (Queue a)
  newSendQ cid qn dst os conv = 
    let q = SendQ {
              qCon  = cid,
              qDest = dst,
              qName = qn,
              qRec  = if hasQopt OWithReceipt os then True else False,
              qWait = if hasQopt OWaitReceipt os then True else False,
              qTx   = if hasQopt OForceTx     os then True else False,
              qTo   = conv}
    in return q

  newRecvQ :: Con        -> Connection -> String -> String -> 
              [Qopt]     -> [F.Header] ->
              InBoundF a -> IO (Queue a)
  newRecvQ cid c qn dst os hs conv = do
    let am   = ackMode os
    let au   = hasQopt OAck os
    let with = hasQopt OWithReceipt os || hasQopt OWaitReceipt os
    sid <- mkUniqueSubId
    rc  <- (if with then mkUniqueRecc else return NoRec)
    P.subscribe (conCon c) (P.mkSub (show sid) dst am) (show rc) hs
    ch <- newChan 
    addSub  cid (sid, ch) 
    addDest cid (dst, ch) 
    let q = RecvQ {
               qCon  = cid,
               qSub  = sid,
               qDest = dst,
               qName = qn,
               qMode = am,
               qAuto = au,
               qFrom = conv}
    if with 
      then do waitReceipt cid rc
              return q
      else    return q

  readQ :: Queue a -> IO (P.Message a)
  readQ q | typeOf q == SendQT = throwIO $ QueueException $
                                     "Read on a SendQ: " ++ (qName q)
          | otherwise = do
    c <- getCon (qCon q)
    if not $ P.connected (conCon c)
      then throwIO $ QueueException $ "Not connected: " ++ (show $ qCon q)
      else case getSub (qSub q) c of
             Nothing -> throwIO $ QueueException $ 
                           "Unknown queue " ++ (qName q)
             Just ch -> do
               m <- (readChan ch >>= frmToMsg q)
               if (qMode q) /= F.Auto
                 then if (qAuto q) then ack (qCon q) m
                      else addAck (qCon q)  (P.msgId m)
                 else return ()
               return m

  writeQ :: Queue a -> Mime.Type -> [F.Header] -> a -> IO ()
  writeQ q mime hs x | typeOf q == RecvQT = 
                         throwIO $ QueueException $
                           "Write with RecvQ (" ++ (qName q) ++ ")"
                     | otherwise = 
    writeQWith q mime hs x >>= (\_ -> return ())

  writeQWith :: Queue a -> Mime.Type -> [F.Header] -> a -> IO Receipt
  writeQWith q mime hs x | typeOf q == RecvQT = 
                             throwIO $ QueueException $
                               "Write with RecvQ (" ++ (qName q) ++ ")"
                         | otherwise = do
    c <- getCon (qCon q)
    if not $ P.connected (conCon c)
      then throwIO $ ConnectException $
                 "Not connected (" ++ (show $ qCon q) ++ ")"
      else do
        tx <- (getCurTx c >>= (\mbT -> 
                 case mbT of
                   Nothing     -> return ""
                   Just (i, t) -> return (show i)))
        if (null tx) && (qTx q)
          then throwIO $ QueueException $
                 "Queue '" ++ (qName q) ++ 
                 "' with OForceTx used outside Transaction"
          else do
            let conv = qTo q
            s  <- conv x
            rc <- (if qRec q then mkUniqueRecc else return NoRec)
            let m = P.mkMessage "" (qDest q) (qDest q) 
                                mime (B.length s) tx s x
            when (qRec q) $ addRec (qCon q) rc 
            P.send (conCon c) m (show rc) hs 
            if (qRec q) && (qWait q) 
              then waitReceipt (qCon q) rc >> return rc
              else return rc

  ack :: Con -> P.Message a -> IO ()
  ack cid msg = do
    r <- ack' cid False msg
    rmAck cid (P.msgId msg)

  ackWith :: Con -> P.Message a -> IO Receipt
  ackWith cid msg = do
    r <- ack' cid True msg  
    rmAck cid (P.msgId msg)
    return r

  ack' :: Con -> Bool -> P.Message a -> IO Receipt
  ack' cid with msg = do
    c <- getCon cid
    if not $ P.connected (conCon c) 
      then throwIO $ ConnectException $ 
             "Not connected (" ++ (show cid) ++ ")"
      else if null (P.msgId msg)
           then throwIO $ ProtocolException "No message id in message!"
           else do
             tx <- (getCurTx c >>= (\mbT -> 
                       case mbT of
                         Nothing     -> return ""
                         Just (x, _) -> return $ show x))
             let msg' = msg {P.msgTx = tx}
             if with 
               then do
                 rc <- mkUniqueRecc
                 addRec cid rc
                 P.ack  (conCon c) msg' $ show rc
                 waitReceipt cid rc 
                 return rc
               else do
                 P.ack (conCon c) msg' ""
                 return NoRec

  withTransaction_ :: Con -> [Topt] -> (Con -> IO ()) -> IO ()
  withTransaction_ cid os op = do
    _ <- withTransaction cid os op
    return ()

  withTransaction :: Con -> [Topt] -> (Con -> IO a) -> IO a
  withTransaction cid os op = do
    tx <- mkUniqueTxId
    let t = mkTrn os
    c <- getCon cid
    if not $ P.connected (conCon c)
      then throwIO $ ConnectException $
             "Not connected (" ++ (show cid) ++ ")"
      else finally (do addTx (tx, t) cid
                       startTx cid c tx t 
                       x <- op cid
                       updTxState tx cid TxEnded
                       return x)
                   -- if an exception is raised in terminate
                   -- we at least will remove the transaction
                   -- from our state and then reraise 
                   (do Ex.catch (terminateTx tx cid)
                          (\e -> do rmThisTx tx cid
                                    throwIO (e::SomeException)))

  getPendingAcks :: Con -> IO [String]
  getPendingAcks cid = do
    c   <- getCon cid
    mbT <- getCurTx c
    case mbT of
      Nothing     -> return $ conAcks c
      Just (_, t) -> return $ txAcks  t

  waitReceipt :: Con -> Receipt -> IO ()
  waitReceipt cid r = do
    ok <- checkReceipt cid r
    if ok then return ()
      else do 
        threadDelay $ ms 1
        waitReceipt cid r

  abort :: String -> IO ()
  abort e = throwIO $ TxException $
              "Tx aborted by application: " ++ e

  terminateTx :: Tx -> Con -> IO ()
  terminateTx tx cid = do
    c   <- getCon cid
    mbT <- getTx tx c
    case mbT of
      Nothing -> throwIO $ OuchException $ 
                   "Transaction disappeared: " ++ (show tx)
      Just t  -> 
        if txState t /= TxEnded
          then endTx False cid c tx t
          else
            if (txReceipts t) || (txPendingAck t)
              then do
                ok <- waitTx tx cid $ txTmo t
                if ok
                  then endTx True cid c tx t
                  else do
                    endTx False cid c tx t
                    let m = if txReceipts t then "Receipts" else "Acks"
                    throwIO $ TxException $
                       "Transaction aborted: Missing " ++ m
              else endTx True cid c tx t

  startTx :: Con -> Connection -> Tx -> Transaction -> IO ()
  startTx cid c tx t = do
    rc <- (if txAbrtRc t then mkUniqueRecc else return NoRec)
    when (txAbrtRc t) $ addRec cid rc 
    P.begin (conCon c) (show tx) (show rc)

  endTx :: Bool -> Con -> Connection -> Tx -> Transaction -> IO ()
  endTx x cid c tx t = do
    let w = txTmo t > 0 
    rc <- (if w then mkUniqueRecc else return NoRec)
    when w $ addRec cid rc 
    if x then P.commit (conCon c) (show tx) (show rc)
         else P.abort  (conCon c) (show tx) (show rc)
    ok <- waitTx tx cid $ txTmo t
    rmTx cid
    if ok then return ()
          else throwIO $ TxException $
                 "Transaction in unknown State: " ++
                 "missing receipt for " ++ (if x then "commit!" 
                                                 else "abort!")

  txPendingAck :: Transaction -> Bool
  txPendingAck t = if (txAbrtAck t)
                     then if (null $ txAcks t) then False else True
                     else False

  txReceipts :: Transaction -> Bool
  txReceipts t = if (txAbrtRc t) 
                   then if (null $ txRecs t) then False else True
                   else False

  waitTx :: Tx -> Con -> Int -> IO Bool
  waitTx tx cid delay = do
    c   <- getCon cid
    mbT <- getTx tx c
    case mbT of
      Nothing -> return True
      Just t  -> 
        if (txPendingAck t) || (txReceipts t)
          then 
            if delay <= 0 then return False
              else do
                threadDelay $ ms 1
                waitTx tx cid (delay - 1)
          else return True

  frmToMsg :: Queue a -> F.Frame -> IO (P.Message a)
  frmToMsg q f = do
    let b = F.getBody f
    let conv = qFrom q
    x <- conv (F.getMime f) (F.getLength f) (F.getHeaders f) b
    let m = P.mkMessage (F.getId     f)
                        (F.getSub    f)
                        (F.getDest   f) 
                        (F.getMime   f)
                        (F.getLength f)
                        "" b x
    return m {P.msgHdrs = F.getHeaders f}
    
  listen :: Con -> IO ()
  listen cid = forever $ do
    c <- getCon cid
    let cc = conCon c
    eiF <- S.receive (P.getRc cc) (P.getSock cc) (P.conMax cc)
    case eiF of
      Left e  -> do
        putStrLn $ "Error: " ++ e
        -- set con to not connected, return
        return ()
      Right f -> 
        case F.typeOf f of
          F.Message -> handleMessage cid f
          F.Error   -> handleError cid c f
          F.Receipt -> handleReceipt cid f
          _         -> putStrLn $ "Unexpected Frame: " ++ (show $ F.typeOf f)

  handleMessage :: Con -> F.Frame -> IO ()
  handleMessage cid f = do
    c <- getCon cid
    case getCh c of
      Nothing -> do
        putStrLn $ "Unknown Queue: " ++ (show f)
      Just ch -> 
        writeChan ch f
    where getCh c = let dst = F.getDest f
                        sid = F.getSub  f
                    in if null sid
                      then getDest dst c
                      else if not $ numeric sid
                             then Nothing -- error handling
                             else getSub (Sub $ read sid) c

  handleError :: Con -> Connection -> F.Frame -> IO ()
  handleError cid c f = do
    let e = F.getMsg f ++ ": " ++ (U.toString $ F.getBody f)
    throwTo (conOwner c) (BrokerException e) 

  handleReceipt :: Con -> F.Frame -> IO ()
  handleReceipt cid f = do
    case parseRec $ F.getReceipt f of
      Just r  -> rmRec cid r
      Nothing -> do
        -- log error!
        putStrLn $ "Invalid receipt: " ++ (F.getReceipt f)

  numeric :: String -> Bool
  numeric = and . map isDigit

  ms :: Int -> Int
  ms u = 1000 * u
