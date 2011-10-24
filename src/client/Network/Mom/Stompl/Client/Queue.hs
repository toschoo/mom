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
                   Topt(..),
                   getPendingAcks, ack)
where

  ----------------------------------------------------------------
  -- todo
  -- - generate receipts newQueue!
  -- - receipts on commit/abort
  -- - nack
  -- - forceTx
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
                                      throwIO)

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

  eqCon :: ConEntry -> ConEntry -> Bool
  eqCon = eq

  updCon :: (Con, Connection) -> [ConEntry] -> [ConEntry]
  updCon c cs = c : deleteBy eqCon c cs

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

  rmCon :: ConEntry -> IO ()
  rmCon c = modifyMVar_ con $ \cs -> do
    let !cs' = deleteBy eqCon c cs
    return cs'

  type SubEntry = (Sub, Chan F.Frame)

  addSub :: SubEntry -> Connection -> Connection
  addSub s c = c {conSubs = s : conSubs c}

  getSub :: Sub -> Connection -> Maybe (Chan F.Frame)
  getSub sid c = lookup sid (conSubs c)

  rmSub :: SubEntry -> Connection -> Connection
  rmSub s c = c {conSubs = ss} 
    where ss = deleteBy eq s (conSubs c)

  type DestEntry = (String, Chan F.Frame)

  addDest :: DestEntry -> Connection -> Connection
  addDest d c = c {conDests = d : conDests c}

  getDest :: String -> Connection -> Maybe (Chan F.Frame)
  getDest dst c = lookup dst (conDests c)

  rmDest :: DestEntry -> Connection -> Connection
  rmDest d c = c {conDests = ds}
    where ds = deleteBy eq d (conDests c)

  type ThreadEntry = (ThreadId, [TxEntry])
  
  data Transaction = Trn {
                       txTmo     :: Int,
                       txWait    :: Bool,
                       txAbrtErr :: Bool,
                       txAbrtAck :: Bool,
                       txAcks    :: [String],
                       txErrs    :: [F.Frame],
                       txRecs    :: [Receipt]
                     }

  mkTrn :: [Topt] -> Transaction
  mkTrn os = Trn {
               txTmo     = tmo os,
               txWait    = hasTopt os OWaitReceipts,
               txAbrtErr = hasTopt os OAbortOnError,
               txAbrtAck = hasTopt os OAbortMissingAcks,
               txAcks    = [],
               txErrs    = [],
               txRecs    = []
             }

  data Topt = OTimeout Int | OWaitReceipts | OAbortOnError | OAbortMissingAcks
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

  addErrToTx :: F.Frame -> Transaction -> Transaction
  addErrToTx f t = t {txErrs = f : txErrs t}

  addErrToCon :: F.Frame -> Connection -> Connection
  addErrToCon f c = c {conErrs = f : conErrs c}

  rmErrFromTx :: F.Frame -> Transaction -> Transaction
  rmErrFromTx f t = t {txErrs = delete f $ txErrs t}

  rmErrFromCon :: F.Frame -> Connection -> Connection
  rmErrFromCon f c = c {conErrs = delete f $ conErrs c}

  addErr :: Con -> F.Frame -> IO ()
  addErr cid f = do
    let toTx  = addErrToTx  f
    let toCon = addErrToCon f
    withCon (updCurTx toTx toCon) cid

  rmErr :: Con -> F.Frame -> IO ()
  rmErr cid f = do
    let fromTx  = rmErrFromTx  f
    let fromCon = rmErrFromCon f
    withCon (updCurTx fromTx fromCon) cid

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

  rmTx :: Con -> IO ()
  rmTx cid = withCon rmTx' cid 

  rmTx' :: ConEntry -> IO (Connection, ())
  rmTx' (cid, c) = do
    tid <- myThreadId
    case lookup tid (conThrds c) of
      Nothing      -> return (c, ())
      Just ts -> 
        if null ts then return (c, ())
          else do
            return (c {conThrds = (tid, tail ts) : 
                           deleteBy eq (tid, ts) (conThrds c)}, ())

  vers :: [F.Version]
  vers = [(1,0), (1,1)]

  withConnection_ :: String -> Int -> Int -> String -> String -> F.Heart -> 
                     (Con -> IO ()) -> IO ()
  withConnection_ host port mx usr pwd beat act = do
    _ <- withConnection host port mx usr pwd beat act
    return ()

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
                   (\l -> do 
                       killThread l
                       _ <- P.disconnect c ""
                       rmCon (cid, mkConnection c me))
                   (\_ -> act cid)
    
  data Queue a = SendQ {
                   qCon  :: Con,
                   qDest :: String,
                   qName :: String,
                   qRec  :: Bool,
                   qWait :: Bool,
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
  typeOf (SendQ _ _ _ _ _ _  ) = SendQT
  typeOf (RecvQ _ _ _ _ _ _ _) = RecvQT

  -- OForceTx
  data Qopt = OSend | OReceive | OWithReceipt | OWaitReceipt | OMode F.AckMode | OAck
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
  newQueue cid qn dst os hs conv = 
    withCon (newQueue' qn dst os hs conv) cid
 
  newQueue' :: String -> String -> [Qopt] -> [F.Header] -> 
               Converter a -> ConEntry -> IO (Connection, Queue a)
  newQueue' qn dst os hs conv (cid, c) = do
    if not $ P.connected (conCon c)
      then throwIO $ ConnectException $ 
                 "Not connected (" ++ (show cid) ++ ")"
      else 
        if hasQopt OSend os 
          then 
            case conv of
              (OutBound f) -> newSendQ cid c qn dst os f
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

  newSendQ :: Con -> Connection -> String -> String -> [Qopt] -> 
              OutBoundF a -> IO (Connection, Queue a)
  newSendQ cid c qn dst os conv = 
    let q = SendQ {
              qCon  = cid,
              qDest = dst,
              qName = qn,
              qRec  = if hasQopt OWithReceipt os then True else False,
              qWait = if hasQopt OWaitReceipt os then True else False,
              qTo   = conv}
    in return (c, q)

  newRecvQ :: Con        -> Connection -> String -> String -> 
              [Qopt]     -> [F.Header] ->
              InBoundF a -> IO (Connection, Queue a)
  newRecvQ cid c qn dst os hs conv = do
    let am = ackMode os
    let au = hasQopt OAck os
    sid <- mkUniqueSubId
    P.subscribe (conCon c) (P.mkSub (show sid) dst am) "" hs
    ch <- newChan 
    let c' = addDest (dst,  ch) $ addSub (sid, ch) c
    let q = RecvQ {
               qCon  = cid,
               qSub  = sid,
               qDest = dst,
               qName = qn,
               qMode = am,
               qAuto = au,
               qFrom = conv}
    return (c', q)

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
                         | otherwise = 
    writeQ' q mime hs x 

  writeQ' :: Queue a -> Mime.Type -> [F.Header] -> a -> IO Receipt
  writeQ' q mime hs x = do
    c <- getCon (qCon q)
    if not $ P.connected (conCon c)
      then throwIO $ ConnectException $
                 "Not connected (" ++ (show $ qCon q) ++ ")"
      else do
        let conv = qTo q
        s  <- conv x
        tx <- (getCurTx c >>= (\mbT -> 
                 case mbT of
                   Nothing     -> return ""
                   Just (i, t) -> return (show i)))
        rc <- (if qRec q
                  then mkUniqueRecc
                  else return NoRec)
        let m = P.mkMessage "" (qDest q) (qDest q) 
                            mime (B.length s) tx s x

        if qRec q then addRec (qCon q) rc else return ()
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
                         Just (x, t) -> return $ show x))
             rc <- (if with
                      then mkUniqueRecc
                      else return NoRec)
             P.ack  (conCon c) msg {P.msgTx = tx} $ show rc
             if with 
               then waitReceipt cid rc >> return rc
               else return rc

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
                       startTx tx cid
                       x <- op cid
                       return x)
                   (do terminateTx tx cid
                       rmTx  cid) -- abort!

  clearAllErrors :: Con -> IO ()
  clearAllErrors cid = do
    es <- getErrors cid
    mapM_ (rmErr cid) es

  getErrors :: Con -> IO [F.Frame]
  getErrors cid = do
    c   <- getCon cid
    mbT <- getCurTx c
    case mbT of
      Nothing     -> return $ conErrs c
      Just (_, t) -> return $ txErrs t

  solveError :: Con -> F.Frame -> IO ()
  solveError = rmErr 

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

  startTx :: Tx -> Con -> IO ()
  startTx tx cid = withCon (startTx' tx) cid

  startTx' :: Tx -> ConEntry -> IO (Connection, ())
  startTx' tx (cid, c) = do
    P.begin (conCon c) (show tx) ""
    return (c, ())

  terminateTx :: Tx -> Con -> IO ()
  terminateTx tx cid = do
    c   <- getCon cid
    mbT <- getTx tx c
    case mbT of
      Nothing -> throwIO $ TxException $ 
                   "Transaction disappeared: " ++ (show tx)
      Just t  -> 
        if (txErr t) || (txPendingAck t)
          then P.abort (conCon c) (show tx) ""
          else do
            let tm = txTmo t
            ok <- waitTx tx cid tm
            if ok
              then P.commit (conCon c) (show tx) ""
              else P.abort  (conCon c) (show tx) ""

  txErr :: Transaction -> Bool
  txErr t = if (txAbrtErr t) 
              then if (null $ txErrs t) then False
                   else True
              else False

  txPendingAck :: Transaction -> Bool
  txPendingAck t = if (txAbrtAck t)
                     then if (null $ txAcks t) then False
                          else True
                     else False

  waitTx :: Tx -> Con -> Int -> IO Bool
  waitTx tx cid delay = do
    c   <- getCon cid
    mbT <- getTx tx c
    case mbT of
      Nothing -> return True
      Just t  -> 
        if not $ txWait t
          then return True
          else if not (txErr t || txPendingAck t)
               then return True
               else 
                 if delay == 0 then return False
                   else do
                     threadDelay $ ms 1
                     waitTx tx cid (delay - 1)

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
    addErr cid f
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
