{-# OPTIONS -fglasgow-exts -fno-cse #-}
module Network.Mom.Stompl.Client.Queue (
                   P.Message, P.mkMessage,
                   P.msgRaw, msgContent,
                   P.msgType, P.msgLen, P.msgHdrs,
                   Queue, Qopt(..), Converter(..),
                   Factory.Con,
                   withConnection, 
                   withConnection_, 
                   newQueue, readQ, writeQ,
                   withTransaction,
                   withTransaction_,
                   Topt(..))
where

  ----------------------------------------------------------------
  -- todo
  -- - store errors, acks and receipts in Transaction
  --   (Queues and listen!)
  -- - receipts on commit/abort
  -- - nack
  -- - test/check for deadlocks
  -- - write Transaction State
  -- - generate receipts for queues!
  -- - waitReceipt on writeQ
  -- - forceTx
  -- - Heartbeat
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
  import           Control.Exception (finally, throwIO)
  import           Data.Typeable (Typeable)

  import           System.IO.Unsafe

  import           Data.List (find, deleteBy, delete, insert)
  import           Data.Char (isDigit)

  import           Codec.MIME.Type as Mime (Type, nullType)

  msgContent :: P.Message a -> a
  msgContent = P.msgCont

  data Connection = Connection {
                      conCon   :: P.Connection,
                      conSubs  :: [SubEntry],
                      conDests :: [DestEntry], -- needs connection in key!
                      conThrds :: [ThreadEntry]}

  type ConEntry = (Con, Connection)

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
                       txState   :: TxState, 
                       txWait    :: Bool,
                       txAbrtErr :: Bool,
                       txAbrtAck :: Bool,
                       txRecs    :: [String],
                       txAcks    :: [String],
                       txErrs    :: [F.Frame]
                     }

  mkTrn :: [Topt] -> Transaction
  mkTrn os = Trn {
               txTmo     = tmo os,
               txState   = TxCreated,
               txWait    = hasTopt os OWaitReceipts,
               txAbrtErr = hasTopt os OAbortOnError,
               txAbrtAck = hasTopt os OAbortMissingAcks,
               txRecs    = [],
               txAcks    = [],
               txErrs    = []
             }

  data Topt = OTimeout Int | OWaitReceipts | OAbortOnError | OAbortMissingAcks
    deriving (Eq, Show)

  hasTopt :: [Topt] -> Topt -> Bool
  hasTopt os o = case find (== o) os of
                   Nothing -> False
                   Just _  -> True

  tmo :: [Topt] -> Int
  tmo os = case find isTimeout os of
             Nothing           -> 0
             Just (OTimeout i) -> i
    where isTimeout o = case o of
                          OTimeout _ -> True
                          _          -> False

  data TxState = TxCreated | TxStarted | TxAborted | TxCommitted
    deriving (Eq, Show)

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
    where addTx2Thrds t tid ts trns = 
            (tid, t : trns) : deleteBy eq (tid, trns) ts

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
    cid <- mkUniqueConId
    c   <- P.connect host port mx usr pwd vers beat
    if not $ P.connected c 
      then throwIO $ ConnectException $ P.getErr c
      else finally (do addCon (cid, Connection c [] [] [])
                       _ <- forkIO $ listen cid
                       act cid)
                   (do c' <- P.disconnect c ""
                       rmCon (cid, Connection c [] [] []))
    
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
                 Nothing        -> F.Auto
                 Just (OMode x) -> x
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
               qAuto = False,
               qFrom = conv}
    return (c', q)

  readQ :: Queue a -> IO (P.Message a)
  readQ q | typeOf q == SendQT = throwIO $ QueueException $
                                     "Read on a SendQ: " ++ (qName q)
          | otherwise = do
    withCon (readQ' q) (qCon q)

  readQ' :: Queue a -> ConEntry -> IO (Connection, P.Message a)
  readQ' q (cid, c) = do
    if not $ P.connected (conCon c) 
      then throwIO $ QueueException $ "Not connected: " ++ (show cid)
      else 
        case getSub (qSub q) c of
          Nothing -> throwIO $ QueueException $ "Unknown queue " ++ (qName q)
          Just ch -> do
            m <- (readChan ch >>= frmToMsg q)
            return (c, m)

  writeQ :: Queue a -> Mime.Type -> [F.Header] -> a -> IO ()
  writeQ q mime hs x | typeOf q == RecvQT = 
                         throwIO $ QueueException $
                           "Write with RecvQ (" ++ (qName q) ++ ")"
                     | otherwise = 
    withCon (writeQ' q mime hs x) (qCon q)

  writeQ' :: Queue a -> Mime.Type -> [F.Header] -> a -> ConEntry -> IO (Connection, ())
  writeQ' q mime hs x (cid, c) = 
    if not $ P.connected (conCon c)
      then throwIO $ ConnectException $
                 "Not connected (" ++ (show $ qCon q) ++ ")"
      else do
        let conv = qTo q
        s  <- conv x
        tx <- (getCurTx c >>= (\mbT -> 
                 case mbT of
                   Nothing     -> return ""
                   Just (x, t) -> return (show x)))
        let m = P.mkMessage "" (qDest q) (qDest q) 
                            mime (B.length s) tx s x
        P.send (conCon c) m "" hs 
        return (c, ())

  ack :: Con -> P.Message a -> IO ()
  ack cid msg = withCon (ack' msg) cid

  ack' :: P.Message a -> ConEntry -> IO (Connection, ())
  ack' msg (cid, c) =
    if not $ P.connected (conCon c) 
      then throwIO $ ConnectException $ 
             "Not connected (" ++ (show cid) ++ ")"
      else do
        tx <- (getCurTx c >>= (\mbT -> 
                  case mbT of
                    Nothing     -> return ""
                    Just (x, t) -> return $ show x))
        P.ack  (conCon c) msg {P.msgTx = tx} "" 
        return (c, ())

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
            let tmo = txTmo t
            ok <- waitTx tx cid tmo
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
    c <- getCon cid
    mbT <- getTx tx c
    case mbT of
      Nothing -> return True
      Just t  -> 
        if not $ txWait t
          then return True
          else if null (txRecs t) 
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
          F.Message -> handleMessage c f
          F.Error   -> return ()
          F.Receipt -> return ()

  handleMessage :: Connection -> F.Frame -> IO ()
  handleMessage c f = do
    case getCh of
      Nothing -> do
        putStrLn "Unkown Queue"
        return () -- error handling
      Just ch -> 
        writeChan ch f
    where getCh = let dst = F.getDest f
                      sid = F.getSub  f
                  in if null sid
                    then getDest dst c
                    else if not $ numeric sid
                           then Nothing -- error handling
                           else getSub (Sub $ read sid) c

  numeric :: String -> Bool
  numeric = and . map isDigit

  ms :: Int -> Int
  ms u = 1000 * u
    
