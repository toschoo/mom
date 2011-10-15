{-# OPTIONS -fglasgow-exts -fno-cse #-}
module Network.Mom.Stompl.Client.Queue (
                   Queue, Qopt(..), Converter(..),
                   withConnection, 
                   withConnection_, 
                   newQueue, readQ, writeQ)
where

  import qualified Socket   as S
  import qualified Protocol as P
  import           Factory  

  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Exception 

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


  data Connection = Connection {
                      conCon   :: P.Connection,
                      conSubs  :: [SubEntry],
                      conDests :: [DestEntry]}

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
      else finally (do addCon (cid, Connection c [] [])
                       _ <- forkIO $ listen cid
                       act cid)
                   (do c' <- P.disconnect c ""
                       rmCon (cid, Connection c [] []))

  data Queue a = SendQ {
                   qCon  :: Con,
                   qDest :: String,
                   qName :: String,
                   qRec  :: Bool,
                   qWait :: Bool,
                   qTo   :: a -> IO B.ByteString}
               | RecvQ {
                   qCon  :: Con,
                   qSub  :: Sub,
                   qDest :: String,
                   qName :: String,
                   qMode :: F.AckMode,
                   qAuto :: Bool, -- library creates Ack
                   qFrom :: B.ByteString -> IO a}

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

  hasOpt :: Qopt -> [Qopt] -> Bool
  hasOpt o os = case find (== o) os of
                  Nothing -> False
                  Just _  -> True

  ackMode :: [Qopt] -> F.AckMode
  ackMode os = case find isMode os of
                 Nothing        -> F.Auto
                 Just (OMode x) -> x
    where isMode x = case x of
                       OMode m -> True
                       _       -> False

  data Converter a = OutBound (a -> IO B.ByteString)
                    | InBound (B.ByteString -> IO a)

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
        if hasOpt OSend os 
          then 
            case conv of
              (OutBound f) -> newSendQ cid c qn dst os f
              _            -> throwIO $ QueueException $
                                    "InBound Converter for SendQ (" ++ 
                                    (show qn) ++ ")"
          else 
            if hasOpt OReceive os
              then 
                case conv of
                  (InBound f) -> newRecvQ cid c qn dst os hs f
                  _           -> throwIO $ QueueException $ 
                                       "OutBound Converter for RecvQ (" ++
                                          (show qn) ++ ")"
              else throwIO $ QueueException $
                         "No direction indicated (" ++ (show qn) ++ ")"

  newSendQ :: Con -> Connection -> String -> String -> [Qopt] -> 
              (a -> IO B.ByteString)  -> IO (Connection, Queue a)
  newSendQ cid c qn dst os conv = 
    let q = SendQ {
              qCon  = cid,
              qDest = dst,
              qName = qn,
              qRec  = if hasOpt OWithReceipt os then True else False,
              qWait = if hasOpt OWaitReceipt os then True else False,
              qTo   = conv}
    in return (c, q)

  newRecvQ :: Con -> Connection -> String -> String -> 
              [Qopt] -> [F.Header] ->
              (B.ByteString -> IO a)  -> IO (Connection, Queue a)
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

  writeQ :: Queue a -> String -> [F.Header] -> a -> IO ()
  writeQ q mime hs x | typeOf q == RecvQT = 
                         throwIO $ QueueException $
                           "Write with RecvQ (" ++ (qName q) ++ ")"
                     | otherwise = 
    withCon (writeQ' q mime hs x) (qCon q)

  writeQ' :: Queue a -> String -> [F.Header] -> a -> ConEntry -> IO (Connection, ())
  writeQ' q mime hs x (cid, c) = 
    if not $ P.connected (conCon c)
      then throwIO $ ConnectException $
                 "Not connected (" ++ (show $ qCon q) ++ ")"
      else do
        let conv = qTo q
        s <- conv x
        let m = P.mkMessage "" mime (show $ qSub q) (B.length s) "" s x
        P.send (conCon c) (qDest q) m "" hs 
        return (c, ())

  frmToMsg :: Queue a -> F.Frame -> IO (P.Message a)
  frmToMsg q f = do
    let b = F.getBody f
    let conv = qFrom q
    x <- conv b
    return $ P.mkMessage (F.getId     f)
                         (F.getMime   f)
                         (F.getSub    f)
                         (F.getLength f)
                         "" b x
    
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
    
