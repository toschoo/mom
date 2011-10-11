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

  import           Data.List (find, deleteBy)
  import           Data.Char (isDigit)

  type ConEntry = (Con, P.Connection)

  {-# NOINLINE con #-}
  con :: MVar [ConEntry]
  con = unsafePerformIO $ newMVar []
 
  addCon :: ConEntry -> IO ()
  addCon c = modifyMVar_ con $ \cs -> return (c:cs)

  -- locking needed
  -- two threads may change 
  -- the same connection:
  -- readCon (for read access only)
  -- coCon   (locks   -> takeMVar)
  -- ciCon   (unlocks -> putMVar)
  getCon :: Con -> IO (Maybe P.Connection)
  getCon cid = do
    cs <- takeMVar con
    let !cs' = cs
    putMVar con cs'
    return $ lookup cid cs'

  subCon :: ConEntry -> IO ()
  subCon c = modifyMVar_ con $ \cs -> do
    let !cs' = deleteBy (\x y -> (fst x) == (fst y)) c cs
    return (c:cs')

  rmCon :: ConEntry -> IO ()
  rmCon c = modifyMVar_ con $ \cs -> do
    let !cs' = deleteBy (\x y -> (fst x) == (fst y)) c cs
    return cs'

  type SubEntry = (Sub, Chan F.Frame)

  {-# NOINLINE sub #-}
  sub :: MVar [SubEntry]
  sub = unsafePerformIO $ newMVar []

  addSub :: SubEntry -> IO ()
  addSub s = modifyMVar_ sub $ \ss -> return (s:ss)

  getSub :: Sub -> IO (Maybe (Chan F.Frame))
  getSub sid = do
    ss <- takeMVar sub
    putMVar sub ss
    return $ lookup sid ss

  type DestEntry = (String, Chan F.Frame)

  -- 1.0 only! ----------------------------
  {-# NOINLINE dest #-}
  dest :: MVar [DestEntry]
  dest = unsafePerformIO $ newMVar []

  addDest :: DestEntry -> IO ()
  addDest d = modifyMVar_ dest $ \ds -> return (d:ds)

  getDest :: String -> IO (Maybe (Chan F.Frame))
  getDest qn = do
    ds <- takeMVar dest 
    let !ds' = ds
    putMVar dest ds'
    return $ lookup qn ds'

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
      else finally (do addCon (cid, c)
                       _ <- forkIO $ listen cid
                       act cid)
                   (do c' <- P.disconnect c ""
                       rmCon (cid, c))
                -- sub and dest?
                -- all subs and dests have to be added to Connection
                -- and removed on rmCon
                -- By the way: Dest needs a Connection-Prefix to
                --             distinguish qs that by chance have
                --             the same name in different connections!

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

  data QType = SendQT | RecvQT
    deriving (Eq)

  typeOf :: Queue a -> QType
  typeOf (SendQ _ _ _ _ _ _  ) = SendQT
  typeOf (RecvQ _ _ _ _ _ _ _) = RecvQT

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
  newQueue cid qn dst os hs conv = do
    mbC <- getCon cid
    case mbC of
      Nothing -> throwIO $ ConnectException "Unknown connection"
      Just c  -> 
        if not $ P.connected c
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

  newSendQ :: Con -> P.Connection -> String -> String -> [Qopt] -> 
              (a -> IO B.ByteString)  -> IO (Queue a)
  newSendQ cid c qn dst os conv = 
    return SendQ {
              qCon  = cid,
              qDest = dst,
              qName = qn,
              qRec  = if hasOpt OWithReceipt os then True else False,
              qWait = if hasOpt OWaitReceipt os then True else False,
              qTo   = conv}

  newRecvQ :: Con -> P.Connection -> String -> String -> 
              [Qopt] -> [F.Header] ->
              (B.ByteString -> IO a)  -> IO (Queue a)
  newRecvQ cid c qn dst os hs conv = do
    let am = ackMode os
    sid <- mkUniqueSubId
    P.subscribe c (P.mkSub (show sid) dst am) "" hs
    ch <- newChan 
    addDest (dst,  ch)
    addSub  (sid, ch)
    return $ RecvQ {
               qCon  = cid,
               qSub  = sid,
               qDest = dst,
               qName = qn,
               qMode = am,
               qAuto = False,
               qFrom = conv}

  readQ :: Queue a -> IO (Either String (P.Message a))
  readQ q | typeOf q == SendQT = return $ Left "Read on a SendQ!"
          | otherwise = do
    mbC <- getCon (qCon q)
    case mbC of
      Nothing -> return $ Left "No connection"
      Just c  -> 
        if not $ P.connected c 
          then return $ Left "Not connected"
          else do
            mbCh <- getSub (qSub  q)
            case mbCh of
              Nothing -> return $ Left "Unknown queue"
              Just ch -> do
                eiM <- (readChan ch >>= frmToMsg q)
                case eiM of
                  Left  e -> return $ Left e
                  Right m -> return $ Right m

  writeQ :: Queue a -> String -> [F.Header] -> a -> IO ()
  writeQ q mime hs x | typeOf q == RecvQT = 
                         throwIO $ QueueException $
                           "Write with RecvQ (" ++ (qName q) ++ ")"
                     | otherwise = do
    mbC <- getCon (qCon q)
    case mbC of
      Nothing -> throwIO $ ConnectException $ 
                   "Unknown Connection in Queue (" ++ 
                   (show $ qName q) ++ ")"
      Just c  -> 
        if not $ P.connected c
          then throwIO $ ConnectException $
                 "Not connected (" ++ (show $ qCon q) ++ ")"
          else do
            let conv = qTo q
            s <- conv x
            let m = P.mkMessage "" mime (B.length s) "" s x
            P.send c (qDest q) m "" hs 

  frmToMsg :: Queue a -> F.Frame -> IO (Either String (P.Message a))
  frmToMsg q f = do
    let b = F.getBody f
    let conv = qFrom q
    eiX <- catch (Right <$> conv b)
                 (\e -> return $ Left $ show e)
    case eiX of
      Left  e -> return $ Left e
      Right x -> 
        return $ Right $ P.mkMessage (F.getId     f)
                                     (F.getMime   f)
                                     (F.getLength f)
                                     "" b x
    
  listen :: Con -> IO ()
  listen cid = forever $ do
    mbC <- getCon cid
    case mbC of
      Nothing -> do
        putStrLn $ "Connection " ++ (show cid) ++ " vanished!!!"
        return ()
      Just c  -> do
        eiF <- S.receive (P.getRc c) (P.getSock c) (P.conMax c)
        case eiF of
          Left e  -> do
            putStrLn $ "Error: " ++ e
            -- set con to not connected, return
            return ()
          Right f -> 
            case F.typeOf f of
              F.Message -> handleMessage cid c f
              F.Error   -> return ()
              F.Receipt -> return ()

  handleMessage :: Con -> P.Connection -> F.Frame -> IO ()
  handleMessage cid c f = do
    mbCh <- let qn  = F.getDest f
                sid = F.getSub  f
            in if null sid
              then getDest qn
              else do
                if null sid 
                  then return Nothing -- error handling
                  else if not $ numeric sid
                       then return Nothing -- error handling
                       else getSub (Sub $ read sid)
    case mbCh of
      Nothing -> do
        putStrLn "Unkown Queue"
        return () -- error handling
      Just ch -> 
        writeChan ch f

  numeric :: String -> Bool
  numeric = and . map isDigit
    
