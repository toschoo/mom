{-# OPTIONS -fglasgow-exts -fno-cse #-}
module Network.Mom.Stompl.Client.Queue (
                   Queue, Qopt(..), Converter(..),
                   withConnection, 
                   newQueue, readQ, writeQ)
where

  import qualified Socket   as S
  import qualified Protocol as P
  import           Factory  

  import qualified Network.Mom.Stompl.Frame as F

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U

  import           Control.Concurrent 
  import           Control.Applicative ((<$>))
  import           Control.Monad
  import           Control.Exception hiding (catch)
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

  data QueueExc = WriteOnRecvExc | ConvertExc String | ConnectExc | SendExc String
    deriving (Show, Typeable)

  instance Exception QueueExc

  withConnection :: String -> Int -> Int -> String -> String -> F.Heart -> 
                    (Con -> IO (Either String a)) -> IO (Either String a)
  withConnection host port mx usr pwd beat act = do
    eiCid <- catch (Right <$> mkUniqueConId)
                   (\e -> return $ Left $ show e)
    case eiCid of
      Left  e   -> return $ Left e
      Right cid -> do
        c <- P.connect host port mx usr pwd vers beat
        if not $ P.connected c 
          then return $ Left $ P.getErr c
          else do
            addCon (cid, c) -- catch!
            eiT <- catch (Right <$> (forkIO $ listen cid))
                         (\e -> return $ Left $ show e)
            r <- case eiT of
                   Left e  -> return $ Left e
                   Right _ -> catch (act cid)
                              (\e -> return $ Left $ show e)
            c' <- P.disconnect c ""
            rmCon (cid, c)
            return r
            -- sub and dest?
            -- all subs and dests have to be added to Connection
            -- and removed on rmCon
            -- By the way: Dest needs a Connection-Prefix to
            --             distinguish qs that by chance have
            --             the same name in different connections!

  data Queue a = SendQ {
                   qCon  :: Con,
                   qDest :: String,
                   qRec  :: Bool,
                   qWait :: Bool,
                   qTo   :: a -> IO (Either String B.ByteString)}
               | RecvQ {
                   qCon  :: Con,
                   qSub  :: Sub,
                   qDest :: String,
                   qMode :: F.AckMode,
                   qAuto :: Bool, -- library creates Ack
                   qFrom :: B.ByteString -> IO (Either String a)}

  data QType = SendQT | RecvQT
    deriving (Eq)

  typeOf :: Queue a -> QType
  typeOf (SendQ _ _ _ _ _  ) = SendQT
  typeOf (RecvQ _ _ _ _ _ _) = RecvQT

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

  data Converter a = OutBound (a -> IO (Either String B.ByteString))
                    | InBound (B.ByteString -> IO (Either String a))

  newQueue :: Con -> String -> [Qopt] -> [F.Header] -> 
              Converter a -> IO (Either String (Queue a))
  newQueue cid qn os hs conv = do
    mbC <- getCon cid
    case mbC of
      Nothing -> return $ Left $ "Unknown connection"
      Just c  -> 
        if not $ P.connected c
          then return $ Left "Not connected"
          else 
            if hasOpt OSend os 
              then 
                case conv of
                  (OutBound f) -> newSendQ cid c qn os f
                  _            -> return $ Left $ 
                                       "InBound Converter for SendQ"
              else 
                if hasOpt OReceive os
                  then 
                    case conv of
                      (InBound f) -> newRecvQ cid c qn os hs f
                      _           -> return $ Left $ 
                                       "OutBound Converter for RecvQ"
                  else return $ Left "No direction indicated"

  newSendQ :: Con -> P.Connection -> String -> [Qopt] -> 
              (a -> IO (Either String B.ByteString))  -> 
              IO (Either String (Queue a))
  newSendQ cid c qn os conv = 
    return $ Right SendQ {
                    qCon  = cid,
                    qDest = qn,
                    qRec  = if hasOpt OWithReceipt os then True else False,
                    qWait = if hasOpt OWaitReceipt os then True else False,
                    qTo   = conv}

  newRecvQ :: Con -> P.Connection -> String -> [Qopt] -> [F.Header] ->
              (B.ByteString -> IO (Either String a))  -> 
              IO (Either String (Queue a))
  newRecvQ cid c qn os hs conv = do
    let am = ackMode os
    sid <- mkUniqueSubId
    c'  <- P.subscribe c (P.mkSub (show sid) qn am) "" hs
    if not $ P.ok c'
      then return $ Left $ P.getErr c'
      else do
        ch <- newChan 
        addDest (qn,  ch)
        addSub  (sid, ch)
        return $ Right $ RecvQ {
                           qCon  = cid,
                           qSub  = sid,
                           qDest = qn,
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
  writeQ q mime hs x | typeOf q == RecvQT = throw WriteOnRecvExc
                     | otherwise = do
    mbC <- getCon (qCon q)
    case mbC of
      Nothing -> throw ConnectExc 
      Just c  -> 
        if not $ P.connected c
          then throw ConnectExc
          else do
            let conv = qTo q
            eiS <- catch (conv x)
                         (\e -> return $ Left $ show e)
            case eiS of
              Left  e  -> throw $ ConvertExc e
              Right s -> do
                let m = P.mkMessage "" mime (B.length s) "" s x
                c' <- P.send c (qDest q) m "" hs
                if not $ P.ok c' 
                  then throw $ SendExc $ P.getErr c'
                  else return ()

  frmToMsg :: Queue a -> F.Frame -> IO (Either String (P.Message a))
  frmToMsg q f = do
    let b = F.getBody f
    let conv = qFrom q
    eiX <- catch (conv b)
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
    
