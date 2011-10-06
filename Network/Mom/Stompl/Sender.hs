module Sender {- (
                            startSender,
                            registerCon, unRegisterCon,
                            bookFrame
                          ) -}
where
  
  import           Types  
  import           Config
  import           Logger
  import           Book
  import qualified Network.Mom.Stompl.Frame  as F

  import qualified Network.Socket            as S
  import qualified Network.Socket.ByteString as SB

  import           Control.Concurrent
  import           Control.Monad.State
  import           Control.Applicative ((<$>))

  import           Data.List  (insert, delete, find)
  import           Data.Maybe (catMaybes)
  import qualified Data.ByteString           as B

  type Sender = StateT Book IO 

  updMsgState :: Int -> String -> String -> SubId -> Sender ()
  updMsgState cid mid n sid = do
    b <- get
    case getQueue n b of
      Nothing -> logS WARNING $ "Queue " ++ n ++ " not found"
      Just q  -> do
        let q' = updMsgPnd cid mid sid q
        put $ addQueue q' $ remQueue q b 

  changeBook :: String -> Sender () 
  changeBook name = do
    logS DEBUG $ "Changing Broker name to '" ++ name ++ "'"
    b <- get
    let b' = b {bookLog = name ++ "." ++ "Sender",
                bookCfg = setName name $ bookCfg b}
    put b'

  insMsg :: String -> String -> Sender ()
  insMsg mid n = do
    b <- get
    put $ addBookMsg mid n b 

  insQueue :: String -> [SubId] -> Sender ()
  insQueue n subs = do
    b <- get
    put $ addQueue (mkQueue n subs) b 

  insSub :: Int -> String -> String -> F.AckMode -> Sender ()
  insSub cid ext n am = do
    b <- get
    let sub = mkSub cid ext n am
    let sid = getSubId sub
    put $ addSub sub b 

  insTx :: Transaction -> Sender ()
  insTx tx = do
    b <- get 
    put $ addTx tx b 

  delTx :: Transaction -> Sender ()
  delTx tx = do
    b <- get
    put $ remTx tx b 

  addToTx :: Int -> F.Frame -> Sender ()
  addToTx cid f = do
    b <- get
    case addTxFrame cid f b of
      Nothing -> logS INFO $ "Unknown Tx in Frame: " ++ (F.getTrans f) -- Error Frame!
      Just b' -> put b'

  delSubsOfCon :: Int -> Sender ()
  delSubsOfCon cid = do
    b <- get
    let ss = getSubsOfCon cid b
    mapM_ delSub ss

  delSub :: Subscription -> Sender ()
  delSub s = do
    b <- get
    put $ remSub s b

  delSubs :: String -> Sender ()
  delSubs sid = do
    b <- get
    put $ remSubs sid b 

  getSubMode :: String -> Sender F.AckMode
  getSubMode sid = do
    b <- get
    case getSub sid b of
      Nothing -> return F.Auto
      Just s  -> return $ getModeOfSub s

  updQueue :: Queue -> Sender ()
  updQueue q = do
    b <- get
    put $ addQueue q $ remQueue q b 

  updSub :: Subscription -> Sender ()
  updSub s = do
    b <- get
    put $ addSub s $ remSub s b 

  insCon :: Int -> S.Socket -> Sender ()
  insCon cid s = do
    b <- get
    put $ addCon (mkCon cid s) b
            
  updCon :: Connection -> Sender ()
  updCon c = do
    b <- get
    put $ addCon c $ remCon c b 

  delCon :: Connection -> Sender ()
  delCon c = do
    b <- get
    put $ remCon c b 

  removeCon :: Int -> Sender ()
  removeCon cid = do
    b <- get
    case getCon cid b of
      Nothing -> return ()
      Just c  -> do
        delCon c
        delSubsOfCon cid

  addMsg :: String -> MsgStore -> Connection -> String -> SendState -> Sender ()
  addMsg n m c sid st = do
    b <- get
    case getQueue n b of
      Nothing -> do
        logS INFO $ "Unknown Queue " ++ n ++
                    ". Discarding Message."
      Just q  -> do
        let q' = addMsgToQ m c sid st q
        put $ addQueue q' $ remQueue q b 

  getCount :: Sender Int
  getCount = get >>= \b -> do
    let (i, b') = countBook b
    put b'
    return i

  registerCon :: (SubMsg -> IO()) -> Int -> S.Socket -> IO ()
  registerCon wSnd cid s = 
    wSnd $ RegMsg cid s 

  unRegisterCon :: (SubMsg -> IO ()) -> Int -> IO ()
  unRegisterCon wSnd cid = 
    wSnd $ UnRegMsg cid 

  bookFrame :: (SubMsg -> IO ()) -> Int -> F.Frame -> IO ()
  bookFrame wSnd cid f = 
    wSnd $ FrameMsg cid f

  logS :: Priority -> String -> Sender ()
  logS p s = do
    b <- get
    liftIO $ logX (writeLog $ bookCfg b) (bookLog b) p s 

  logIO :: Book -> Priority -> String -> IO ()
  logIO b p s = do
    logX (writeLog $ bookCfg b) (bookLog b) p s 

  ------------------------------------------------------------------------
  -- Start and run Sender
  ------------------------------------------------------------------------
  startSender :: Config -> IO ()
  startSender c = do
    evalStateT runSender $ mkBook c

  runSender :: Sender ()
  runSender = do
    logS INFO "Sender up and running..."
    forever $ do
      m <- waitRequest
      logS DEBUG "request..."
      handleRequest m
      return ()

  ------------------------------------------------------------------------
  -- Handling requests
  ------------------------------------------------------------------------
  waitRequest :: Sender SubMsg
  waitRequest = do
    b <- get
    let brk = bookCfg b
    liftIO $ readSender brk

  handleRequest :: SubMsg -> Sender ()
  handleRequest msg = do
    case msg of
      FrameMsg cid f    -> handleFrame cid f False
      SockMsg  cid s st -> return ()
      RegMsg   cid s    -> insCon cid s
      UnRegMsg cid      -> removeCon cid 
      CfgSndMsg name    -> changeBook name

  handleFrame :: Int -> F.Frame -> Bool -> Sender ()
  handleFrame cid f tx = 
    case F.typeOf f of
      F.Connect     -> return () -- ignore
      F.Connected   -> return () -- ignore
      F.Message     -> return () -- forward
      F.Receipt     -> return () -- forward
      F.Disconnect  -> return () -- send receipt
      F.Subscribe   -> handleSub    cid f
      F.Unsubscribe -> handleUnSub  cid f
      F.Begin       -> handleBegin  cid f
      F.Commit      -> handleCommit cid f
      F.Abort       -> handleAbort  cid f
      F.Ack         -> handleAck    cid f tx
      F.Send        -> handleSend   cid f tx
      F.Error       -> handleError  cid f

  ------------------------------------------------------------------------
  -- Begin Frame
  ------------------------------------------------------------------------
  handleBegin :: Int -> F.Frame -> Sender ()
  handleBegin cid f = do
    let tid = F.getTrans f
    logS DEBUG $ "Handle Begin: " ++ tid
    insTx $ mkTx cid tid []

  ------------------------------------------------------------------------
  -- Commit Frame
  ------------------------------------------------------------------------
  handleCommit :: Int -> F.Frame -> Sender ()
  handleCommit cid f = do
    let tid = mkTxId cid $ F.getTrans f
    logS DEBUG $ "Handle Commit for Tx " ++ tid
    b <- get
    case getTx tid b of
      Nothing -> do
        logS INFO $ "Unknown Tx on commit: " ++ tid
        -- send error frame
      Just tx -> do
        logS DEBUG $ "Commiting " ++ tid
        mapM_ (\x -> handleFrame cid x True) $ reverse $ txFrames tx
        delTx tx

  ------------------------------------------------------------------------
  -- Abort Frame
  ------------------------------------------------------------------------
  handleAbort :: Int -> F.Frame -> Sender ()
  handleAbort cid f = do
    let tid = mkTxId cid $ F.getTrans f
    logS DEBUG $ "Handle Abort for Tx " ++ tid
    b <- get
    case getTx tid b of
      Nothing -> do
        logS INFO $ "Unknown Tx on abort: " ++ tid
        -- send error frame
      Just tx -> do
        logS DEBUG $ "Aborting " ++ tid
        delTx tx

  ------------------------------------------------------------------------
  -- Ack Frame
  ------------------------------------------------------------------------
  handleAck :: Int -> F.Frame -> Bool -> Sender ()
  handleAck cid f tx = do
    if tx || (null $ F.getTrans f) 
      then doAck cid f
      else addToTx cid f

  doAck :: Int -> F.Frame -> Sender ()
  doAck cid f = do
    logS DEBUG "Handle Ack"
    let mid = F.getId f
    b <- get
    case getQofMsg mid b of
      Nothing -> logS INFO $ "Unknown message " ++ mid
      Just n  -> do
        let sid = mkSubId cid (F.getSub f) n
        am <- getSubMode sid
        case am of
          F.Client     -> updMsgOlder b   mid n sid
          F.ClientIndi -> updMsgState cid mid n sid
          _            -> liftIO $ putStrLn "AckMode Auto!" -- return ()
     where updMsgOlder b mid n sid = 
              case getQueue n b of
                Nothing -> return ()
                Just q  -> do
                  let ms  = dropWhile (\x -> getMsgId x /= mid) $ qMsgs q
                  let ms' = msgFilter cid sid Sent ms
                  mapM_ (\x -> updMsgState cid (getMsgId x) n sid) ms' 

  ------------------------------------------------------------------------
  -- Subscribe Frame
  ------------------------------------------------------------------------
  -- should we ensure unique subscriptions per
  -- cid / queue or
  -- cid / id / queue ?
  handleSub :: Int -> F.Frame -> Sender ()
  handleSub cid f = do
    logS DEBUG "Handle Subscription"
    b <- get
    case getCon cid b of
      Nothing -> return () -- error & disconnect
      Just _  -> do
        let q  = F.getDest f
        insSub cid (F.getId f) (F.getDest f) (F.getAcknow f)
        -- logS DEBUG $ show $ bookQs     b
        -- logS DEBUG $ show $ bookSubs   b
        -- logS DEBUG $ show $ bookCons   b

  ------------------------------------------------------------------------
  -- Unsubscribe Frame
  ------------------------------------------------------------------------
  handleUnSub :: Int -> F.Frame -> Sender ()
  handleUnSub cid f = do
    logS DEBUG "Handle Unsubscribe"
    b <- get
    case getCon cid b of
      Nothing -> return ()
      Just _  -> do
        let i = F.getId   f
        let subSearch = if null i 
                          then getSub
                               (mkSubId cid (F.getId f) (F.getDest f))
                          else getSubById cid i 
        case subSearch b of
          Nothing -> do
            logS WARNING $ "Subscription " ++ (mkSubId cid (F.getId f) (F.getDest f)) ++ " not found!"
            return ()
          Just s  -> do
            delSub s
            -- logS DEBUG $ show $ bookSubs b
            -- logS DEBUG $ show $ bookCons b

  ------------------------------------------------------------------------
  -- Send Frame
  ------------------------------------------------------------------------
  -- send receipt
  handleSend :: Int -> F.Frame -> Bool -> Sender ()
  handleSend cid f tx = do
    logS DEBUG ("Sending " ++ (show $ F.typeOf f) ++ " Frame")
    if tx || (null $ F.getTrans f) 
      then doSend  cid f
      else addToTx cid f

  doSend :: Int -> F.Frame -> Sender ()
  doSend cid f = do
    b <- get
    case getCon cid b of
      Nothing -> return ()
      Just _  -> do
        let n    = F.getDest f
        let subs = getSubscribers n b
        let cc   = dropNothing $ map (\s -> (getCon (getSubConId s) b, Just s)) subs
        mid <- show <$> getCount
        case F.sndToMsg mid f of
          Nothing -> logS ERROR "Can't convert Send to Message"
          Just m  -> do
            insMsg mid n 
            mapM_ (\c-> sendFrame n c (F.getTrans f) m) cc

  ------------------------------------------------------------------------
  -- Error Frame
  ------------------------------------------------------------------------
  handleError :: Int -> F.Frame -> Sender ()
  handleError cid f = do
    b <- get
    case getCon cid b of
      Nothing -> return ()
      Just c  -> sendFrame "" (c, Nothing) "" f

  ------------------------------------------------------------------------
  -- Actually send a Frame
  ------------------------------------------------------------------------
  sendFrame :: String -> (Connection, Maybe Subscription) -> String -> F.Frame -> Sender ()
  sendFrame n (c, mbS) trn f = do
    let m  = F.putFrame f
    let sid = case mbS of
                Nothing -> ""
                Just x  -> getSubId x
    if (not $ sockUp   c) ||
       (not $ pndOnCon c) 
      then addMsgx n m sid Pending
      else do
        let s = getSock c
        b <- get
        l <- liftIO $ catch (SB.send s m)
                            (onError b s)
        if l /= B.length m
          then do
            logS ERROR "Can't send Frame"
            updCon $ updSockState Down c 
            addMsgx n m sid Pending
          else case mbS of
                 Nothing -> return ()
                 Just x  -> 
                   if getModeOfSub x /= F.Auto
                     then addMsgx n m sid Sent
                     else return () 
    where onError b s e = do
            logIO b WARNING (show e)
            return 0
          addMsgx nm m sid st = do
            let ms = mkStore m f [(getConId c, st)]
            if null nm
              then updCon $ addPndToCon ms c 
              else addMsg nm ms c sid st
