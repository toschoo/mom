{-# LANGUAGE ConstraintKinds #-}
module Main
where

  import           Common
  import           System.Exit
  import           System.Timeout
  import qualified System.ZMQ as Z
  import           System.IO (hFlush, stdout)
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic hiding (stop)
  import qualified Data.ByteString.Char8 as B
  import           Data.Time.Clock
  import           Data.List (sort, nub)
  import           Data.Maybe (fromJust)
  import qualified Data.Conduit as C
  import qualified Data.Conduit.List as CL
  import           Data.Conduit (($$), ($=), (=$))    
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Monad (when, unless)
  import           Control.Monad.Trans (liftIO)
  import           Control.Exception (try, throwIO, SomeException)

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
  import           Network.Mom.Patterns.Broker.Common
  import           Network.Mom.Patterns.Broker.Client
  import           Network.Mom.Patterns.Broker.Server 
  import           Network.Mom.Patterns.Broker.Broker

  import           Heartbeat

  clsock, srvsock :: String
  clsock = "inproc://clients"
  srvsock = "inproc://servers"

  osock1, osock2, osock3, osock4, osock5, osock6 :: String
  osock1 = "inproc://_out1"
  osock2 = "inproc://_out2"
  osock3 = "inproc://_out3"
  osock4 = "inproc://_out4"
  osock5 = "inproc://_out5"
  osock6 = "inproc://_out6"

  prpCReq :: NonEmptyList String -> Property
  prpCReq (NonEmpty s) = monadicIO $ do
    r <- run $ C.runResourceT $ streamList (map B.pack s) $= 
                                mdpCSndReq "Test"         $=
                                sndId                     $$ rcv
    assert (r == s)
    where rcv = do
            _ <- mdpCRcvReq
            map B.unpack <$> CL.consume

  prpCRep :: NonEmptyList String -> Property
  prpCRep (NonEmpty s) = monadicIO $ do
    r <- (map B.unpack) <$> run (
           C.runResourceT $ streamList (map B.pack s) $= 
                            mdpCSndRep (B.pack "Test")
                                       [B.pack "xxx"] $$
                            ignoreId          =$
                            mdpCRcvRep "Test" =$ CL.consume)
    assert (r == s)

  ignoreId :: Conduit B.ByteString ()
  ignoreId = C.await >>= \_ -> passThrough

  sndId :: Conduit B.ByteString ()
  sndId = C.yield (B.pack "xxx") >> passThrough

  prpWReq :: NonEmptyList String -> Property
  prpWReq (NonEmpty s) = monadicIO $ do
    r <- run $ C.runResourceT $ streamList (map B.pack s) $= 
                                mdpWSndReq (B.pack "xxx")
                                           [B.pack "zzz"] $=
                                ignoreId                  $$ rcv
    assert (r == s)
    where rcv = do
            _ <- mdpWRcvReq
            map B.unpack <$> CL.consume

  prpWRep :: NonEmptyList String -> Property
  prpWRep (NonEmpty s) = monadicIO $ do
    r <- run $ C.runResourceT $ streamList (map B.pack s) $= 
                                mdpWSndRep [B.pack "xxx"] $=
                                sndId                     $$ rcv
    assert (r == s)
    where rcv = do
            _ <- mdpWRcvRep
            map B.unpack <$> CL.consume

  prpMMIReq :: NonEmptyList Char -> Property
  prpMMIReq (NonEmpty s) = monadicIO $ do
    r <- run $ C.runResourceT $ streamList [B.pack s] $= 
                                mdpCSndReq (B.unpack $ mmiHdr `B.append` mmiSrv) $=
                                sndId        $$ rcv
    assert (r == s)
    where rcv = do
            _ <- mdpCRcvReq
            (concat . map B.unpack) <$> CL.consume

  prpMMIRep :: NonEmptyList Char -> Property
  prpMMIRep (NonEmpty s) = monadicIO $ do
    [r] <- (map B.unpack) <$> run (
             C.runResourceT $ streamList [B.pack s] $= 
                            mdpCSndRep (mmiHdr `B.append` mmiSrv)
                                       [B.pack "xxx"] $$
                            ignoreId   =$
                            mdpCRcvRep (B.unpack $ mmiHdr `B.append` mmiSrv)
                                       =$ CL.consume)
    assert (r == s)

  prpMdpBeat :: Property
  prpMdpBeat = monadicIO $ do
    f <- run (C.runResourceT $ streamList [] $= mdpWBeat $= sndId $$ mdpWRcvRep)
    case f of
      WBeat _ -> assert True
      _       -> assert False

  prpConnect :: NonEmptyList Char -> Property
  prpConnect (NonEmpty s) = monadicIO $ do
    f <- run (C.runResourceT $ mdpWConnect s $= sndId $$ mdpWRcvRep)
    case f of
      WReady _  sn -> assert (s == B.unpack sn)
      _            -> assert False

  prpDisConnect :: Property
  prpDisConnect = monadicIO $ do
    f <- run (C.runResourceT $ mdpWDisconnect $= sndId $$ mdpWRcvRep)
    case f of
      WDisc _  -> assert True
      _        -> assert False

  prpReady :: NonEmptyList Char -> Property
  prpReady (NonEmpty s) = testContext (Just s) $ \ctx ->
    try $ Z.withSocket ctx Z.XRep $ \b -> do
      Z.bind b srvsock
      withServer ctx s srvsock Connect 
                     onTmo (showErr "Server") bounce $ \_ -> do
        rs <- recvAll b
        f  <- C.runResourceT $ streamList rs $$ mdpWRcvRep
        case f of
          WReady _ sn -> return $ Just (B.unpack sn)
          _           -> return Nothing

  prpBrkDisc :: Property
  prpBrkDisc = testContext (True) $ \ctx ->
    try $ Z.withSocket ctx Z.XRep $ \b -> do
      Z.bind b srvsock
      exc <- newEmptyMVar
      withServer ctx "Test" srvsock Connect 
                      onTmo (pubErr exc) bounce $ \_ -> do
        rs <- recvAll b
        f  <- C.runResourceT $ streamList rs $$ mdpWRcvRep
        case f of
          WReady i _ -> do
            C.runResourceT $ mdpWBrkDisc i $$ sndSock b
            mbE <- timeout 10000 $ takeMVar exc
            case mbE of
              Nothing -> return False
              Just e  -> return (e == "ProtocolExc \"Broker disconnects\"") -- not very elegant
          _          -> throwIO $ ProtocolExc "Server not ready!"

  prpSrvDisc :: Property
  prpSrvDisc = testContext (True) $ \ctx ->
    try $ Z.withSocket ctx Z.XRep $ \b -> do
      Z.bind b srvsock
      ok <- withServer ctx "Test" srvsock Connect 
                       onTmo (showErr "Server") bounce $ \_ -> do
        rs <- recvAll b
        f  <- C.runResourceT $ streamList rs $$ mdpWRcvRep
        case f of
          WReady _ _ -> return True
          _          -> return False
      if ok then do rs <- recvAll b
                    f  <- C.runResourceT $ streamList rs $$ mdpWRcvRep
                    case f of
                      WDisc _ -> return True
                      _       -> return False
            else return False

  prpCheckSrv:: NonEmptyList Char -> Property
  prpCheckSrv (NonEmpty s) = testContext (Just False) $ \ctx -> 
    try $ testBroker ctx $ \_ -> 
      withClient ctx s clsock Connect $ \c -> do
          mbX <- checkService  c 10000
          case mbX of
            Nothing -> return Nothing
            Just x  -> return $ Just x
     
  prpPassOne :: NonEmptyList Char -> Property
  prpPassOne (NonEmpty s) = testContext (Just s) $ \ctx -> 
    try $ testBroker ctx $ \_ -> 
      withClient ctx "TestService" clsock Connect $ \c ->
        withServer ctx "TestService" srvsock Connect 
                        onTmo (showErr "Server") bounce $ \_ -> do
          waitForWorker c
          request c (-1) (C.yield $ B.pack s)
                         (do mbX <- C.await
                             case mbX of
                               Nothing -> return Nothing
                               Just x  -> return $ Just $ B.unpack x)

  prpPassAll :: NonEmptyList String -> Property
  prpPassAll (NonEmpty s) = testContext (Just s) $ \ctx ->
    try $ testBroker ctx $ \_ -> 
      withClient ctx "TestService" clsock Connect $ \c -> 
        withServer ctx "TestService" srvsock Connect 
                   onTmo (showErr "Server") bounce $ \_ -> do
          waitForWorker c
          request c (-1) (streamList $ map B.pack s)
                         (Just . (map B.unpack) <$> CL.consume)

  prpBeat1 :: Property
  prpBeat1 = testContext (True) $ \ctx ->
    try $ testBroker ctx $ \_ ->
      Z.withSocket ctx Z.XReq $ \srv -> do
        Z.connect srv srvsock
        C.runResourceT $ mdpWConnect "Test" $$ sndSock srv
        putStr "Beat" >> hFlush stdout
        beat srv (10::Int)
    where beat srv i | i <= 0    = putStrLn "" >> return True
                     | otherwise = do
            once <- getCurrentTime
            mbB  <- timeout 1000000 $ recvAll srv
            putStr "." >> hFlush stdout
            case mbB of
              Nothing -> return False
              Just bs -> do
                f  <- C.runResourceT $ streamList bs $= sndId $$ mdpWRcvRep
                case f of
                  WBeat _ -> do
                    now <- getCurrentTime
                    if now > timeAdd once 1000
                      then return False
                      else do
                        C.runResourceT $ streamList [] $= mdpWBeat $$ sndSock srv
                        beat srv (i-1)
                  _         -> return False

  prpBeatN :: Property
  prpBeatN = do
    let r = (10::Int)
    n <- choose (10,50)::(Gen Int)
    testContext (True) $ \ctx ->
      try $ testBroker ctx $ \_ -> 
        startServers ctx n [] $ \pp -> do
          v  <- newMVar []
          putStrLn $ "Rounds: " ++ show (r*n)
          poll v (r*n) pp handle
          putStrLn ""
          withMVar v $ \us -> return $ evaluate n us
    where handle v n (Z.S s Z.In) f = 
            case f of
              WBeat _ -> do
                C.runResourceT $ streamList [] $= mdpWBeat $$ sndSock s
                now <- getCurrentTime
                modifyMVar_ v $ \l -> return $ (n, now):l
              _ -> throwIO $ ProtocolExc "unexpected frame"
          handle _ _ _ _ = return ()

  prpRoundRobin :: Property
  prpRoundRobin = do
    -- n <- choose (10,50)::(Gen Int) -- very slow
    let n = 10 
    testContext [1..n] $ \ctx ->
      try $ testBroker ctx $ \_ ->
        startServers2 ctx n $ do
          withClient ctx "RR" clsock Connect $ \c -> do
            threadDelay 10000 -- we have to wait for *all* servers
            go c n []
    where go :: Client -> Int -> [Int] -> IO [Int]
          go c n rr | n <= 0    = return (sort rr)
                    | otherwise = do
             r <- (B.unpack . head . fromJust) <$> request c (-1) 
                        (C.yield $ B.pack "") (Just <$> getSrv)
             go c (n-1) (read r:rr)
          getSrv = do
            mbX <- C.await
            case mbX of
              Nothing -> return []
              Just x  -> return [x]
          

  startServers :: Context -> Int -> [Z.Poll] -> 
                  ([Z.Poll] -> IO r)         -> IO r
  startServers ctx n ss job | n <= 0    = job ss
                            | otherwise = 
            Z.withSocket ctx Z.XReq $ \s -> do
              Z.connect s srvsock
              C.runResourceT $ mdpWConnect "Test" $$ sndSock s
              startServers ctx (n-1) ((Z.S s Z.In):ss) job

  startServers2 :: Context -> Int -> IO r -> IO r
  startServers2 ctx n job | n <= 0    = job 
                          | otherwise = 
    withServer ctx "RR" srvsock Connect 
               onTmo (showErr ("Server " ++ show n)) 
                     (reply n) $ \_ -> startServers2 ctx (n-1) job
    where reply x _ = do
            _ <- C.await 
            C.yield (B.pack $ show x)

  handleBrk :: MVar r -> Int -> [Z.Poll] -> 
               (MVar r -> Int -> Z.Poll -> WFrame -> IO ()) -> IO ()
  handleBrk _ _ [] _ = return ()
  handleBrk v n ((Z.S s Z.In):ss) hndl = do
            x <- recvAll s
            f <- C.runResourceT $ streamList x $= sndId $$ mdpWRcvRep
            hndl v n (Z.S s Z.In) f
            handleBrk v (n+1) ss hndl
  handleBrk v n (_:ss) hndl  = handleBrk v (n+1) ss hndl

  poll ::  MVar r -> Int -> [Z.Poll] -> 
          (MVar r -> Int -> Z.Poll   -> WFrame -> IO ()) -> IO ()
  poll v n pp hndl | n <= 0    = return ()
                   | otherwise = do
    putStr "." >> hFlush stdout
    rr <- Z.poll pp 1000000
    handleBrk v 1 rr hndl
    poll  v (n-1) pp hndl
          
  getKeys :: [(Int, b)] -> [Int]
  getKeys = nub . map fst

  extract :: Int -> [(Int, b)] -> [b]
  extract k xs = [snd x | x <- xs, fst x == k]

  partitionBeats :: [(Int, b)] -> [Int] -> [[b]]
  partitionBeats bs = map (\k -> extract k bs) 

  evaluate :: Int -> [(Int, UTCTime)] -> Bool
  evaluate i us = 
    let ks = getKeys us
     in if length ks == i 
          then analyseResults $ partitionBeats us ks
          else False

  analyseResults :: [[UTCTime]] -> Bool
  analyseResults us = and (map analyseResult us)
    where analyseResult []  = True
          analyseResult [_] = True
          analyseResult (x1:x2:xs) =
            if x1 > timeAdd x2 1000 then False
                                    else analyseResult (x2:xs)
  
  sndSock :: Z.Socket o -> Sink
  sndSock s = do
    mbX <- C.await
    case mbX of
      Nothing -> return ()
      Just x  -> go x
    where go x = do
            mbN <- C.await
            case mbN of
              Nothing -> liftIO (Z.send s x [])
              Just n  -> liftIO (Z.send s x [Z.SndMore]) >> go n

  testWith :: Eq a => a	-> (IO (Either SomeException a)) -> Property
  testWith ss action = monadicIO $ do
    ei <- run action
    case ei of
      Left  e -> run (print e) >> assert False
      Right x -> assert (x == ss)

  testBroker :: Context -> (Controller -> IO r) -> IO r
  testBroker ctx = withBroker ctx "Test" (10000) clsock srvsock (showErr "Broker")

  igerr :: OnError_
  igerr _ _ _ = return ()

  showErr :: String -> OnError_
  showErr s _ e m = putStrLn $ m ++ " in " ++ s ++ ": " ++  show e

  raise :: String -> OnError_
  raise s _ e m = do putStrLn $ m ++ " in " ++ s ++ ": " ++  show e
                     throwIO e

  pubErr :: MVar String -> OnError_
  pubErr v _ e _ = putMVar v (show e)

  onTmo :: StreamAction
  onTmo _ = return ()

  bounce :: StreamConduit
  bounce _ = passThrough 

  waitForWorker :: Client -> IO ()
  waitForWorker c = do
    x <- checkService c 5000 -- this timeout is crucial
    unless (isTrue x) $ waitForWorker c
    where isTrue Nothing  = False
          isTrue (Just x) = x

  waitForWorkerStop :: Client -> IO ()
  waitForWorkerStop c = do
    x <- checkService c 10000
    when (isTrue x) $ waitForWorker c
    where isTrue Nothing  = False
          isTrue (Just x) = x
  
  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    putStrLn "========================================="
    putStrLn "       Patterns Library Test Suite"
    putStrLn "                 MDP Broker"
    putStrLn "========================================="
    r <- runTest "Client Request"
                    (deepCheck prpCReq)       ?>
         runTest "Client Reply"
                    (deepCheck prpCRep)       ?>
         runTest "Server Request"
                    (deepCheck prpWReq)       ?>
         runTest "Server Reply "
                    (deepCheck prpWRep)       ?>
         runTest "MMI Request"
                    (deepCheck prpMMIReq)     ?>
         runTest "MMI Reply"
                    (deepCheck prpMMIRep)     ?>
         runTest "HeartBeat"
                    (deepCheck prpMdpBeat)    ?>
         runTest "Connect"
                    (deepCheck prpConnect)    ?> 
         runTest "DisConnect"
                    (deepCheck prpDisConnect) ?> 
         runTest "Server connects"
                    (deepCheck prpReady)      ?> 
         runTest "Broker disconnects"
                    (deepCheck prpBrkDisc)    ?> 
         runTest "Server disconnects"
                    (deepCheck prpSrvDisc)    ?> 
         runTest "One Server beats"
                    (oneCheck prpBeat1)       ?> 
         runTest "n servers beat"
                    (oneCheck prpBeatN)       ?> 
         runTest "Check Service"
                    (deepCheck prpCheckSrv)   ?> 
         runTest "Pass one passes one"
                    (deepCheck prpPassOne)    ?>   
         runTest "Pass all"
                    (deepCheck prpPassAll)    ?>
         runTest "Round Robin"
                    (deepCheck prpRoundRobin)
         
    case r of
      Success {} -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
