module Main
where

  import           Common
  import           System.Exit
  import qualified System.ZMQ as Z
  import           Test.QuickCheck
  import qualified Data.ByteString.Char8 as B
  import           Data.Time.Clock
  import           Data.Maybe (fromJust)
  import qualified Data.Conduit.List as CL
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Monad.Trans (liftIO)
  import           Control.Exception (try, throwIO)

  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams
  import qualified Network.Mom.Patterns.Streams as S (stop)

  import           Network.Mom.Patterns.Basic
  import           Heartbeat


  isock,osock :: String
  isock = "inproc://_in"
  osock = "inproc://_out"
  osock1, osock2, osock3, osock4, osock5, osock6 :: String
  osock1 = "inproc://_out1"
  osock2 = "inproc://_out2"
  osock3 = "inproc://_out3"
  osock4 = "inproc://_out4"
  osock5 = "inproc://_out5"
  osock6 = "inproc://_out6"

  prpClReqAtOnce :: NonEmptyList String -> Property
  prpClReqAtOnce (NonEmpty ss) = testContext (Just ss) $ \ctx -> do
      socktask ctx
      try $ withClient ctx "Test" isock Connect $ \c -> 
        request c (-1) (streamList (map B.pack ss)) 
                       ((Just . map B.unpack) <$> CL.consume)

  {-
  prpClCheckReq :: NonEmptyList String -> Property
  prpClCheckReq (NonEmpty ss) = testContext (Just ss) $ \ctx -> do
      socktask ctx
      try $ withClient ctx "Test" isock Connect $ \c -> do
        _ <- request c 0 (streamList (map B.pack ss)) 
                         ((Just . map B.unpack) <$> CL.consume)
        checkReceive c (-1) ((Just . map B.unpack) <$> CL.consume)

  prpClAsync :: NonEmptyList String -> Property
  prpClAsync (NonEmpty ss) = 
      testContext (Just ss) $ \ctx -> do
        socktask ctx
        try $ withClient ctx "Test" isock Connect $ \c -> req c ss
    where req c xs = do 
            _ <- request c 0 (streamList $ map B.pack xs)
                             (Just <$> CL.consume)
            checkReceive c (-1) ((Just . map B.unpack) <$> CL.consume)
  -}

  prpClTmo :: Property
  prpClTmo = testContext True $ \ctx -> 
      try $ withClient ctx "Test" isock Bind $ \c  -> do
        deafserver ctx
        let tmo = 1::Msec
        let usec = fromIntegral (tmo * 1000)
        before <- getCurrentTime
        _ <- request c usec (streamList [B.empty]) 
                            ((Just . map B.unpack) <$> CL.consume)
        after <- getCurrentTime
        return (after >= timeAdd before tmo &&
                after <  timeAdd before (10*tmo)) 
    where deafserver ctx = 
            forkIO go >>= \_ -> return ()
            where go = Z.withSocket ctx Z.Rep $ \s -> do
                         Z.connect s isock
                         _ <- recvAll s 
                         threadDelay 1000

  prpSvStop :: Property
  prpSvStop = testContext True $ \ctx -> 
      try $ withServer ctx "TestServer" isock Bind 
            (showErr "Server") bounce $ \s -> do
        S.stop s
        return True

  prpSvRepAtOnce :: NonEmptyList String -> Property
  prpSvRepAtOnce (NonEmpty ss) = testContext (Just ss) $ \ctx -> 
      try $ withServer ctx "TestServer" isock Bind 
            (showErr "Server") bounce $ \_ ->
        withClient ctx "Test Client" isock Connect $ \c -> 
          request c (-1) (streamList (map B.pack ss)) 
                         ((Just . map B.unpack) <$> CL.consume)

  prpQueue :: NonEmptyList String -> Property
  prpQueue (NonEmpty ss) = testContext (Just ss) $ \ctx -> 
      try $ withQueue ctx "test" (isock, Bind)
                                 (osock, Bind)
                                 (raise "Queue") $ \_ ->
        withServer ctx "TestServer" osock Connect
                   (showErr "Server") bounce $ \_ ->
          withClient ctx "Test Client" isock Connect $ \c -> 
            request c (-1) (streamList (map B.pack ss)) 
                           ((Just . map B.unpack) <$> CL.consume)

  prpPubIssue :: NonEmptyList String -> Property
  prpPubIssue (NonEmpty ss) = testContext ss $ \ctx -> 
      try $ withPub ctx isock Bind $ \p -> 
        Z.withSocket ctx Z.Sub $ \s -> do
          Z.connect s isock
          Z.subscribe s "test"
          issue p ["test"] (streamList (map B.pack ss))
          (drop 1 . map B.unpack) <$> recvAll s

  prpForwarder :: NonEmptyList String -> Property
  prpForwarder (NonEmpty ss) = testContext ss $ \ctx -> 
      try $ withForwarder ctx "test" ["test"] 
                          (osock, Bind) 
                          (isock, Bind) 
                          (raise "Forwarder") $ \_ -> 
        withPub ctx isock Connect $ \p ->
          Z.withSocket ctx Z.Sub  $ \s -> do
            Z.connect s osock
            Z.subscribe s "test"
            issue p ["test"] (streamList (map B.pack ss))
            (drop 1 . map B.unpack) <$> recvAll s

  prpSubIssue :: NonEmptyList String -> Property
  prpSubIssue (NonEmpty ss) = testContext (Just ss) $ \ctx -> 
      try $ withPub ctx isock Bind $ \p -> 
        withSub ctx isock Connect  $ \s -> do
          subscribe s ["test"]
          issue p ["test"] (streamList (map B.pack ss))
          checkSub s (-1) ((Just . map B.unpack) <$> CL.consume)

  prpSubPartial :: NonEmptyList String -> Property
  prpSubPartial (NonEmpty ss) = 
      let xs = zipF ss
          ys = zipS ss
       in testContext xs $ \ctx -> 
          try $ withPub ctx isock Bind $ \p ->
            withSub ctx isock Connect  $ \s -> do
              subscribe s ["first"]
              goF   p xs ys
              goSub s xs []
    where goF _  [] _  = return ()
          goF p xs ys  = issue p ["first"] (streamList [B.pack $ head xs]) >>
                          goS p (tail xs) ys
          goS _  _ []  = return ()
          goS p xs ys  = issue p ["scond"] (streamList [B.pack $ head ys]) >>
                          goF p xs (tail ys)
          goSub _ [] rs = return rs
          goSub s xs rs = do
              r <- checkSub s 1000 (Just <$> CL.consume)
              goSub s (tail xs) (rs ++ map B.unpack (fromJust r))

  prpSubMultiPartial :: NonEmptyList String -> Property
  prpSubMultiPartial (NonEmpty ss) = 
      let xs = zipF ss
          ys = zipS ss
       in testContext xs $ \ctx -> 
          try $ withPub ctx isock Bind $ \p ->
            withSub ctx isock Connect  $ \s -> do
              subscribe s ["first", "third"]
              goF   p xs ys
              goSub s xs []
    where goF _  [] _  = return ()
          goF p [x] _  = issue p ["third"] (streamList [B.pack x])
          goF p (x:y:xs) ys  = do
            issue p ["first"] (streamList [B.pack x]) 
            issue p ["third"] (streamList [B.pack y]) 
            goS p xs ys
          goS _  _ []  = return ()
          goS p xs ys  = issue p ["scond"] (streamList [B.pack $ head ys]) >>
                          goF p xs (tail ys)
          goSub _ [] rs = return rs
          goSub s xs rs = do
              r <- checkSub s 10000 (Just <$> CL.consume)
              goSub s (tail xs) (rs ++ map B.unpack (fromJust r))

  prpPush :: NonEmptyList String -> Property
  prpPush (NonEmpty ss) = testContext ss $ \ctx -> 
      try $ withPusher ctx isock Bind $ \p -> 
        Z.withSocket ctx Z.Pull $ \s -> do
          Z.connect s isock
          push p (streamList (map B.pack ss))
          map B.unpack <$> recvAll s

  prpPipeline :: NonEmptyList String -> Property
  prpPipeline (NonEmpty ss) = testContext ss $ \ctx -> 
      try $ withPipe ctx "test"
                          (osock, Bind) 
                          (isock, Bind) 
                          (raise "Pipeline") $ \_ -> 
        withPusher ctx isock Connect $ \p ->
          Z.withSocket ctx Z.Pull $ \s -> do
            Z.connect s osock
            push p (streamList (map B.pack ss))
            map B.unpack <$> recvAll s

  prpPull :: NonEmptyList String -> Property
  prpPull (NonEmpty ss) = testContext ss $ \ctx -> 
      try $ withPusher ctx isock Bind    $ \s -> do
        m <- newEmptyMVar
        withPuller ctx "Test" isock Connect  
                       (raise "Puller") 
                       (feedbk m) $ \_ -> do
          push s (streamList (map B.pack ss))
          map B.unpack <$> takeMVar m
    where feedbk m = do
            rs <- CL.consume
            liftIO (putMVar m  rs)
          
  
  zipF, zipS :: [a] -> [a]
  zipF []  = []
  zipF [x] = [x]
  zipF (x:_:ys) = x : zipF ys
  zipS []  = []
  zipS [_] = []
  zipS (_:y:ys) = y : zipS ys

  socktask :: Context -> IO ()
  socktask ctx = do
      m <- newEmptyMVar
      _ <- forkIO $ go m 
      takeMVar m
    where go m = Z.withSocket ctx Z.Rep $ \s -> do
                   Z.bind s isock
                   putMVar m ()
                   recvAll s >>= sendAll s

  bounce :: Conduit B.ByteString ()
  bounce = passThrough 

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

  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    putStrLn "========================================="
    putStrLn "       Patterns Library Test Suite"
    putStrLn "                 Basic"
    putStrLn "========================================="
    r <- runTest "Client Request at once"
                  (deepCheck prpClReqAtOnce)     ?>
         runTest "Client Timeout"
                  (deepCheck prpClTmo)           ?>
         runTest "Server stops"
                  (deepCheck prpSvStop )         ?> 
         runTest "Server at once"
                  (deepCheck prpSvRepAtOnce)     ?>
         runTest "Queue"
                  (deepCheck prpQueue)           ?>
         runTest "Publisher simple issue"
                  (deepCheck prpPubIssue)        ?> 
         runTest "Forwarder"
                  (deepCheck prpForwarder)       ?>
         runTest "Subscribe"
                  (deepCheck prpSubIssue)        ?>
         runTest "Partial Subscription"
                  (deepCheck prpSubPartial)      ?>
         runTest "Partial multiple subscription"
                  (deepCheck prpSubMultiPartial) ?> 
         runTest "Just push"
                  (deepCheck prpPush)            ?>
         runTest "Pipeline"
                  (deepCheck prpPipeline)        ?>
         runTest "Just pull"
                  (deepCheck prpPull)    

         {-
         runTest "Timeout" (oneCheck prp_onTmo)      ?>
         runTest "Error" (deepCheck prp_onErr)       ?> 
         runTest "Parameter" (deepCheck prp_Param)   ?> 
         runTest "Start/Pause" (deepCheck prp_Pause) ?> 
         runTest "add"         (deepCheck prp_add)
         -}
    case r of
      Success {} -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
