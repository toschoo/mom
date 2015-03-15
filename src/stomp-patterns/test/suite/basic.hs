module Main
where

  import           System.Exit
  import           System.Timeout
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import           Common

  import           Registry  
  import           Types 

  import           Network.Mom.Stompl.Patterns.Basic -- <--- SUT
  import           Network.Mom.Stompl.Patterns.Balancer
  import           Network.Mom.Stompl.Client.Queue

  import           Data.List (nub)
  import           Data.Maybe
  import           Data.Time.Clock
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO)
  import           Control.Monad (void)
  import           Codec.MIME.Type (nullType)

  ------------------------------------------------------------------------
  -- client / server
  ------------------------------------------------------------------------
  -- simple request 
  prpRequest :: NonEmptyList Char -> Property
  prpRequest (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c -> 
      withClient c "Cl1" "Service1" 
                   ("/q/client1",  [], [],  stringIn) 
                   ("/q/service1", [], [], stringOut) $ \cl ->
        withServerThread c "Srv1" "Service1" nullType [] (return . msgContent)
                   ("/q/service1", [], [],  stringIn)
                   ("unknown",     [], [], stringOut)
                   ("", 0, (0,0,0)) onerr $ do
          mbM <- request cl (-1) nullType [] is
          case mbM of
            Nothing -> return []
            Just m  -> return $ msgContent m
    assert (s == is)

  --- simple request with tmo ---------------------------------------------
  prpReqTmoOk :: NonEmptyList Char -> Property
  prpReqTmoOk (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c -> 
      withClient c "Cl1" "Service1" 
                   ("/q/client1",  [], [],  stringIn) 
                   ("/q/service1", [], [], stringOut) $ \cl ->
        withServerThread c "Srv1" "Service1" nullType [] (return . msgContent)
                   ("/q/service1", [], [],  stringIn)
                   ("unknown",     [], [], stringOut)
                   ("", 0, (0,0,0)) onerr $ do
          mbM <- request cl 90000 nullType [] is
          case mbM of
            Nothing -> return []
            Just m  -> return $ msgContent m
    assert (s == is)

  --- simple request with tmo fails ---------------------------------------
  prpReqTmoNOk :: NonEmptyList Char -> Property
  prpReqTmoNOk (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c -> 
      withClient c "Cl1" "Service1" 
                   ("/q/client1",  [], [],  stringIn) 
                   ("/q/service1", [], [], stringOut) $ \cl -> do
        mbM <- request cl 1000 nullType [] is
        case mbM of
          Nothing -> return []
          Just m  -> return $ msgContent m
    assert (s == [])

  --- check request -------------------------------------------------------
  prpReqCheck :: NonEmptyList Char -> Property
  prpReqCheck (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c -> 
      withClient c "Cl1" "Service1" 
                   ("/q/client1",  [], [],  stringIn) 
                   ("/q/service1", [], [], stringOut) $ \cl ->
        withServerThread c "Srv1" "Service1" nullType [] (return . msgContent)
                   ("/q/service1", [], [],  stringIn)
                   ("unknown",     [], [], stringOut)
                   ("", 0, (0,0,0)) onerr $ do
          void $ request cl 0 nullType [] is
          mbM <- checkRequest cl 90000
          case mbM of
            Nothing -> return []
            Just m  -> return $ msgContent m
    assert (s == is)

  --- withServerThread, 1 client -----------------------------------------
  prpSrvThrd1 :: NonEmptyList Char -> Property
  prpSrvThrd1 (NonEmpty is) = monadicIO $ do
    s <- run $ testWithBalancer "/q/service1" $ \c -> 
      withClient c "Cl1" "Service1" 
                   ("/q/client1",  [], [],  stringIn) 
                   ("/q/service1", [], [], stringOut) $ \cl ->
        withServerThread c "Srv1" "Service1" nullType [] (return . msgContent)
                   ("/q/internal", [], [],  stringIn)
                   ("unknown",     [], [], stringOut)
                   ("/q/registry1", 500000, (500,0,2000)) onerr $ do
          mbM <- request cl 100000 nullType [] is
          case mbM of
            Nothing -> return []
            Just m  -> return $ msgContent m
    assert (s == is)

  --- withServerThread, n clients -----------------------------------------
  prpSrvThrdN :: Int -> NonEmptyList Char -> Property
  prpSrvThrdN n (NonEmpty is) | n <= 0 || n > 100  = prpSrvThrdN 10 (NonEmpty is)
                              | otherwise          = monadicIO $ do
    rs <- run $ testWithBalancer "/q/service1" $ \c -> 
      withClients n c "/q/service1" $ \cs ->
        withServerThread c "Srv1" "Service1" nullType [] (return . msgContent)
                   ("/q/internal", [], [],  stringIn)
                   ("unknown",     [], [], stringOut)
                   ("/q/registry1", 500000, (500,0,2000)) onerr $ do
          m  <- newMVar []
          mapM_ (withReq m) cs
          nub <$> evalR m
    if length rs /= 1 
      then do run $ print rs
              assert False      
      else assert (head rs == is)
    where evalR ms = do x <- readMVar ms 
                        if length x == n 
                          then return $ map go x
                          else threadDelay 1000 >> evalR ms
          go mbM     =
            case mbM of
              Nothing -> ""
              Just m  -> msgContent m
          withReq m cl = 
            void $ forkIO (do
              mbM <- request cl 200000 nullType [] is
              modifyMVar_ m $ \rs -> return (mbM:rs))

  -- with error / with and without registry ------------------------------
  prpSrvExcOk :: MVar Int -> NonEmptyList Char -> Property
  prpSrvExcOk o (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c -> 
      withClient c "Cl1" "Service1" 
                   ("/q/client1",  [], [],  stringIn) 
                   ("/q/service1", [], [], stringOut) $ \cl -> do
        x <- newMVar 0
        withServerThread c "Srv1" "Service1" nullType [] (job x)
                   ("/q/service1", [], [],  stringIn)
                   ("unknown",     [], [], stringOut)
                   ("", 0, (0,0,0)) (subtleErr o) $ do
          mb_ <- request cl 50000 nullType [] is
          case mb_ of
            Just _  -> return "" -- error!
            Nothing -> do
              mbM <- request cl 50000 nullType [] is
              case mbM of
                Nothing -> return []
                Just m  -> return $ msgContent m
    assert (s == is)
    where job :: MVar Int -> Message String -> IO String 
          job x m = do l <- readMVar x
                       if l == 0 
                         then do modifyMVar_ x (\_ -> return 1)
                                 throwIO (AppX "Test!")
                         else return $ msgContent m

  ------------------------------------------------------------------------
  -- Pipeline
  ------------------------------------------------------------------------
  -- simple push ----------------------------------------------------------
  prpPush :: NonEmptyList Char -> Property
  prpPush (NonEmpty is) = monadicIO $ do
    m <- run   newEmptyMVar 
    s <- run $ testWithCon $ \c ->
      testWithWorker m c "" "/q/task1" $ \_ -> 
        withPusher c "Push1" "Task1" 
                     ("/q/task1", [], [], stringOut) $ \p -> do
          push p nullType [] is
          mbR <- timeout 100000 $ takeMVar m
          case mbR of
            Nothing -> return ""
            Just r  -> return r 
    assert (s == is)

  -- simple push 1 ---------------------------------------------------------
  prpPush1 :: NonEmptyList Char -> Property
  prpPush1 (NonEmpty is) = monadicIO $ do
    m <- run   newEmptyMVar 
    s <- run $ testWithBalancer "/q/task1" $ \c -> 
      testWithWorker m c "/q/registry1" "/q/internal" $ \_ -> 
        withPusher c "Push1" "Task1" 
                    ("/q/task1", [], [], stringOut) $ \p -> do
          push p nullType [] is
          mbR <- timeout 100000 $ takeMVar m
          case mbR of
            Nothing -> return ""
            Just r  -> return r 
    assert (s == is)

  -- push n ---------------------------------------------------------------
  prpPushN :: Int -> NonEmptyList Char -> Property
  prpPushN i (NonEmpty is) | i <= 1 || i > 100 = prpPushN 10 (NonEmpty is)
                           | otherwise         = monadicIO $ do
    m <- run   newEmptyMVar 
    s <- run $ testWithBalancer "/q/task1" $ \c -> 
      testWithWorker m c "/q/registry1" "/q/internal" $ \_ -> 
        withPushers  i c "/q/task1" $ \ps -> do
          mapM_ (\p -> push p nullType [] is) ps
          rs <- nub <$> collectR m [] (0::Int)
          if length rs /= 1 then return ""
                            else return $ head rs
    assert (s == is)
    where collectR m rs stp | stp > 10  = throwIO $ TimeoutX "on MVar!"
                            | otherwise = do 
            mbR <- timeout 100000 $ takeMVar m
            case mbR of
              Nothing -> collectR m rs (stp+1)
              Just r  -> let rs' = r:rs
                          in if length rs' == i 
                               then return rs'
                               else collectR m rs' 0

  -- exception in push ----------------------------------------------------
  prpPushExc :: MVar Int -> NonEmptyList Char -> Property
  prpPushExc o (NonEmpty is) = monadicIO $ do
    m <- run   newEmptyMVar 
    e <- run $ newMVar (0::Int)
    s <- run $ testWithCon $ \c ->
      withTaskThread c "Task" "Task1" (createErr e m)
                      ("/q/task1", [], [], stringIn)
                      ("", 0, (0,0,0)) (subtleErr o) $ 
        withPusher c "Push1" "Task1" 
                     ("/q/task1", [], [], stringOut) $ \p -> do
          push p nullType [] is
          push p nullType [] is
          mbR <- timeout 100000 $ takeMVar m
          case mbR of
            Nothing -> return ""
            Just r  -> return r 
    assert (s == is)
    where createErr e m msg = do 
            x <- readMVar e
            if x == 0 
              then do modifyMVar_ e $ \_ -> return 1
                      throwIO $ AppX "Error!"
              else putMVar m $ msgContent msg
                         
  ------------------------------------------------------------------------
  -- Publish and Subscribe
  ------------------------------------------------------------------------
  -- publish to 1 --------------------------------------------------------
  prpPub :: NonEmptyList Char -> Property
  prpPub (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c ->
      withPub c "Pub1" "Pub1" "/q/sub2pub1" onerr
                ("unknown", [], [], stringOut) $ \p -> 
        withSub c "Sub1" "Pub1" "/q/sub2pub1" 500000
                ("/q/sub1", [], [],  stringIn) $ \s -> do
          publish p nullType [] is
          mbI <- checkIssue s 100000 
          case mbI of
            Nothing -> return ""
            Just i  -> return $ msgContent i
    assert (s == is)
                         
  -- publish to n --------------------------------------------------------
  prpPubN :: Int -> NonEmptyList Char -> Property
  prpPubN n (NonEmpty is) | n <= 1 || n > 100 = prpPubN 10 (NonEmpty is)
                          | otherwise         = monadicIO $ do
    s <- run $ testWithCon $ \c -> 
      withPub c "Pub1" "Pub1" "/q/sub2pub1" onerr
               ("unknown", [], [], stringOut) $ \p -> 
        -- t1 <- getCurrentTime
        withSubs n c "/q/sub2pub1" $ \ss -> do -- withSubs is slow
                                               -- because we do all
                                               -- the registration
                                               -- ceremonies one 
                                               -- after another
          publish p nullType [] is
          m <- newMVar []
          mapM_ (collectR m) ss
          r <- nub <$> waitR m (0::Int)
          -- t2 <- getCurrentTime
          -- putStrLn $ show n ++ " cycles = " ++ show (t2 `diffUTCTime`  t1)
          if length r == 1 then return $ head r
                           else return ""
    assert (s == is)
    where collectR m s = 
            void (forkIO $ do 
                    mbR <- checkIssue s (-1) -- 100000
                    case mbR of
                      Nothing -> throwIO $ TimeoutX "on checkIssue"
                      Just r  -> modifyMVar_ m $ \rs -> 
                                   return (msgContent r:rs))
          waitR m i | i > 1000  = throwIO $ TimeoutX "on waiting"
                    | otherwise = do
            x <- readMVar m
            if length x == n
              then return x
              else threadDelay 100 >> waitR m (i+1)
                         
  -- publisher thread -----------------------------------------------------
  prpPubThread :: NonEmptyList Char -> Property
  prpPubThread (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c ->
      withPubThread c "Pub1" "Pub1" "/q/sub2pub1" 
                    nullType [] (return is) 
                    ("unknown", [], [], stringOut) 
                    50000 onerr $ 
        withSub c "Sub1" "Pub1" "/q/sub2pub1" 500000
                ("/q/sub1", [], [],  stringIn) $ \s -> do
          mbI <- checkIssue s 100000
          case mbI of
            Nothing -> return ""
            Just i  -> return $ msgContent i
    assert (s == is)

  -- to test rate monotonic publishing ------------------------------------
  -- the test is not very good, since the network delay -------------------
  -- varies a lot       ---------------------------------------------------
  prpPubMon :: NonEmptyList Char -> Property
  prpPubMon (NonEmpty is) = monadicIO $ do
    n <- pick $ choose (10,50)
    s <- run $ testWithCon $ \c ->
      withPubThread c "Pub1" "Pub1" "/q/sub2pub1" 
                    nullType [] (return is) 
                    ("unknown", [], [], stringOut) 
                    50000 onerr $ 
        withSub c "Sub1" "Pub1" "/q/sub2pub1" 500000
                 ("/q/sub1", [], [],  stringIn) (go n [])
    assert (s == is)
    where go :: Int -> [String] -> SubA String -> IO String
          go 0 [] _ = return ""
          go 0 rs _ = let r = nub rs
                       in if length r /= 1 then return ""
                                           else return $ head r
          go n rs s = do mbI <- checkIssue s 100000
                         case mbI of
                           Nothing -> return ""
                           Just i  -> go (n-1) (msgContent i:rs) s
                                     
  -- exception in pub thread ----------------------------------------------
  prpPubExc :: MVar Int -> NonEmptyList Char -> Property
  prpPubExc o (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c -> do
      e <- newMVar (0::Int)
      withPubThread c "Pub1" "Pub1" "/q/sub2pub1" 
                    nullType [] (createErr e) 
                    ("unknown", [], [], stringOut) 
                    50000 (subtleErr o) $ 
        withSub c "Sub1" "Pub1" "/q/sub2pub1" 500000
                 ("/q/sub1", [], [],  stringIn) $ \s -> do
          mbI <- checkIssue s 1000000
          case mbI of
            Nothing -> return ""
            Just i  -> return $ msgContent i
    assert (s == is)
    where createErr e = do 
            x <- readMVar e
            if x == 0 
              then do modifyMVar_ e $ \_ -> return 1
                      throwIO $ AppX "Error!"
              else return is

  -- sub -----------------------------------------------------------------
  prpSub :: NonEmptyList Char -> Property
  prpSub (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c -> do
      m <- newEmptyMVar
      withPub c "Pub1" "Pub1" "/q/sub2pub1" onerr
                ("unknown", [], [], stringOut) $ \p ->
        withSubThread c "Sub1" "Pub1" "/q/sub2pub1" 500000
                       ("/q/sub1", [], [], stringIn)
                       (putMVar m . msgContent) onerr $ do
          publish p nullType [] is
          takeMVar m
    assert (s == is)

  -- exception in sub -----------------------------------------------------
  prpSubExc :: MVar Int -> NonEmptyList Char -> Property
  prpSubExc o (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c -> do
      m <- newEmptyMVar
      e <- newMVar (0::Int)
      withPub c "Pub1" "Pub1" "/q/sub2pub1" onerr
                ("unknown", [], [], stringOut) $ \p ->
        withSubThread c "Sub1" "Pub1" "/q/sub2pub1" 500000
                       ("/q/sub1", [], [], stringIn)
                       (createErr m e) 
                       (subtleErr   o) $ do
          publish p nullType [] is
          publish p nullType [] is
          takeMVar m
    assert (s == is)
    where createErr m e msg = do 
            x <- readMVar e
            if x == 0 
              then do modifyMVar_ e $ \_ -> return 1
                      throwIO $ AppX "Error!"
              else putMVar m $ msgContent msg

  -- sub mvar ------------------------------------------------------------
  prpSubMVar :: NonEmptyList Char -> Property
  prpSubMVar (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c -> do
      m <- newMVar ""
      withPub c "Pub1" "Pub1" "/q/sub2pub1" onerr
                ("unknown", [], [], stringOut) $ \p ->
        withSubMVar c "Sub1" "Pub1" "/q/sub2pub1" 500000
                       ("/q/sub1", [], [], stringIn) m onerr $ do
          publish p nullType [] is
          testMVar 100 m     
    assert (s == is)
    where testMVar :: Int -> MVar String -> IO String
          testMVar 0 _ = return ""
          testMVar n m = do x <- readMVar m
                            if null x then threadDelay 10000 
                                           >> testMVar (n-1) m
                                      else return x
          
  setBack :: Registry -> JobName -> Int -> IO ()
  setBack r jn i = mapAllR r jn (\p -> p{prvNxt = timeAdd (prvNxt p) i})

  setTo :: Registry -> JobName -> UTCTime -> IO ()
  setTo r jn now = mapAllR r jn (\p -> p{prvNxt = now})

  withClients :: Int -> Con -> QName -> 
                 ([ClientA String String] -> IO a) -> IO a
  withClients n c sq action = withNClient n []
    where withNClient 0 cs = action cs
          withNClient k cs = 
            withClient c ("CL-" ++ show k) "Service1"
                         ("/q/client" ++ show k, [], [],  stringIn)
                         (sq,                    [], [], stringOut) $ \cl ->
              withNClient (k-1) (cl:cs)

  withPushers :: Int -> Con -> QName -> ([PusherA String] -> IO a) -> IO a
  withPushers n c tq action = withNPushers n []
    where withNPushers 0 ps = action ps
          withNPushers k ps = 
            withPusher c ("PUSH-" ++ show k) "Task1"
                         (tq, [], [], stringOut) $ \p ->
               withNPushers (k-1) (p:ps)

  withSubs :: Int -> Con -> QName -> ([SubA String] -> IO a) -> IO a
  withSubs n c rq action  = withNSubs n []
    where withNSubs 0 ss  = action ss
          withNSubs k ss  = 
            withSub c ("Sub"    ++ show k) "Pub1" rq 500000
                      ("/q/sub" ++ show k, [], [], stringIn) $ \s ->
              withNSubs (k-1) (s:ss)

  testWithBalancer :: QName -> (Con -> IO a) -> IO a
  testWithBalancer jq action =
    withConnection "localhost" 61613 [] [] $ \c -> 
      withBalancer c "Bal-1" "/q/registry1" (0,5000) jq onerr $ action c

  testWithWorker :: MVar String -> Con -> QName -> QName -> 
                                  (Con -> IO a) -> IO a
  testWithWorker m c rq sq action = 
    withTaskThread c "Task-1" "Task1" tsk 
                     (sq, [], [],  stringIn)
                     (rq, 500000, (500,0,5000))
                     onerr $ action c
    where tsk msg = putMVar m (msgContent msg) 

  testWithReg :: (Registry -> IO a) -> IO a
  testWithReg action =
    withConnection "localhost" 61613 [] [] $ \c -> 
      withRegistry c "Test-1" "/q/registry1" (0,0) onerr action

  testWithCon :: (Con -> IO a) -> IO a
  testWithCon = withConnection "localhost" 61613 [] [] 
                   
  onerr :: OnError
  onerr e m = putStrLn $ "Error in " ++ m ++ ": " ++ show e

  nonemptyString :: NonEmptyList (NonEmptyList Char) -> [String]
  nonemptyString (NonEmpty ns) = map (\(NonEmpty c) -> c) ns

  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    putStrLn "========================================="
    putStrLn "   Stompl Patterns Library Test Suite"
    putStrLn "                Basic"
    putStrLn "========================================="
    o <- newMVar 0
    r <- -- testWithCon $ \c -> do
         runTest "simple request (-1)"
                  (deepCheck prpRequest)    ?>
         runTest "simple request, timeout successful" 
                  (deepCheck prpReqTmoOk)      ?>
         runTest "simple request, timeout fail" 
                  (deepCheck prpReqTmoNOk)     ?> 
         runTest "checkRequest" 
                  (deepCheck prpReqCheck)      ?> 
         runTest "Server Thread - 1 client"
                  (deepCheck prpSrvThrd1)      ?> 
         runTest "Server Thread - n clients"
                  (someCheck 10 prpSrvThrdN)   ?> 
         runTest "Server Thread - Exception"
                  (deepCheck (prpSrvExcOk o))  ?> 
         runTest "Worker Thread - push"
                  (deepCheck prpPush)          ?> 
         runTest "Worker Thread - registry 1"
                  (deepCheck prpPush1)         ?> 
         runTest "Worker Thread - registry n"
                  (someCheck 10 prpPushN)      ?> 
         runTest "Worker Thread - Exception"
                  (deepCheck (prpPushExc o))   ?> 
         runTest "Simple Publisher"
                  (deepCheck prpPub)           ?> 
         runTest "Simple Publisher with n"
                  (someCheck 10 prpPubN)       ?> 
         runTest "PubThread"
                  (deepCheck prpPubThread)     ?> 
         runTest "PubThread - monotonic"
                  (someCheck 10 prpPubMon)     ?> 
         runTest "PubThread - Exception"
                  (deepCheck (prpPubExc o))    ?> 
         runTest "SubThread"
                  (deepCheck prpSub)           ?> 
         runTest "SubThread - Exception"
                  (deepCheck (prpSubExc o))    ?> 
         runTest "SubMVar"
                  (deepCheck prpSubMVar)
         
    case r of
      Success {} -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
