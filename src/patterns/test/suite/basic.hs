module Main
where

  import           Helper
  import           System.Exit
  import           System.Timeout
  import           System.IO (stdout, hFlush)
  import qualified System.ZMQ as Z
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import           Network.Mom.Patterns
  import qualified Data.Enumerator      as E
  import qualified Data.Enumerator.List as EL
  import           Data.Enumerator (($$))
  import qualified Data.Sequence as S
  import           Data.Sequence ((|>))
  import           Data.List (nub)
  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.Time.Clock
  import           Data.Either
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Monad
  import           Control.Monad.Loops
  import           Control.Monad.Trans (liftIO)
  import           Control.Exception (try, throwIO, AssertionFailed(..), SomeException)

  ------------------------------------------------------------------------------
  -- All
  -- - change option
  ------------------------------------------------------------------------------

  ------------------------------------------------------------------------------
  -- Simple synchronous request
  ------------------------------------------------------------------------------
  prp_synReq :: NonEmptyList String -> Property
  prp_synReq (NonEmpty ss) = testContext ss $ \ctx -> 
    testServer ctx ss $ \_ -> 
      withClient ctx (Address "inproc://srv" []) outString inString $ \c -> 
        request c (just "") EL.consume

  ------------------------------------------------------------------------------
  -- Simple asynchronous request
  ------------------------------------------------------------------------------
  prp_asynReq :: NonEmptyList String -> Property
  prp_asynReq (NonEmpty ss) = testContext ss $ \ctx -> 
    testServer ctx ss $ \_ -> 
      withClient ctx (Address "inproc://srv" []) outString inString $ \c -> 
        askFor c (just "") >> waitClient c
    where waitClient c = do
            mb <- checkFor c EL.consume
            case mb of
              Just ei -> return ei
              Nothing -> do threadDelay 1000
                            waitClient c

  ------------------------------------------------------------------------------
  -- Bind / Connect
  ------------------------------------------------------------------------------
  prp_conSrv :: NonEmptyList String -> Property
  prp_conSrv (NonEmpty ss) = testContext ss $ \ctx -> do
    let add1 = Address "inproc://srv1" []
    let add2 = Address "inproc://srv2" []
    withQueue  ctx "Queue" (add1, Bind) (add2, Bind) onErr_ $ \_ ->
      withClient ctx add1 outString inString $ \c -> 
        withServer ctx "Test" noparam 128 add2 Connect
                   inString outString onErr
                   (\_ -> one "")-- ignore
                   (\_ _ _ -> mkStream ss) $ \_ -> 
          request c (just "") EL.consume

  ------------------------------------------------------------------------------
  -- Bytestring Converter
  ------------------------------------------------------------------------------
  prp_bstrReq :: NonEmptyList String -> Property
  prp_bstrReq (NonEmpty ss) = testContext (map B.pack ss) $ \ctx ->
    withServer ctx "Test" noparam 128
                   (Address "inproc://srv" []) Bind 
                   idIn idOut onErr
                   (\_ -> one B.empty)-- ignore
                   (\_ _ _ -> mkStream $ map B.pack ss) $ \_ -> 
      withClient ctx (Address "inproc://srv" []) idOut idIn $ \c ->
        request c (just B.empty) EL.consume

  ------------------------------------------------------------------------------
  -- UTF Converter
  ------------------------------------------------------------------------------
  utf8 :: String
  utf8  = "此派男野人です。\n" ++
          "此派男野人です。それ派個共です。\n" ++
          "ひと"

  prp_utf8Req :: Property
  prp_utf8Req = testContext [utf8] $ \ctx ->
    withServer ctx "Test" noparam 128
                   (Address "inproc://srv" []) Bind 
                   inUTF8 outUTF8 onErr
                   (\_ -> one "") -- ignore
                   (\_ _ _ -> mkStream [utf8]) $ \_ -> 
      withClient ctx (Address "inproc://srv" []) outUTF8 inUTF8 $ \c -> do
        putStrLn $ "Test: " ++ utf8
        request c (just "") EL.consume

  ------------------------------------------------------------------------------
  -- issue / withSub
  ------------------------------------------------------------------------------
  prp_IssueSub :: NonEmptyList String -> Property
  prp_IssueSub (NonEmpty ss) = testContext ss $ \ctx -> do
    m <- newEmptyMVar
    let add = Address "inproc://pub" []
    withPub ctx add outString $ \p -> 
      withSub ctx "Sub" noparam [""] add inString onErr_ 
                 (\_ _ -> EL.consume >>= \l -> 
                            liftIO (putMVar m l)) $ \_ -> do
                 issue p (mkStream ss)
                 Right <$> takeMVar m

  ------------------------------------------------------------------------------
  -- issue / wait
  ------------------------------------------------------------------------------
  prp_IssueWait :: NonEmptyList String -> Property
  prp_IssueWait (NonEmpty ss) = testContext ss $ \ctx -> do
    let add = Address "inproc://pub" []
    withPub ctx add outString $ \p -> 
      withSporadicSub ctx add inString alltopics $ \s -> do
        issue p (mkStream ss)
        waitSub s EL.consume

  ------------------------------------------------------------------------------
  -- issue / check
  ------------------------------------------------------------------------------
  prp_IssueCheck :: NonEmptyList String -> Property
  prp_IssueCheck (NonEmpty ss) = testContext ss $ \ctx -> do
    let add = Address "inproc://pub" []
    withPub ctx add outString $ \p -> 
      withSporadicSub ctx add inString alltopics $ \s -> do
        issue p (mkStream ss)
        waitForIssue s
    where waitForIssue s = do
            mb <- checkSub s EL.consume
            case mb of
              Nothing -> do threadDelay 1000
                            waitForIssue s
              Just x  -> return x

  ------------------------------------------------------------------------------
  -- Periodic wait
  ------------------------------------------------------------------------------
  prp_Periodic :: NonEmptyList String -> Property
  prp_Periodic (NonEmpty ss) = testContext ("10":ss) $ \ctx -> do
    let add = Address "inproc://pub" []
    let t   = "10"
    withPeriodicPub ctx "Test" noparam 10000 add
                        outString onErr_ 
                        (\_ _ _ s -> 
                            case s of
                              (E.Continue k) -> 
                                mkStream ss $$ k (E.Chunks [t])
                              _ -> E.returnI s) $ \_ ->
      withSporadicSub ctx add inString [t] $ \s -> waitSub s EL.consume

  ------------------------------------------------------------------------------
  -- Periodicity 
  ------------------------------------------------------------------------------
  prp_Periodicity :: Property
  prp_Periodicity = monadicIO $ run (withContext 1 $ action) >>= assert
    where action ctx = do
            let d = 20000
            withPeriodicPub ctx "Test" noparam d add
                                outString onErr_ 
                                (\_ _ _ -> mkStream [""]) $ \_ -> 
              withSporadicSub ctx add inString alltopics $ \s -> do
                ok <- and <$> mapM (go s $ d) [1..100]
                putStrLn "" 
                return ok
          go :: Sub String -> Timeout -> Int -> IO Bool
          go s d _ = do
            t1 <- getCurrentTime
            ei <- waitSub s EL.consume
            case ei of
              Left  _ -> return False
              Right _ -> do
                t2  <- getCurrentTime
                if t2 > t1 && 
                   t2 <= uToNominal (3 * (fromIntegral d)) `addUTCTime` t1
                  then putStr "." >> hFlush stdout >> return True
                  else putStrLn ("Diff: " ++ show t1 ++ " - " ++ show t2) >> return False
          uToNominal :: Int -> NominalDiffTime 
          uToNominal t = fromIntegral t / (1000000::NominalDiffTime)
          add = Address "inproc://pub" []

  ------------------------------------------------------------------------------
  -- resubscribe
  ------------------------------------------------------------------------------
  prp_Resubscribe :: NonEmptyList String -> Property
  prp_Resubscribe (NonEmpty ss) = testContext ("20":ss) $ \ctx -> do
    let add = Address "inproc://pub" []
    withPub ctx add outString $ \p -> 
      withSporadicSub ctx add inString ["10"] $ \s -> do
        mb1 <- testcase p s "10" "20"
        case mb1 of 
          Nothing -> return $ Right []
          Just  x1 -> 
            case x1 of 
              Left e  -> return $ Left e
              Right _ -> do
                resubscribe s "20"
                mb2 <- testcase p s "20" "30"
                case mb2 of
                  Nothing -> return $ Right []
                  Just x2  -> 
                    case x2 of
                      Left e   -> return $ Left e
                      Right _  -> do
                        mb3 <- testcase p s "10" "30"
                        case mb3 of
                          Nothing -> return $ Right []
                          Just x3  -> 
                            case x3 of
                              Left e   -> return $ Left e
                              Right _  -> do
                                unsubscribe s "10"
                                mb4 <- testcase p s "20" "10"
                                case mb4 of
                                  Nothing -> return $ Right []
                                  Just x4 -> return x4
    where waitForIssue :: Sub String -> Int -> 
                          IO (Maybe (Either SomeException [String]))
          waitForIssue s n = do 
            mb <- checkSub s EL.consume
            case mb of
              Nothing -> if n > 0 
                           then do threadDelay 1000
                                   waitForIssue s (n-1)
                           else return Nothing
              Just x  -> return $ Just x
          envStream t s step = 
            case step of
              (E.Continue k) -> 
                mkStream s $$ k (E.Chunks [t])
              _ -> E.returnI step 
          testcase p s yes no = do  
            issue p (envStream no ss)
            mb <- waitForIssue s 5
            case mb of
              Just  _ -> return Nothing
              Nothing -> do
                issue p (envStream yes ss)
                waitForIssue s 5

  ------------------------------------------------------------------------------
  -- Subscribe non-string
  ------------------------------------------------------------------------------
  prp_subNonString :: Int -> Property
  prp_subNonString x = monadicIO $ run $ withContext 1 $ \ctx -> do
    let add = Address "inproc://pub" []
    withPub ctx add outInt $ \p -> 
      withSporadicSub ctx add inInt [""] $ \s -> do
        issue p (just x)
        ei <- waitSub s EL.consume
        case ei of
          Right [i] -> return $ i == x
          _         -> return False
    where outInt = return . B.pack . show
          inInt  = return . read   . B.unpack

  ------------------------------------------------------------------------------
  -- Pipe
  ------------------------------------------------------------------------------
  prp_pipe :: NonEmptyList String -> Property
  prp_pipe (NonEmpty ss) = testContext ss $ \ctx -> do
    m <- newEmptyMVar
    let add = Address "inproc://pipe" []
    withPipe ctx add outString $ \p -> 
      withPuller ctx "Pull" noparam add inString onErr_ 
                 (\_ _ -> EL.consume >>= \l -> 
                            liftIO (putMVar m l)) $ \_ -> do
                 push p (mkStream ss)
                 Right <$> takeMVar m

  ------------------------------------------------------------------------------
  -- Pair
  ------------------------------------------------------------------------------
  prp_pair :: NonEmptyList String -> Property
  prp_pair (NonEmpty ss) = testContext ss $ \ctx -> do
    let add = Address "inproc://pair" []
    withPeer ctx add Bind inString outString $ \p1 -> 
      withPeer ctx add Connect inString outString $ \p2 -> do
        send p1 (mkStream ss)
        ei1 <- receive p2 EL.consume
        case ei1 of
          Left  e -> return $ Left e
          Right x -> do
            send p2 (mkStream x)
            receive p1 EL.consume

  ------------------------------------------------------------------------------
  -- Close Server
  ------------------------------------------------------------------------------
  prp_closeAfterSrv :: NonEmptyList String -> Property
  prp_closeAfterSrv (NonEmpty ss) = testContext ss $ \ctx ->
    action ctx >>= \_ -> threadDelay 10000 >> action ctx
    where action ctx = 
            withServer ctx "Test" noparam 1
                           (Address "inproc://srv" []) Bind 
                           inString outString onErr
                           (\_ -> one "") -- ignore
                           (\_ _ _ -> mkStream ss) $ \_ ->
               withClient ctx (Address "inproc://srv" []) 
                              outString inString $ \c -> 
                 request c (just "") EL.consume

  ------------------------------------------------------------------------------
  -- Close Pub
  ------------------------------------------------------------------------------
  prp_closeAfterPub :: NonEmptyList String -> Property
  prp_closeAfterPub (NonEmpty ss) = testContext ss $ \ctx ->
    action ctx >>= \_ -> threadDelay 10000 >> action ctx
    where action ctx = let add = Address "inproc://pub" []
            in withPeriodicPub ctx "Test" noparam 10000 add
                            outString onErr_ 
                            (\_ _ _ -> mkStream ss) $ \_ ->
                withSporadicSub ctx add inString alltopics $ \p ->
                  waitSub p EL.consume

  ------------------------------------------------------------------------------
  -- Close Sub
  ------------------------------------------------------------------------------
  prp_closeAfterSub :: NonEmptyList String -> Property
  prp_closeAfterSub (NonEmpty ss) = testContext ss $ \ctx ->
    action ctx >>= \_ -> threadDelay 10000 >> action ctx
    where action ctx = do
            let add = Address "inproc://pub" []
            m <- newEmptyMVar
            withPub ctx add outString $ \p ->
              withSub ctx "Sub" noparam [""] add inString onErr_ 
                 (\_ _ -> EL.consume >>= \l -> 
                            liftIO (putMVar m l)) $ \_ -> do
                 issue p (mkStream ss)
                 Right <$> takeMVar m

  ------------------------------------------------------------------------------
  -- Close Puller
  ------------------------------------------------------------------------------
  prp_closeAfterPuller :: Property
  prp_closeAfterPuller = monadicIO $ run $ withContext 1 $ \ctx ->
    action ctx >> threadDelay 10000 >> action ctx
    where action ctx = let add = Address "inproc://pipe" []
            in withPipe ctx add outString $ \p ->
                withPuller ctx "Test" noparam add
                           inString onErr_ 
                           (\_ _ -> EL.consume >>= \_ -> return ()) $ \_ ->
                 push p (just "")

  ------------------------------------------------------------------------------
  -- Close Z.socket
  ------------------------------------------------------------------------------
  prp_closeAfterZMQ :: NonEmptyList String -> Property
  prp_closeAfterZMQ (NonEmpty ss) = testContext (head ss) $ \ctx ->
    action ctx >>= \_ -> threadDelay 10000 >> action ctx
    where action ctx =
            Z.withSocket ctx Z.Pub $ \p -> do
              Z.bind p "inproc://pub"
              Z.withSocket ctx Z.Sub $ \s -> do
                Z.connect s "inproc://pub"
                Z.subscribe s ""
                Z.send p (B.pack $ head ss) []
                Right <$> (B.unpack) <$> Z.receive s  []

  ------------------------------------------------------------------------------
  -- Error
  ------------------------------------------------------------------------------
  prp_onErrSrv :: Property
  prp_onErrSrv = testContext [Error, Error, Error] $ \ctx -> do
     m <- newEmptyMVar
     e1 <- tstError m (Address "inproc://srv1" []) 
              ctx onerr (\_ -> one "") (\_ _ _ _ _ -> tryIO mkErr) 
              (action m ctx $ Address "inproc://srv1" []) 
     e2 <- tstError m (Address "inproc://srv2" [])
              ctx onerr (\_ -> EL.consume >> tryIO mkErr ) 
                        (\_ _ _ _ _ -> tryIO $ putStrLn "")
              (action m ctx $ Address "inproc://srv2" []) 
     e3 <- tstError m (Address "inproc://srv3" [])
              ctx onerr (\_ -> EL.consume) 
                        (\x _ _ _ s -> do
                            k <- liftIO $ isEmptyMVar x
                            if k then liftIO (putMVar x ()) >> tryIO mkErr
                                 else mkStream [""] s)
              (cont m ctx $ Address "inproc://srv3" []) 
     threadDelay 10000 -- close everything
     return $ Right [e1, e2, e3]
    where mkErr           = throwIO (AssertionFailed "Test")
          onerr m c _ _ _ = putMVar m c >> return (Just B.empty)
          tstError m srv ctx handle rcv fetch act = do
            x <- newEmptyMVar
            withServer ctx "Test" noparam 1 srv Bind
                          inString outString (handle m)
                          rcv (fetch x) act 
          action m ctx cli _ = 
            withClient ctx cli
                           outString inString $ \c -> do
              _ <- request c (just "") EL.consume
              takeMVar m
          cont m ctx cli _ = 
            withClient ctx cli
                       outString inString $ \c -> do
              _ <- request c (just "") EL.consume
              _ <- request c (just "") EL.consume
              takeMVar m

  ------------------------------------------------------------------------------
  -- Error (Pub)
  ------------------------------------------------------------------------------
  prp_onErrPub :: Property
  prp_onErrPub = testContext [Error, Error] $ \ctx -> do
     m <- newEmptyMVar
     e1 <- tstError m (Address "inproc://pub1" []) ctx
              (action m ctx $ Address "inproc://pub1" []) 
     e2 <- tstError m (Address "inproc://pub3" []) ctx
              (cont m ctx $ Address "inproc://pub3" []) 
     threadDelay 10000 -- close everything
     return $ Right [e1, e2]
    where mkErr           = throwIO (AssertionFailed "Test")
          onerr m c _ _ _ = putMVar m c
          fetch x _ _ _ s = do
            k <- liftIO $ isEmptyMVar x
            if k then liftIO (putMVar x ()) >> tryIO mkErr
                 else mkStream [""] s
          tstError m srv ctx act = do
            x <- newEmptyMVar
            withPeriodicPub ctx "Test" noparam 10000 srv 
                outString (onerr m) (fetch x) act 
          action m _   _   _ = takeMVar m
          cont m ctx cli _ = 
            withSporadicSub ctx cli inString alltopics $ \s -> do
              _ <- checkSub s EL.consume
              x <- takeMVar m
              _ <- waitSub s EL.consume
              _ <- waitSub s EL.consume
              return x

  ------------------------------------------------------------------------------
  -- Error (Sub)
  ------------------------------------------------------------------------------
  prp_onErrSub :: Property
  prp_onErrSub = testContext [Error, Error] $ \ctx -> do
     m <- newEmptyMVar
     e1 <- tstError m ctx (Address "inproc://pub1" []) 
     e2 <- tstError m ctx (Address "inproc://pub3" [])
     threadDelay 10000 -- close everything
     return $ Right [e1, e2]
    where mkErr           = throwIO (AssertionFailed "Test")
          onerr m c _ _ _ = putMVar m c
          rcv x _ _       = do
            k <- liftIO $ isEmptyMVar x
            if k then liftIO (putMVar x ()) >> tryIO mkErr
                 else EL.consume >>= \_ -> return ()
          tstError m ctx add = do
            x <- newEmptyMVar
            withPub ctx add outString $ \p ->
              withSub ctx "Sub" noparam [""] add
                      inString (onerr m) (rcv x) $ \_ -> do
                issue p (mkStream [""])
                takeMVar m

  ------------------------------------------------------------------------------
  -- Error (Pipe)
  ------------------------------------------------------------------------------
  prp_onErrPipe :: Property
  prp_onErrPipe = testContext [Error, Error] $ \ctx -> do
     m <- newEmptyMVar
     e1 <- tstError m ctx (Address "inproc://pub1" []) 
     e2 <- tstError m ctx (Address "inproc://pub3" [])
     threadDelay 10000 -- close everything
     return $ Right [e1, e2]
    where mkErr           = throwIO (AssertionFailed "Test")
          onerr m c _ _ _ = putMVar m c
          rcv x _ _       = do
            k <- liftIO $ isEmptyMVar x
            if k then liftIO (putMVar x ()) >> tryIO mkErr
                 else EL.consume >>= \_ -> return ()
          tstError m ctx add = do
            x <- newEmptyMVar
            withPipe ctx add outString $ \p ->
              withPuller ctx "Puller" noparam add
                      inString (onerr m) (rcv x) $ \_ -> do
                push p (mkStream [""])
                takeMVar m

  ------------------------------------------------------------------------------
  -- Worker Threads
  ------------------------------------------------------------------------------
  prp_threads :: Property
  prp_threads = monadicIO $ do
    ts <- run $ withContext 1 action
    assert $ (nub ts) == ts
    where inThread :: B.ByteString -> IO Int
          inThread  = return . read   . B.unpack
          outThread :: Int -> IO B.ByteString
          outThread = return . B.pack . show
          sndThread :: Fetch String Int
          sndThread c p i s = 
            read <$> (drop 9) <$> show <$> liftIO myThreadId >>= \t -> 
              fetchJust t c p i s
          action ctx = let add = Address "inproc://srv" []
            in withServer ctx "Test" noparam 5 add Bind
                   inString outThread onErr
                   (\_ -> one "") -- ignore
                   sndThread $ \_ -> 
                 withClient ctx add outString inThread $ \c -> do
                   ts <- mapM (\_ -> request c (just "") EL.consume) 
                              ([1..5]::[Int])
                   return $ map (\x -> case x of
                                         Right [t] -> t
                                         _         -> 0) ts

  ------------------------------------------------------------------------------
  -- Worker Threads (Puller)
  ------------------------------------------------------------------------------
  prp_pullThreads :: Property
  prp_pullThreads = monadicIO $ do
    ts <- run $ withContext 1 action
    assert $ (nub ts) == ts
    where saveThread m _ _ = do
            liftIO (myThreadId >>= \t -> modifyMVar_ m (\l -> return $ t:l))
            EL.consume >>= \_ -> return ()
          action ctx = do
            let add = Address "inproc://pull" []
            m <- newMVar []
            withPipe ctx add outString $ \p ->
              withPuller ctx "One" noparam add 
                         inString onErr_ (saveThread m) $ \_ ->
                withPuller ctx "Two" noparam add 
                           inString onErr_  (saveThread m) $ \_ ->
                  withPuller ctx "Three" noparam add 
                             inString onErr_ (saveThread m) $ \_ -> do
                   mapM_ (\_ -> push p $ just "") ([1..3]::[Int])
                   whileM_ (do l <- readMVar m
                               return $ length l == 3) $ threadDelay 1000
                   readMVar m

  ------------------------------------------------------------------------------
  -- ParamSrv
  ------------------------------------------------------------------------------
  prp_paramSrv :: String -> Property
  prp_paramSrv s = monadicIO $ run (withContext 1 action) >>= assert
    where action ctx   = do
            let add = Address "inproc://srv" []
            withServer ctx "Test" noparam 3 add Bind
                          inString outString onErr
                          (\_ -> one "")
                          (\_ param _ -> mkStream [param]) $ \srv ->
              withClient ctx add outString inString $ \c -> do
                ei1 <- request c (just "") EL.consume 
                case ei1 of
                  Right [x] -> 
                    if x /= noparam then return False
                      else do
                        changeParam srv s
                        _   <- request c (just "") EL.consume -- ignore one
                        ei2 <- request c (just "") EL.consume -- for all 
                        ei3 <- request c (just "") EL.consume -- workers
                        ei4 <- request c (just "") EL.consume 
                        let (ls,rs) = partitionEithers [ei2, ei3, ei4]
                        if not (null ls) then return False
                          else return (and $ map (==[s]) rs)
                  _         -> return False

  ------------------------------------------------------------------------------
  -- ParamPub
  ------------------------------------------------------------------------------
  prp_paramPub :: String -> Property
  prp_paramPub s = monadicIO $ run (withContext 1 action) >>= assert
    where action ctx   = do
            let add = Address "inproc://pub" []
            withPeriodicPub ctx "Test" noparam 10000 add
                          outString onErr_ 
                          (\_ param _ -> mkStream [param]) $ \p ->
              withSporadicSub ctx add inString alltopics $ \sub -> do
                ei1 <- waitSub sub EL.consume 
                case ei1 of
                  Right [x] -> 
                    if x /= noparam then return False
                      else do
                        changeParam p s
                        _   <- waitSub sub EL.consume -- ignore one
                        ei2 <- waitSub sub EL.consume 
                        case ei2 of
                          Right [y] -> return (y == s)
                          _         -> return False
                  _         -> return False

  ------------------------------------------------------------------------------
  -- ParamSub
  ------------------------------------------------------------------------------
  prp_paramSub :: String -> Property
  prp_paramSub s = monadicIO $ run (withContext 1 action) >>= assert
    where action ctx   = do
            m <- newEmptyMVar
            let add = Address "inproc://pub" []
            withPub ctx add outString $ \p ->
              withSub ctx "Sub" noparam [""] add
                      inString onErr_ (rcv m) $ \sub -> do
                issue p (mkStream [""])
                x <- takeMVar m
                if x /= noparam then return False
                  else do
                    changeParam sub s
                    issue p (mkStream [""])
                    y <- takeMVar m
                    return (y == s)
          rcv x _ param   = EL.consume >>= \_ -> liftIO $ putMVar x param

  ------------------------------------------------------------------------------
  -- ParamPull
  ------------------------------------------------------------------------------
  prp_paramPull :: String -> Property
  prp_paramPull s = monadicIO $ run (withContext 1 action) >>= assert
    where action ctx   = do
            m <- newEmptyMVar
            let add = Address "inproc://pipe" []
            withPipe ctx add outString $ \pipe -> 
              withPuller ctx "Test" noparam add
                         inString onErr_ (rcv m) $ \p -> do
                push pipe (mkStream [""])
                x <- takeMVar m
                if x /= noparam then return False
                  else do
                    changeParam p s
                    push pipe (mkStream [""])
                    y <- takeMVar m
                    return (y == s)
          rcv x _ param   = EL.consume >>= \_ -> liftIO $ putMVar x param

  ------------------------------------------------------------------------------
  -- Pause Server
  ------------------------------------------------------------------------------
  prp_pauseSrv :: NonEmptyList String -> Property
  prp_pauseSrv (NonEmpty ss) = testContext ss action
    where action ctx   = do
            let add = Address "inproc://srv" []
            withServer ctx "Test" noparam 3 add Bind
                          inString outString onErr
                          (\_ -> one "")
                          (\_ _ _ -> mkStream ss) $ \srv ->
              withClient ctx add outString inString $ \c -> do
                ei <- request c (just "") EL.consume 
                case ei of
                  Left  _ -> return $ Right []
                  Right x -> 
                    if x /= ss then return $ Right []
                      else do
                        pause srv
                        askFor c (just "") -- EL.consume
                        mb <- waitForRequest c 10
                        case mb of
                          Just  _ -> return $ Right []
                          Nothing -> do
                            resume srv
                            mei2 <- waitForRequest c 10
                            case mei2 of
                              Nothing -> return $ Right []
                              Just y  -> return y
          waitForRequest :: Client i o -> Int -> 
                     IO (Maybe (Either SomeException [i]))
          waitForRequest c n = do
            mb <- checkFor c EL.consume
            case mb of
              Nothing -> if n == 0 then return Nothing
                           else do threadDelay 1000
                                   waitForRequest c (n-1)
              j -> return j

  ------------------------------------------------------------------------------
  -- Pause Pub
  ------------------------------------------------------------------------------
  prp_pausePub :: NonEmptyList String -> Property
  prp_pausePub (NonEmpty ss) = testContext ss action
    where action ctx   = do
            let add = Address "inproc://pub" []
            withPeriodicPub ctx "Test" noparam 1000 add 
                            outString onErr_
                            (\_ _ _ -> mkStream ss) $ \pub ->
              withSporadicSub ctx add inString alltopics $ \sub -> do
                ei <- waitSub sub EL.consume 
                case ei of
                  Left  _ -> return $ Right []
                  Right x -> 
                    if x /= ss then return $ Right []
                      else do
                        pause pub
                        waitForPause sub -- wait until pause takes effect
                        mb <- waitForSub sub 10
                        case mb of
                          Just  _ -> return (Right [])
                          Nothing -> do
                            resume pub
                            mei2 <- waitForSub sub 10
                            case mei2 of
                              Nothing -> return $ Right []
                              Just y  -> return y
          waitForSub :: Sub i -> Int -> 
                        IO (Maybe (Either SomeException [i]))
          waitForSub s n = do
            mb <- checkSub s EL.consume
            case mb of
              Nothing -> if n == 0 then return Nothing
                           else do threadDelay 1000
                                   waitForSub s (n-1)
              j       -> return j
          waitForPause s = do
            mb <- timeout 20000 $ waitSub s EL.consume
            case mb of
              Nothing -> return ()
              Just _  -> waitForPause s

  ------------------------------------------------------------------------------
  -- Pause Sub
  ------------------------------------------------------------------------------
  prp_pauseSub :: NonEmptyList String -> Property
  prp_pauseSub (NonEmpty ss) = testContext ss action
    where action ctx   = do
            m <- newMVar []
            let add = Address "inproc://pub" []
            withPub ctx add outString $ \pub ->
              withSub ctx "Sub" noparam [""] add
                      inString onErr_ (rcv m) $ \sub -> do
                issue pub (mkStream ss)
                mbx <- waitForSub m 10
                case mbx of
                  Nothing -> return $ Right []
                  Just  x -> 
                    if x /= ss then return $ Right []
                      else do
                        pause sub
                        issue pub (mkStream ss)
                        mb <- waitForSub m 10
                        case mb of
                          Just  _ -> return (Right [])
                          Nothing -> do
                            resume sub
                            mby <- waitForSub m 10
                            case mby of
                              Nothing -> return $ Right []
                              Just y  -> return $ Right y
          rcv m _ _ = EL.consume >>= \x -> 
                         liftIO $ modifyMVar_ m (\_ -> return x)
          waitForSub :: MVar [String] -> Int -> IO (Maybe [String])
          waitForSub m n = do
            x <- modifyMVar m (\s -> return ([], s))
            case x of
              [] -> if n == 0 then return Nothing
                           else do threadDelay 1000
                                   waitForSub m (n-1)
              xs -> return $ Just xs

  ------------------------------------------------------------------------------
  -- Pause Pull
  ------------------------------------------------------------------------------
  prp_pausePull :: NonEmptyList String -> Property
  prp_pausePull (NonEmpty ss) = testContext ss action
    where action ctx   = do
            m <- newMVar []
            let add = Address "inproc://pub" []
            withPipe ctx add outString $ \pipe ->
              withPuller ctx "Test" noparam add
                         inString onErr_ (rcv m) $ \puller -> do
                push pipe (mkStream ss)
                mbx <- waitForPuller m 10
                case mbx of
                  Nothing -> return $ Right []
                  Just  x -> 
                    if x /= ss then return $ Right []
                      else do
                        pause puller
                        push pipe (mkStream ss)
                        mb <- waitForPuller m 10
                        case mb of
                          Just  _ -> return (Right [])
                          Nothing -> do
                            resume puller
                            mby <- waitForPuller m 10
                            case mby of
                              Nothing -> return $ Right []
                              Just y  -> return $ Right y
          rcv m _ _ = EL.consume >>= \x -> 
                         liftIO $ modifyMVar_ m (\_ -> return x)
          waitForPuller :: MVar [String] -> Int -> IO (Maybe [String])
          waitForPuller m n = do
            x <- modifyMVar m (\s -> return ([], s))
            case x of
              [] -> if n == 0 then return Nothing
                           else do threadDelay 1000
                                   waitForPuller m (n-1)
              xs -> return $ Just xs

  ------------------------------------------------------------------------------
  -- Generic Tests
  ------------------------------------------------------------------------------
  testContext :: Eq a => a -> 
                 (Context -> IO (Either SomeException a)) -> Property
  testContext ss action = monadicIO $ do
    ei <- run $ withContext 1 action 
    case ei of
      Left  e -> run (putStrLn $ show e) >> assert False
      Right x -> assert (x == ss)
      
  ------------------------------------------------------------------------------
  -- Generic Server Tests
  ------------------------------------------------------------------------------
  testServer :: Z.Context -> [String] -> (Service -> IO a) -> IO a
  testServer ctx ss action =
    withServer ctx "Test" noparam 1
                   (Address "inproc://srv" []) Bind 
                   inString outString onErr
                   (\_ -> one "") -- ignore
                   (\_ _ _ -> mkStream ss) action

  trycon :: Z.Socket a -> String -> IO ()
  trycon s a = do ei <- try $ Z.connect s a
                  case ei of
                    Left  e -> do threadDelay 1000
                                  let _ = show (e::SomeException)
                                  trycon s a
                    Right _ -> return ()

  ------------------------------------------------------------------------
  -- stream from list
  ------------------------------------------------------------------------
  mkStream :: [a] -> E.Enumerator a IO b
  mkStream ss step = 
    case step of
      (E.Continue k) -> 
        if null ss then E.continue k
          else mkStream (tail ss) $$ k (E.Chunks [head ss])
      _ -> E.returnI step  

  ------------------------------------------------------------------------------
  -- return a list in an MVar
  ------------------------------------------------------------------------------
  dump :: MVar [a] -> Dump a
  dump m _ _ = EL.consume >>= liftIO . (putMVar m) 

  -------------------------------------------------------------
  -- controlled quickcheck, arbitrary tests
  -------------------------------------------------------------
  deepCheck :: (Testable p) => p -> IO Result
  deepCheck = quickCheckWithResult stdArgs{maxSuccess=100,
                                           maxDiscard=500}

  -------------------------------------------------------------
  -- do just one test
  -------------------------------------------------------------
  oneCheck :: (Testable p) => p -> IO Result
  oneCheck = quickCheckWithResult stdArgs{maxSuccess=1,
                                          maxDiscard=1}

  -------------------------------------------------------------
  -- combinator, could be a monad...
  -------------------------------------------------------------
  applyTest :: IO Result -> IO Result -> IO Result
  applyTest r f = do
    r' <- r
    case r' of
      Success _ -> f
      x         -> return x

  infixr ?>
  (?>) :: IO Result -> IO Result -> IO Result
  (?>) = applyTest

  -------------------------------------------------------------
  -- Name tests
  -------------------------------------------------------------
  runTest :: String -> IO Result -> IO Result
  runTest s t = putStrLn ("Test: " ++ s) >> t

  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    putStrLn "========================================="
    putStrLn "       Patterns Library Test Suite"
    putStrLn "             Basic Services"
    putStrLn "========================================="
    r <- runTest "Simple synchronous request"
                  (deepCheck prp_synReq)           ?>    
         runTest "Simple asynchronous request" 
                  (deepCheck prp_asynReq)          ?> 
         runTest "ByteString Request"
                  (deepCheck prp_bstrReq)          ?>
         runTest "UTF8 Request"
                  (oneCheck prp_utf8Req)           ?> 
         runTest "Connected Server"
                  (deepCheck prp_conSrv)           ?> 
         runTest "Issue withSub"
                  (deepCheck prp_IssueSub)         ?> 
         runTest "Issue Wait"
                  (deepCheck prp_IssueWait)        ?> 
         runTest "Issue Check "
                  (deepCheck prp_IssueCheck)       ?> 
         runTest "Periodic"
                  (deepCheck prp_Periodic)         ?> 
         runTest "Periodicity"
                  (oneCheck prp_Periodicity)       ?> 
         runTest "Resubscribe"
                  (deepCheck prp_Resubscribe)      ?>
         runTest "Subscribe Non-String"
                  (deepCheck prp_subNonString)     ?> 
         runTest "Pipe"
                  (deepCheck prp_pipe)             ?> 
         runTest "Pair"
                  (deepCheck prp_pair)             ?> 
         runTest "Server Threads"
                  (deepCheck prp_threads)          ?>
         runTest "Pull Threads"
                  (deepCheck prp_pullThreads)      ?>
         runTest "On Error Server"
                  (deepCheck prp_onErrSrv)         ?>
         runTest "On Error Publisher"
                  (deepCheck prp_onErrPub)         ?>
         runTest "On Error Subscriber"
                  (deepCheck prp_onErrSub)         ?>
         runTest "On Error Puller"
                  (deepCheck prp_onErrPipe)        ?>
         runTest "Close after Server"
                  (deepCheck prp_closeAfterSrv)    ?>
         runTest "Close after Pub"
                  (deepCheck prp_closeAfterPub)    ?> 
         runTest "Close after Sub"
                  (deepCheck prp_closeAfterSub)    ?> 
         runTest "Close after Puller"
                  (deepCheck prp_closeAfterPuller) ?> 
         runTest "Close after ZMQ"
                  (deepCheck prp_closeAfterZMQ)    ?> 
         runTest "Change Param Server"
                  (deepCheck prp_paramSrv)         ?>
         runTest "Change Param Pub"
                  (deepCheck prp_paramPub)         ?>
         runTest "Change Param Sub"
                  (deepCheck prp_paramSub)         ?>
         runTest "Change Param Pipe"               
                  (deepCheck prp_paramPull)        ?> 
         runTest "Pause Server"               
                  (deepCheck prp_pauseSrv)         ?> 
         runTest "Pause Pub"               
                  (deepCheck prp_pausePub)         ?> 
         runTest "Pause Sub"               
                  (deepCheck prp_pauseSub)         ?>
         runTest "Pause Puller"               
                  (deepCheck prp_pausePull)
        
    case r of
      Success _ -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
