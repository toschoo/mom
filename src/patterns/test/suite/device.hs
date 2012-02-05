module Main
where

  import           Helper
  import           System.IO
  import           System.Exit
  import           System.Timeout
  import qualified System.ZMQ as Z
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import           Network.Mom.Patterns
  import qualified Data.Enumerator      as E
  import qualified Data.Enumerator.List as EL
  import           Data.Enumerator (($$))
  import qualified Data.Sequence as S
  import           Data.Sequence ((|>))
  import qualified Data.ByteString.Char8 as B
  import           Data.Time.Clock
  import           Data.Monoid
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Monad.Trans (liftIO)
  import           Control.Exception (try, throwIO, AssertionFailed(..), SomeException)

  ------------------------------------------------------------------------
  -- For debugging it's much nicer to work with digits
  ------------------------------------------------------------------------
  data Digit = Digit Int
    deriving (Read, Eq, Ord)

  instance Show Digit where
    show (Digit d) = show d

  instance Arbitrary Digit where
    arbitrary = Digit <$> elements [0..9]

  ------------------------------------------------------------------------------
  -- pass all gets all
  ------------------------------------------------------------------------------
  prp_passAll :: NonEmptyList String -> Property
  prp_passAll (NonEmpty ss) = {- collect (length ls) $ -} monadicIO $ do 
    -- let ss = map strString ls -- ["k", "hello",  "world", ",", ""] -- map strString ls
    l  <- run $ testDevice passall ss -- ["hello", "world"]
    assert $ ss == l

  ------------------------------------------------------------------------------
  -- end ends with the sent message
  ------------------------------------------------------------------------------
  prp_end :: NonEmptyList String -> Property
  prp_end (NonEmpty ss) = {- collect (length ls) $ -} monadicIO $ do
     ll <- run $ testDevice (endOn 0) ss
     case ll of 
       [l] -> assert $ head ss == l
       _   -> assert False

  ------------------------------------------------------------------------------
  -- emit emits all
  ------------------------------------------------------------------------------
  prp_emit :: NonEmptyList String -> Property
  prp_emit (NonEmpty ss) = {- collect (length ls) $ -} monadicIO $ do 
    l  <- run $ testDevice (emitOn 3) ss
    -- run $ putStrLn $ "expected: " ++ (show $ take 4 ss) ++ " got: " ++ show l
    assert $ take 4 ss == l

  ------------------------------------------------------------------------------
  -- emitPart emits all + end
  ------------------------------------------------------------------------------
  prp_emitPart :: NonEmptyList String -> Property
  prp_emitPart (NonEmpty ss) = {- collect (length ls) $ -} monadicIO $ do 
    l  <- run $ testDevice (emitPOn 3 "END") ss
    -- run $ putStrLn $ "expected: " ++ (show $ take 4 ss) ++ " got: " ++ show l
    case l of
      [] -> assert False
      _  -> assert $ (take 4 ss == init l) && (last l == "END")

  ------------------------------------------------------------------------------
  -- absorb until
  ------------------------------------------------------------------------------
  prp_absorb :: NonEmptyList String -> Property
  prp_absorb (NonEmpty ss) = {- collect (length ls) $ -} monadicIO $ do 
    l  <- run $ testDevice (absorbUntil 3) ss
    -- run $ putStrLn $ "expected: " ++ (show $ take 4 ss) ++ " got: " ++ show l
    assert $ take 4 ss == l

  ------------------------------------------------------------------------------
  -- merge until
  ------------------------------------------------------------------------------
  prp_merge :: NonEmptyList String -> Property
  prp_merge (NonEmpty ss) = {- collect (length ls) $ -} monadicIO $ do 
    l  <- run $ testDevice (mergeUntil 3) ss
    if null l then assert False
      else assert $ concat (take 4 ss) == head l

  ------------------------------------------------------------------------------
  -- Test timeout
  ------------------------------------------------------------------------------
  prp_onTmo :: Property
  prp_onTmo = monadicIO $ run (withContext 1 tstDevice) >>= assert
    where go d m _ = do
            now <- getCurrentTime
            threadDelay $ 2 * (fromIntegral d)
            t   <- readMVar m
            if t > now && t <= uToNominal (3 * (fromIntegral d)) 
                               `addUTCTime` now
              then putStr "." >> hFlush stdout >> return True
              else return False
          uToNominal :: Int -> NominalDiffTime 
          uToNominal t = fromIntegral t / (1000000::NominalDiffTime)
          tstTmo m = do
            now <- getCurrentTime
            modifyMVar_ m (\_ -> return now)
          tstDevice ctx = 
            withPub ctx (Address "inproc://pub" []) idOut $ \_ -> do
              let d = 10000
              t     <- getCurrentTime
              tmo   <- newMVar t
              withDevice ctx "Test" noparam d
                             [pollEntry "XSub" XSub 
                                  (Address "inproc://pub" []) Connect "",
                              pollEntry "XPub" XPub
                                  (Address "inproc://sub" []) Bind ""]
                              idIn idOut onErr_ 
                              (\_ -> tstTmo tmo) (\_ -> putThrough) $ \dv -> do
                  (and <$> mapM (go d tmo) ([1..100]::[Int])) ~> (do
                    let d' = 2 * d
                    putStrLn "\nchange Timeout"
                    changeTimeout dv d'
                    ok <- and <$> mapM (go d' tmo) ([1..100]::[Int])
                    putStrLn ""
                    return ok)

  ------------------------------------------------------------------------------
  -- Error
  ------------------------------------------------------------------------------
  prp_onErr :: Property
  prp_onErr = monadicIO $ run (withContext 1 tstDevice) >>= assert
    where mkErr           = throwIO (AssertionFailed "Test")
          onerr m c _ _ _ = putMVar m c
          tstFatal m _    = do
            e <- takeMVar m
            case e of
              Fatal -> return True
              _     -> return False
          tstError p m _  = do
            issue p (mkStream [B.pack "test"])
            e <- takeMVar m
            case e of
              Error -> return True
              _     -> return False
          tstDevice ctx   = 
            withPub ctx (Address "inproc://pub" []) idOut $ \p -> do
              errDevice ctx p mkErr tstFatal ~> (do
                threadDelay 10000 >> -- give the ZMQ some time
                  errDevice ctx p (return ()) (tstError p))
          errDevice ctx _ tmo action = do
              e <- newEmptyMVar
              withDevice ctx "Test" noparam 10000
                             [pollEntry "XSub" XSub 
                                  (Address "inproc://pub" []) Connect "",
                              pollEntry "XPub" XPub
                                  (Address "inproc://sub" []) Bind ""]
                              idIn idOut (onerr e)
                              (\_ -> tmo) (\_ _ _ -> tryIO mkErr) (action e)

  ------------------------------------------------------------------------------
  -- Param
  ------------------------------------------------------------------------------
  prp_Param :: String -> Property
  prp_Param s = monadicIO $ run (withContext 1 tstDevice) >>= assert
    where tstDevice ctx   = 
            withPub ctx (Address "inproc://pub" []) idOut $ \pub -> do
              m <- newEmptyMVar
              withDevice ctx "Test" noparam (-1)
                             [pollEntry "XSub" XSub 
                                  (Address "inproc://pub" []) Connect "",
                              pollEntry "XPub" XPub
                                  (Address "inproc://sub" []) Bind ""]
                              idIn idOut onErr_
                              (\_ -> return ()) 
                              (\p _ _ -> EL.consume >>= \_ -> liftIO $
                                           putMVar m p) $ \dv -> do
                  issue pub (mkStream [B.pack "test"])
                  x <- takeMVar m
                  case x of
                    "" -> do
                      changeParam dv s
                      issue pub (mkStream [B.pack "test"])
                      y <- takeMVar m 
                      return (y == s)
                    _ -> return False

  ------------------------------------------------------------------------------
  -- start / pause
  ------------------------------------------------------------------------------
  prp_Pause :: NonEmptyList String -> Property
  prp_Pause (NonEmpty ss) = monadicIO $ run (withContext 1 tstDevice) >>= assert
    where tstDevice ctx   = 
            withPub ctx (Address "inproc://pub" []) outString $ \pub -> do
              m <- newEmptyMVar
              withDevice ctx "Test" noparam (-1)
                             [pollEntry "XSub" XSub 
                                  (Address "inproc://pub" []) Connect "",
                              pollEntry "XPub" XPub
                                  (Address "inproc://sub" []) Bind ""]
                              inString outString onErr_
                              (\_ -> return()) 
                              (\_ -> passall) $ \dv ->
                withSub ctx "Sub" noparam "" (Address "inproc://sub" []) 
                      inString onErr_ (dump m) $ \_ -> do
                  issue pub (mkStream ss)
                  mbx <- timeout 50000 $ takeMVar m
                  case mbx of
                    Nothing -> return False
                    Just x  -> 
                      if x /= ss then 
                          putStrLn ("received: " ++ show x) >> return False
                        else do
                          pause dv
                          issue pub (mkStream ss)
                          y <- timeout 50000 $ takeMVar m
                          case y of
                            Just _  -> return False
                            Nothing -> do
                              resume dv
                              issue pub (mkStream ss)
                              mbz <- timeout 50000 $ takeMVar m
                              case mbz of
                                Nothing -> return False
                                Just z  -> return (z == ss)

  ------------------------------------------------------------------------------
  -- device commands
  -- note: we cannot test remove in this manner 
  --       since we use "inproc" sockets, withSub can only connect
  --       to a publisher that has already bound its address;
  --       when we remove this publisher, withSub runs into an error...
  ------------------------------------------------------------------------------
  prp_add :: NonEmptyList String -> Property
  prp_add (NonEmpty ss) = monadicIO $ run (withContext 1 tstDevice) >>= assert
    where tstSub ctx m action =
            withSub ctx "Sub2" noparam ""
                        (Address "inproc://sub2" []) 
                        inString onErr_ (dump m) $ \_ -> action
          tstReceive m mb = do
            mbx <- timeout 50000 $ takeMVar m
            case mbx of
              Nothing -> case mb of
                           Just _  -> putStrLn "Just expected" >> return False
                           Nothing -> return True
              Just x  -> case mb of
                           Nothing -> putStrLn "Nothing expected" >> return False
                           Just _  -> return (x == ss)
          tstDevice ctx   = 
            withPub ctx (Address "inproc://pub" []) outString $ \pub -> do
              m1 <- newEmptyMVar
              m2 <- newEmptyMVar
              withDevice ctx "Test" noparam (-1)
                             [pollEntry "XSub" XSub 
                                  (Address "inproc://pub" []) Connect "",
                              pollEntry "XPub1" XPub
                                  (Address "inproc://sub1" []) Bind ""]
                              inString outString onErr_
                              (\_ -> return())
                              (\_ -> passall) $ \dv -> 
                withSub ctx "Sub1" noparam "" 
                        (Address "inproc://sub1" []) 
                        inString onErr_ (dump m1) $ \_ -> do
                    let pe = pollEntry "XPub2" XPub 
                             (Address "inproc://sub2" []) Bind ""
                    issue pub (mkStream ss)
                    tstReceive m1 (Just ss) ~> (do
                        addDevice dv pe
                        tstSub ctx m2 $ do
                          issue pub (mkStream ss)
                          tstReceive m1 (Just ss) ~>
                            tstReceive m2 (Just ss))

  infixr ~>
  (~>) :: IO Bool -> IO Bool -> IO Bool
  x ~> y = x >>= \r -> if r then y else return False
  
  ------------------------------------------------------------------------------
  -- unicode
  ------------------------------------------------------------------------------

  ------------------------------------------------------------------------------
  -- Device works with sockets
  ------------------------------------------------------------------------------
  prp_deviceWithSocks :: NonEmptyList String -> Property
  prp_deviceWithSocks (NonEmpty ss) = monadicIO $ do
    -- let ss = map strString ls -- ["k", "hello",  "world", ",", ""] -- map strString ls
    l  <- run $ testSocket passall ss 
    assert $ ss == l

  ------------------------------------------------------------------------------
  -- Queue
  ------------------------------------------------------------------------------
  prp_Queue :: NonEmptyList String -> Property
  prp_Queue (NonEmpty ss) = monadicIO $ do
    -- let ss = map strString ls -- ["k", "hello",  "world", ",", ""] -- map strString ls
    eil <- run $ testQueue ss 
    case eil of
      Left  _ -> assert False
      Right l -> assert $ ss == l

  ------------------------------------------------------------------------------
  -- Forwarder
  ------------------------------------------------------------------------------
  prp_Forwarder :: NonEmptyList String -> Property
  prp_Forwarder (NonEmpty ss) = monadicIO $ do
    -- let ss = map strString ls -- ["k", "hello",  "world", ",", ""] -- map strString ls
    l <- run $ testForward ss 
    assert $ ss == l

  ------------------------------------------------------------------------------
  -- Pipeline
  ------------------------------------------------------------------------------
  prp_Pipeline :: NonEmptyList String -> Property
  prp_Pipeline (NonEmpty ss) = monadicIO $ do
    -- let ss = map strString ls -- ["k", "hello",  "world", ",", ""] -- map strString ls
    l <- run $ testPipeline ss 
    assert $ ss == l

  ------------------------------------------------------------------------------
  -- Generic Device Tests
  ------------------------------------------------------------------------------
  testDevice :: Transformer String -> [String] -> IO [String]
  testDevice t ss = withContext 1 $ \ctx -> 
      withPub ctx (Address "inproc://pub" []) outString $ \pub -> 
        withDevice ctx "Test" noparam (-1) 
                       [pollEntry "XSub" XSub 
                            (Address "inproc://pub" []) Connect "",
                        pollEntry "XPub" XPub
                            (Address "inproc://sub" []) Bind ""]
                        inString outString onErr_ 
                        (\_ -> return ()) (\_ -> t) $ \_ -> do 
          m <- newEmptyMVar 
          withSub ctx "Sub" noparam "" (Address "inproc://sub" []) 
                      inString onErr_ (dump m) $ \_ -> do
            issue pub (mkStream ss)
            mbl <- timeout 500000 $ takeMVar m
            case mbl of
               Nothing -> do putStrLn $ "Timeout on " ++ show ss
                             return []
               Just l  -> return l

  ------------------------------------------------------------------------------
  -- Queue
  ------------------------------------------------------------------------------
  testQueue :: [String] -> IO (Either SomeException [String])
  testQueue ss = withContext 1 $ \ctx -> 
      withServer ctx "Server" noparam 128
                     (Address "inproc://srv" []) Bind
                     inString outString onErr
                     (\_ -> EL.consume) 
                     (\_ _ -> mkStream) $ \_ -> 
        withQueue ctx "Test Queue" 
                      (Address "inproc://cli" [], Bind)
                      (Address "inproc://srv" [], Connect) onErr_ $ \_ ->
          withClient ctx (Address "inproc://cli" []) 
                         outString inString $ \c -> 
            request c (mkStream ss) EL.consume

  ------------------------------------------------------------------------------
  -- Forwarder
  ------------------------------------------------------------------------------
  testForward :: [String] -> IO [String]
  testForward ss = withContext 1 $ \ctx -> 
      withPub ctx (Address "inproc://pub" []) outString $ \pub -> 
        withForwarder ctx "Test Forwarder" ""
                      (Address "inproc://pub" [], Connect)
                      (Address "inproc://sub" [], Bind) onErr_ $ \_ -> do
          m <- newEmptyMVar 
          withSub ctx "Sub" noparam "" (Address "inproc://sub" []) 
                      inString onErr_ (dump m) $ \_ -> do
            issue pub (mkStream ss)
            mbl <- timeout 500000 $ takeMVar m
            case mbl of
               Nothing -> do putStrLn $ "Timeout on " ++ show ss
                             return []
               Just l  -> return l

  ------------------------------------------------------------------------------
  -- Pipe
  ------------------------------------------------------------------------------
  testPipeline :: [String] -> IO [String]
  testPipeline ss = withContext 1 $ \ctx -> do
      withPipe ctx (Address "inproc://push" []) outString $ \p ->
        withPipeline ctx "Test Pipeline"
                      (Address "inproc://push" [], Connect)
                      (Address "inproc://pull" [], Bind) onErr_ $ \_ -> do
          m <- newEmptyMVar
          withPuller ctx "Puller" noparam
                     (Address "inproc://pull" []) 
                     inString onErr_ (dump m) $ \_ -> do
            push p (mkStream ss)
            mbl <- timeout 500000 $ takeMVar m
            case mbl of
              Nothing -> do putStrLn $ "Timeout on " ++ show ss
                            return []
              Just l  -> return l

  ------------------------------------------------------------------------------
  -- Device works with Z.Socket
  ------------------------------------------------------------------------------
  testSocket :: Transformer String -> [String] -> IO [String]
  testSocket t ss = withContext 1 $ \ctx -> 
      Z.withSocket ctx Z.Pub $ \pub -> do
        Z.bind pub "inproc://pub"
        withDevice ctx "Test" noparam (-1) 
                       [pollEntry "XSub" XSub 
                            (Address "inproc://pub" []) Connect "",
                        pollEntry "XPub" XPub
                            (Address "inproc://sub" []) Bind ""]
                        inString outString onErr_ 
                        (\_ -> return ()) (\_ -> t) $ \_ -> 
          Z.withSocket ctx Z.Sub $ \sub -> do
            trycon  sub "inproc://pub"
            Z.subscribe sub ""
            mapSend pub ss
            reverse <$> rcv sub 
    where mapSend _ []     = return ()
          mapSend s (x:xs) = let os = if null xs then [] else [Z.SndMore]
                              in Z.send s (B.pack x) os >> mapSend s xs
          rcv s = go []
            where go ls = do
                     x <- B.unpack <$> Z.receive s []
                     m <- Z.moreToReceive s
                     if m 
                       then go (x:ls)
                       else return (x:ls)
 
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
  -- Just build up a list and store it in MVar
  ------------------------------------------------------------------------------
  makeList :: MVar [Int] -> E.Iteratee Int IO ()
  makeList m = do
    mb <- EL.head
    case mb of
      Nothing -> return ()
      Just i  -> tryIO (modifyMVar_ m (\l -> return (i:l))) >> makeList m

  ------------------------------------------------------------------------------
  -- return a list in an MVar
  ------------------------------------------------------------------------------
  dump :: MVar [a] -> Dump a
  dump m _ _ = EL.consume >>= liftIO . (putMVar m) 

  ------------------------------------------------------------------------------
  -- return a list in an MVar
  ------------------------------------------------------------------------------
  passall :: Streamer o -> S.Seq o -> E.Iteratee o IO ()
  passall s os = EL.head >>= \mbo -> go mbo s os
    where go mbo str _ = 
            case mbo of
              Nothing -> return ()
              Just x  -> do
                mbo' <- EL.head
                let lst = case mbo' of 
                            Nothing -> True
                            Just _  -> False
                pass str trg x lst (go mbo')
          trg = filterTargets s (/= getStreamSource s)

  ------------------------------------------------------------------------------
  -- end on i
  ------------------------------------------------------------------------------
  endOn :: Int -> Streamer o -> S.Seq o -> E.Iteratee o IO ()
  endOn i s os = EL.head >>= \mbo -> go 0 mbo s os
    where go c mbo _ _ = 
            case mbo of
              Nothing -> return ()
              Just x  -> 
                 if c >= i then end s trg x
                   else do
                     mbo' <- EL.head
                     let lst = case mbo' of
                                 Nothing -> True
                                 Just _  -> False
                      in pass s trg x lst (go (c+1) mbo')
          trg = filterTargets s (/= getStreamSource s)

  ------------------------------------------------------------------------------
  -- emit on i
  ------------------------------------------------------------------------------
  emitOn :: Int -> Streamer o -> S.Seq o -> E.Iteratee o IO ()
  emitOn i s _ = EL.head >>= \mbo -> go 0 mbo s S.empty
    where go c mbo _ os = 
             case mbo of
               Nothing -> if c > i then return ()
                            else emit s trg os continueHere
               Just x  -> do
                 mbo' <- EL.head
                 if c == i 
                   then emit s trg (os |> x) ignoreStream
                    else go (c+1) mbo' s (os |> x)
          trg = filterTargets s (/= getStreamSource s)

  ------------------------------------------------------------------------------
  -- emitPart on i
  ------------------------------------------------------------------------------
  emitPOn :: Int -> o -> Streamer o -> S.Seq o -> E.Iteratee o IO ()
  emitPOn i e s _ = EL.head >>= \mbo -> go 0 mbo s S.empty
    where go c mbo _ os = 
             case mbo of
               Nothing -> if c > i then end s trg e
                            else emitPart s trg os (sendLast trg)
               Just x  -> do
                 mbo' <- EL.head
                 if c == i 
                   then emitPart s trg  (os |> x) (sendLast trg)
                   else go (c+1) mbo' s (os |> x)
          sendLast t _ _ = end s t e
          trg = filterTargets s (/= getStreamSource s)

  ------------------------------------------------------------------------------
  -- absorb until i
  ------------------------------------------------------------------------------
  absorbUntil :: Int -> Streamer o -> S.Seq o -> E.Iteratee o IO ()
  absorbUntil i s _ = EL.head >>= \mbo -> go 0 mbo s S.empty
    where go c mbo _ os = 
             case mbo of
               Nothing -> if c > i + 1 then return ()
                            else emit s trg os continueHere
               Just x  -> do
                 mbo' <- EL.head
                 if c <= i 
                   then absorb s x   os $ go (c+1) mbo'
                   else emit   s trg os ignoreStream
          trg = filterTargets s (/= getStreamSource s)

  ------------------------------------------------------------------------------
  -- merge until i
  ------------------------------------------------------------------------------
  mergeUntil :: Monoid o => Int -> Streamer o -> S.Seq o -> E.Iteratee o IO ()
  mergeUntil i s _ = EL.head >>= \mbo -> go 0 mbo s S.empty
    where go c mbo _ os = 
             case mbo of
               Nothing -> if c > i + 1 then return ()
                            else emit s trg os continueHere
               Just x  -> do
                 mbo' <- EL.head
                 if c <= i 
                   then merge s x   os $ go (c+1) mbo'
                   else emit  s trg os ignoreStream
          trg = filterTargets s (/= getStreamSource s)
    
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
    r <- runTest "Pass all passes all"
                  (deepCheck prp_passAll)            ?>
         runTest "emit" (deepCheck prp_emit)         ?>
         runTest "emitPart" (deepCheck prp_emitPart) ?>
         runTest "End" (deepCheck prp_end)           ?>
         runTest "absorb" (deepCheck prp_absorb)     ?>
         runTest "merge" (deepCheck prp_merge)       ?>
         runTest "Device works with Sockets"
                  (deepCheck prp_deviceWithSocks)    ?>
         runTest "Queue" (deepCheck prp_Queue)       ?>
         runTest "Forward" (deepCheck prp_Forwarder) ?>
         runTest "Pipeline" (deepCheck prp_Pipeline) ?>
         runTest "Timeout" (oneCheck prp_onTmo)      ?>
         runTest "Error" (deepCheck prp_onErr)        ?> 
         runTest "Parameter" (deepCheck prp_Param)   ?> 
         runTest "Start/Pause" (deepCheck prp_Pause) ?> 
         runTest "add"         (deepCheck prp_add)
    case r of
      Success _ -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
