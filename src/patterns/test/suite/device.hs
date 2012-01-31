module Main
where

  import           Helper
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
  import           Data.List ((\\))
  import qualified Data.ByteString.Char8 as B
  import           Data.Monoid
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Monad
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

  data Legal = Legal Char 
    deriving (Read, Eq, Ord)

  toChar :: Legal -> Char
  toChar (Legal c) = c

  type LStr = [Legal]
  
  strString :: [Legal] -> String
  strString = map toChar

  instance Show Legal where
    show (Legal s) = show s

  instance Arbitrary Legal where
    arbitrary = Legal <$> elements ['a'..'z']

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
      xs -> assert $ (take 4 ss == init l) && (last l == "END")

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
                  

  ------------------------------------------------------------------------------
  -- Server Test
  ------------------------------------------------------------------------------

  ------------------------------------------------------------------------------
  -- Store previous value in MVar, increment to bound
  ------------------------------------------------------------------------------
  listup :: MVar Int -> Int -> IO (Maybe Int)
  listup m e = do
    c <- modifyMVar m (\i -> return (i + 1, i))
    if c >= e then return Nothing else return $ Just c 

  ------------------------------------------------------------------------
  -- Return counter
  ------------------------------------------------------------------------
  listupFor :: Int -> Int -> IO Int
  listupFor c _ = return c

  ------------------------------------------------------------------------
  -- increment counter
  ------------------------------------------------------------------------
  listup' :: Int -> IO Int
  listup' i = return (i+1)

  ------------------------------------------------------------------------
  -- listup for fetch
  ------------------------------------------------------------------------
  fetchup :: MVar Int -> Context -> String -> Int -> IO (Maybe Int)
  fetchup m _ _ i = listup m i

  ------------------------------------------------------------------------
  -- listupFor for fetch
  ------------------------------------------------------------------------
  fetchupFor :: Context -> String -> Int -> Int -> IO Int
  fetchupFor _ _ c i = listupFor c i

  ------------------------------------------------------------------------
  -- listup' for fetch
  ------------------------------------------------------------------------
  fetchup' :: Context -> String -> Int -> IO Int
  fetchup' _ _ = listup'

  ------------------------------------------------------------------------
  -- Make Int stream [0..i]
  ------------------------------------------------------------------------
  mkIntStream :: Int -> E.Enumerator Int IO a
  mkIntStream = go 0
    where go c i step = 
            case step of
              (E.Continue k) -> 
                if c < i then go (c+1) i $$ k (E.Chunks [c])
                  else E.continue k
              _ -> E.returnI step  

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
    where go mbo str _ = do
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
    where go c mbo s os = 
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
    where go c mbo s os = 
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
    where go c mbo s os = 
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
    where go c mbo s os = 
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
    where go c mbo s os = 
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
                  (deepCheck prp_passAll)  ?>
         runTest "emit" (deepCheck prp_emit) ?>
         runTest "emitPart" (deepCheck prp_emitPart) ?>
         runTest "End" (deepCheck prp_end) ?>
         runTest "absorb" (deepCheck prp_absorb) ?>
         runTest "merge" (deepCheck prp_merge) ?>
         runTest "Device works with Sockets"
                  (deepCheck prp_deviceWithSocks) ?>
         runTest "Queue" (deepCheck prp_Queue)    ?>
         runTest "Forward" (deepCheck prp_Forwarder)  ?>
         runTest "Pipeline" (deepCheck prp_Pipeline) 
    case r of
      Success _ -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
