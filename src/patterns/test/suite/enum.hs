module Main
where

  import           System.Exit
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import           Network.Mom.Patterns
  import qualified Data.Enumerator      as E
  import qualified Data.Enumerator.List as EL
  import           Data.Enumerator (($$))
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Monad.Trans (liftIO)
  import           Control.Exception (throwIO, AssertionFailed(..))
  import           Data.List (intercalate)

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
  -- enumWith stops on Nothing
  ------------------------------------------------------------------------------
  prp_ewStopOnNothing :: Digit -> Property
  prp_ewStopOnNothing (Digit i) = {- collect i $ -} monadicIO $ do
    im <- run $ newMVar 0
    om <- run $ newMVar []
    _  <- run $ E.run_ (enumWith (listup im) i $$ makeList om)
    l  <- run $ readMVar om
    -- run $ putStrLn ("Expected: " ++ show i ++ ", got: " ++ show (reverse l))
    assert $ [0..(i-1)] == reverse l

  ------------------------------------------------------------------------------
  -- enumFor stops on c == e
  ------------------------------------------------------------------------------
  prp_efStopOnEnd :: Digit -> Property
  prp_efStopOnEnd (Digit i) = monadicIO $ do
    om <- run $ newMVar []
    _  <- run $ E.run_ (enumFor listupFor (0, i) 0 $$ makeList om)
    l  <- run $ readMVar om
    -- run $ putStrLn ("Expected: " ++ show [0..(i-1)] ++ ", got: " ++ show (reverse l))
    assert $ [0..(i-1)] == reverse l

  ------------------------------------------------------------------------------
  -- enumFor listupFor == enumWith listup
  ------------------------------------------------------------------------------
  prp_ewef :: Digit -> Property
  prp_ewef (Digit i) = monadicIO $ do
    im <- run $ newMVar 0
    om <- run $ newMVar []
    _  <- run $ E.run_ (enumWith (listup im) i $$ makeList om)
    lw <- run $ modifyMVar om $ \l -> return ([],l)
    _  <- run $ E.run_ (enumFor listupFor (0, i) i $$ makeList om)
    lf <- run $ readMVar om
    assert $ lw == lf

  ------------------------------------------------------------------------------
  -- once returns just one value 
  ------------------------------------------------------------------------------
  prp_onceIsOne :: Digit -> Property
  prp_onceIsOne (Digit i) = monadicIO $ do
    om <- run $ newMVar []
    _  <- run $ E.run_ (once listup' i $$ makeList om)
    l  <- run $ readMVar om
    -- run $ putStrLn ("Expected: " ++ show [0..(i-1)] ++ ", got: " ++ show (reverse l))
    assert $ [i+1] == l

  ------------------------------------------------------------------------------
  -- just returns just the value
  ------------------------------------------------------------------------------
  prp_just :: Digit -> Property
  prp_just (Digit i) = monadicIO $ do
    om <- run $ newMVar []
    _  <- run $ E.run_ (just i $$ makeList om)
    l  <- run $ readMVar om
    assert $ [i] == l

  ------------------------------------------------------------------------------
  -- just = once return
  ------------------------------------------------------------------------------
  prp_justIsOnce :: Digit -> Property
  prp_justIsOnce (Digit i) = monadicIO $ do
    om <- run $ newMVar []
    _  <- run $ E.run_ (once return i $$ makeList om)
    lo <- run $ modifyMVar om $ \l -> return ([],l)
    _  <- run $ E.run_ (just i $$ makeList om)
    lj <- run $ readMVar om
    assert $ lo == lj

  ------------------------------------------------------------------------------
  -- just = enumFor return (0,1) 
  ------------------------------------------------------------------------------
  prp_justIsFor :: Digit -> Property
  prp_justIsFor (Digit i) = monadicIO $ do
    om <- run $ newMVar []
    _  <- run $ E.run_ (enumFor (\_ -> return) (i-1, i) i $$ makeList om)
    lo <- run $ modifyMVar om $ \l -> return ([],l)
    _  <- run $ E.run_ (just i $$ makeList om)
    lj <- run $ readMVar om
    assert $ lo == lj

  ------------------------------------------------------------------------------
  -- enumWith == fetcher
  ------------------------------------------------------------------------------
  prp_fetchIsWith :: Digit -> Property
  prp_fetchIsWith (Digit i) = monadicIO $ do
    im <- run $ newMVar 0
    om <- run $ newMVar []
    _  <- run $ E.run_ (enumWith (listup im) i $$ makeList om)
    le <- run $ modifyMVar om $ \l -> return ([],l)
    run $ modifyMVar_ im $ \_ -> return 0
    _  <- run $ withContext 1 $ \ctx ->
            E.run_ (fetcher (fetchup im) ctx noparam i $$ makeList om)
    lf <- run $ readMVar om
    -- run $ putStrLn ("Expected: " ++ show le ++ ", got: " ++ show lf)
    assert $ le == lf

  ------------------------------------------------------------------------------
  -- enumFor == fetchFor
  ------------------------------------------------------------------------------
  prp_fetchIsFor :: Digit -> Property
  prp_fetchIsFor (Digit i) = monadicIO $ do
    om <- run $ newMVar []
    _  <- run $ E.run_ (enumFor listupFor (0, i) i $$ makeList om)
    le <- run $ modifyMVar om $ \l -> return ([],l)
    _  <- run $ withContext 1 $ \ctx ->
            E.run_ (fetchFor fetchupFor (0, i) ctx noparam i $$ makeList om)
    lf <- run $ readMVar om
    -- run $ putStrLn ("Expected: " ++ show lo ++ ", got: " ++ show lj)
    assert $ le == lf

  ------------------------------------------------------------------------------
  -- once == fetch1
  ------------------------------------------------------------------------------
  prp_fetchIsOnce :: Digit -> Property
  prp_fetchIsOnce (Digit i) = monadicIO $ do
    om <- run $ newMVar []
    _  <- run $ E.run_ (once listup' i $$ makeList om)
    le <- run $ modifyMVar om $ \l -> return ([],l)
    _  <- run $ withContext 1 $ \ctx ->
            E.run_ (fetch1 fetchup' ctx noparam i $$ makeList om)
    lf <- run $ readMVar om
    -- run $ putStrLn ("Expected: " ++ show lo ++ ", got: " ++ show lj)
    assert $ le == lf

  ------------------------------------------------------------------------------
  -- just == fetchJust
  ------------------------------------------------------------------------------
  prp_fetchIsJust :: Digit -> Property
  prp_fetchIsJust (Digit i) = monadicIO $ do
    om <- run $ newMVar []
    _  <- run $ E.run_ (just i $$ makeList om)
    le <- run $ modifyMVar om $ \l -> return ([],l)
    _  <- run $ withContext 1 $ \ctx ->
            E.run_ (fetchJust_ i ctx noparam () $$ makeList om)
    lf <- run $ readMVar om
    -- run $ putStrLn ("Expected: " ++ show lo ++ ", got: " ++ show lj)
    assert $ le == lf

  ------------------------------------------------------------------------------
  -- one returns always one value!
  ------------------------------------------------------------------------------
  prp_oneIsOne :: Digit -> Property
  prp_oneIsOne (Digit i) = monadicIO $ do
    l  <- run $ E.run_ (mkIntStream i $$ one i)
    -- run $ putStrLn ("Expected: " ++ show i ++ ", got: " ++ show (reverse l))
    assert $ l == 0

  ------------------------------------------------------------------------------
  -- mbOne returns Nothing or Just one value!
  ------------------------------------------------------------------------------
  prp_oneIsJustOne :: Digit -> Property
  prp_oneIsJustOne (Digit i) = monadicIO $ do
    mbl  <- run $ E.run_ (mkIntStream i $$ mbOne)
    -- run $ putStrLn ("Expected: " ++ show i ++ ", got: " ++ show (reverse l))
    assert $ case mbl of
               Nothing -> i == 0
               Just l  -> l == 0

  ------------------------------------------------------------------------------
  -- mbOne is head
  ------------------------------------------------------------------------------
  prp_oneIsHead :: [String] -> Property
  prp_oneIsHead ss = monadicIO $ do
    mbl1  <- run $ E.run_ (mkStream ss $$ mbOne)
    mbl2  <- run $ E.run_ (mkStream ss $$ EL.head)
    -- run $ putStrLn ("Expected: " ++ show i ++ ", got: " ++ show (reverse l))
    assert $ mbl1 == mbl2

  ------------------------------------------------------------------------------
  -- toList returns all
  ------------------------------------------------------------------------------
  prp_toListAll :: Digit -> Property
  prp_toListAll (Digit i) = monadicIO $ do
    l  <- run $ E.run_ (mkIntStream i $$ toList)
    -- run $ putStrLn ("Expected: " ++ show i ++ ", got: " ++ show l)
    assert $ [0..i-1] == l

  ------------------------------------------------------------------------------
  -- toList is consume
  ------------------------------------------------------------------------------
  prp_toListIsConsume :: Digit -> Property
  prp_toListIsConsume (Digit i) = monadicIO $ do
    l1  <- run $ E.run_ (mkIntStream i $$ toList)
    l2  <- run $ E.run_ (mkIntStream i $$ EL.consume)
    -- run $ putStrLn ("Expected: " ++ show i ++ ", got: " ++ show l)
    assert $ l1 == l2

  ------------------------------------------------------------------------------
  -- toString 
  ------------------------------------------------------------------------------
  prp_toString :: [String] -> Property
  prp_toString ss = monadicIO $ do
    l <- run $ E.run_ (mkStream ss $$ toString ":") 
    -- run $ putStrLn ("Expected: " ++ show l2 ++ 
    --                 ", got: " ++ show l1)
    assert $ intercalate ":" ss == l

  ------------------------------------------------------------------------------
  -- append
  ------------------------------------------------------------------------------
  prp_append :: [String] -> Property
  prp_append ss = monadicIO $ do
    l <- run $ E.run_ (mkStream ss $$ append)
    assert $ concat ss == l 

  ------------------------------------------------------------------------------
  -- store
  ------------------------------------------------------------------------------
  prp_store :: [String] -> Property
  prp_store ss = monadicIO $ do
    m <- run $ newMVar []
    run $ E.run_ (mkStream ss $$ store (sv m)) 
    l <- run $ readMVar m
    assert $ ss == reverse l 
    where sv m i = modifyMVar_ m (\is -> return (i:is)) 

  ------------------------------------------------------------------------------
  -- sink
  ------------------------------------------------------------------------------
  prp_sink :: [String] -> Property
  prp_sink ss = monadicIO $ do
    m <- run newEmptyMVar
    run $ withContext 1 $ \ctx ->
          E.run_ (mkStream ss $$ sink (op m) cl sv ctx noparam) 
    l <- run $ readMVar m
    assert $ ss == l 
    where op m _ _   = putMVar m [] >> return m
          cl _ _ m   = modifyMVar_ m (return . reverse)
          sv _ _ m i = modifyMVar_ m (\is -> return (i:is)) 

  ------------------------------------------------------------------------------
  -- sink with Error
  ------------------------------------------------------------------------------
  prp_sinkErr :: [String] -> Property
  prp_sinkErr ss = monadicIO $ do
    m <- run newEmptyMVar
    _ <- run $ withContext 1 $ \ctx ->
               E.run (mkStream ss $$ sink (op m) cl sv ctx noparam)
    run (isEmptyMVar m) >>= assert
    where op m _ _   = putMVar m [] >> return m
          cl _ _ m   = takeMVar m >>= \_ -> return ()
          sv _ _ m i = modifyMVar_ m (\is ->
                         if length is > 1
                           then throwIO $ AssertionFailed "Test"
                           else return (i:is))

  ------------------------------------------------------------------------------
  -- sinkI
  ------------------------------------------------------------------------------
  prp_sinkI :: [String] -> Property
  prp_sinkI ss = monadicIO $ do
    m <- run newEmptyMVar
    run $ withContext 1 $ \ctx ->
          E.run_ (mkStream ss $$ sinkI (op m) cl sv ctx noparam) 
    e <- run $ isEmptyMVar m
    l <- if e then return [] else run $ readMVar m
    assert $ ss == l 
    where op m _ _ i = putMVar m [i] >> return m
          cl _ _ m   = modifyMVar_ m (return . reverse)
          sv _ _ m i = modifyMVar_ m (\is -> return (i:is)) 

  ------------------------------------------------------------------------------
  -- sinkI with Error
  ------------------------------------------------------------------------------
  prp_sinkIErr :: [String] -> Property
  prp_sinkIErr ss = monadicIO $ do
    m <- run newEmptyMVar
    _ <- run $ withContext 1 $ \ctx ->
                E.run (mkStream ss $$ sinkI (op m) cl sv ctx noparam)
    run (isEmptyMVar m) >>= assert
    where op m _ _ i = putMVar m [i] >> return m
          cl _ _ m   = takeMVar m >>= \_ -> return ()
          sv _ _ m i = modifyMVar_ m (\is -> 
                         if length is > 1
                           then throwIO $ AssertionFailed "Test"
                           else return (i:is))

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
    r <- runTest "enumWith stops on Nothing"
                  (deepCheck prp_ewStopOnNothing) ?>
         runTest "enumFor stops on last index"
                 (deepCheck prp_efStopOnEnd)      ?>
         runTest "enumWith implements enumFor" 
                 (deepCheck prp_ewef)             ?>
         runTest "once returns a single value"
                 (deepCheck prp_onceIsOne)        ?>
         runTest "just returns just the value"
                 (deepCheck prp_just)             ?>
         runTest "just is once"
                 (deepCheck prp_justIsOnce)       ?>
         runTest "just as For"
                 (deepCheck prp_justIsFor)        ?>
         runTest "fetch as enumWith"
                 (deepCheck prp_fetchIsWith)      ?>
         runTest "fetch as enumFor"
                 (deepCheck prp_fetchIsFor)       ?>
         runTest "fetch as once"
                 (deepCheck prp_fetchIsOnce)      ?>    
         runTest "fetch as just"
                 (deepCheck prp_fetchIsJust)      ?>
         runTest "one returns one value"
                 (deepCheck prp_oneIsOne)         ?>
         runTest "one as EL.head"
                 (deepCheck prp_oneIsHead)        ?>
         runTest "one as just"
                 (deepCheck prp_oneIsJustOne)     ?>
         runTest "toList gets all"
                 (deepCheck prp_toListAll)        ?>
         runTest "toList is consume"
                 (deepCheck prp_toListIsConsume)  ?>
         runTest "toString is intercalate"
                 (deepCheck prp_toString)         ?>
         runTest "append is concat"
                 (deepCheck prp_append)           ?>
         runTest "store gets all"
                 (deepCheck prp_store)            ?>
         runTest "sink"
                 (deepCheck prp_sink)             ?>
         runTest "Source is closed on Error (sink)"
                 (deepCheck prp_sinkErr)          ?>
         runTest "sinkI"
                 (deepCheck prp_sinkI)            ?>
         runTest "Source is closed on Error (sinkI)"
                 (deepCheck prp_sinkIErr)
    case r of
      Success _ -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
