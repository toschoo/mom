module Main
where

  import           System.Exit
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import           Common

  import           Registry  -- <--- SUT
  import           Types 

  import           Network.Mom.Stompl.Client.Queue

  import           Data.List (find, nub, delete, sort, partition)
  import qualified Data.ByteString.Char8 as B
  import           Data.Maybe
  import qualified Data.Sequence as S
  import           Data.Foldable (toList)
  import           Data.Time.Clock
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Monad (void,when)

  prpInsertR :: NonEmptyList (NonEmptyList Char) -> Property
  prpInsertR ns = let is = nub $ nonemptyString ns
                   in monadicIO $ do
    s <- run $ testWithReg $ \r -> do
      mapM_ (\q -> insertR r "service1" Service q 0) is
      getProvider r "service1" $ length is
    let qs = map prvQ s
    assert (sort qs == sort is)

  prpGet1 :: NonEmptyList (NonEmptyList Char) -> Property
  prpGet1 ns = let is = nub $ nonemptyString ns
                in monadicIO $ do
    q <- run $ testWithReg $ \r -> do
      mapM_ (\q -> insertR r "service1" Service q 0) is
      getProvider r "service1" 1
    assert (not  (null q) &&
            prvQ (head q) == head is)

  prpGet1NTimes :: NonEmptyList (NonEmptyList Char) -> Property
  prpGet1NTimes ns = let is = nub $ nonemptyString ns
                      in monadicIO $ do
    q <- run $ testWithReg $ \r -> do
      mapM_ (\q -> insertR r "service1" Service q 0) is
      void $ getProvider r "service1" (length is - 1)
      getProvider r "service1" 1
    assert (not  (null q) &&
            prvQ (head q) == last is)

  prpGetN :: NonEmptyList (NonEmptyList Char) -> Property
  prpGetN ns = let is = nub $ nonemptyString ns
                in monadicIO $ do
    n <- pick $ choose (1,length is)
    q <- run $ testWithReg $ \r -> do
      mapM_ (\q -> insertR r "service1" Service q 0) is
      getProvider r "service1" n
    assert (not  (null q) &&
            length q == n &&
            map prvQ q == take n is)

  prpGetNxN :: NonEmptyList (NonEmptyList Char) -> Property
  prpGetNxN ns = let is = nub $ nonemptyString ns
                  in monadicIO $ do
    n1 <- pick $ choose (0,if length is > 1 then length is - 1 else 0) 
    n2 <- pick $ choose (if n1 == 0 then 1 else 1,length is-n1)
    q <- run $ testWithReg $ \r -> do
      mapM_ (\q -> insertR r "service1" Service q 0) is
      when (n1 > 0) $ void $ getProvider r "service1" n1
      getProvider r "service1" n2
    assert (not  (null q)  &&   
            length q == n2 &&
            map prvQ q == take n2 (drop n1 is))

  prpUpd  :: NonEmptyList (NonEmptyList Char) -> Property
  prpUpd  ns = let is = nub $ nonemptyString ns
                in monadicIO $ do
    n <- pick $ choose (0,length is-1)
    let p = is!!n
    now <- run $ (\now -> timeAdd now (-500)) <$> getCurrentTime
    qs  <- run $ testWithReg $ \r -> do
      mapM_ (\q -> insertR r "service1" Service q 500) is
      setTo r "service1" now
      updR r "service1" p 
      getProvider r "service1" $ length is
    case find (\x -> prvQ x == p) qs of
      Nothing -> assert False
      Just q  -> let h     = head qs
                     (l,r) = partition (\x -> prvNxt x == now) qs
                  in assert (not (null r)  &&
                             length r == 1 &&
                             prvQ (head r) == p) 

  prpMapRService :: NonEmptyList (NonEmptyList Char) -> Property
  prpMapRService ns = let is = nub $ nonemptyString ns
                       in monadicIO $ do
    m <- run $ newMVar []
    i <- pick $ choose (1::Int,2::Int)
    let t = if i == 1 then Service else Task
    run $ testWithReg $ \r -> do
      mapM_ (\q -> insertR r "service1" t q 0) is
      mapM_ (\_ -> mapR r "service1" (act m)) [1..length is]
    qs <- run (reverse <$> readMVar m)
    assert (qs == is)
    where act m p = modifyMVar_ m $ \xs -> return (prvQ p : xs)

  prpMapRSrvOld :: NonEmptyList (NonEmptyList Char) -> Property
  prpMapRSrvOld ns = let is = nub $ nonemptyString ns
                       in monadicIO $ do
    m   <- run $ newMVar []
    now <- run $ (\now -> timeAdd now (-6000)) <$> getCurrentTime
    i   <- pick $ choose (1::Int,2::Int)
    let t = if i == 1 then Service else Task
    run $ testWithReg $ \r -> do
      mapM_ (\q -> insertR r "service1" t q 500) is
      setTo r "service1" now
      mapM_ (\(n,q) -> when (n `rem` 2 /= 0) $ updR r "service1" q) $ 
            zip [1..length is] is
      mapM_ (\_ -> mapR r "service1" (act m)) [1..length is]
    qs <- run (nub . reverse <$> readMVar m)
    assert (qs == del2nd 1 is)
    where act m p = modifyMVar_ m $ \xs -> return (prvQ p : xs)
          del2nd :: Int -> [a] -> [a]
          del2nd _ [] = []
          del2nd i (x:xs) | i `rem` 2 == 0 =     del2nd (i+1) xs
                          | otherwise      = x : del2nd (i+1) xs

  prpMapRTopic :: NonEmptyList (NonEmptyList Char) -> Property
  prpMapRTopic ns = let is = nub $ nonemptyString ns
                       in monadicIO $ do
    m <- run $ newMVar []
    run $ testWithReg $ \r -> do
      mapM_ (\q -> insertR r "service1" Topic q 0) is
      void $ mapR r "service1" (act m)
    qs <- run (reverse <$> readMVar m)
    assert (qs == is)
    where act m p = modifyMVar_ m $ \xs -> return (prvQ p : xs)

  -- register
  -- heartbeat
  -- unregister

  setBack :: Registry -> JobName -> Int -> IO ()
  setBack r jn i = mapAllR r jn (\p -> p{prvNxt = timeAdd (prvNxt p) i})

  setTo :: Registry -> JobName -> UTCTime -> IO ()
  setTo r jn now = mapAllR r jn (\p -> p{prvNxt = now})

  testWithReg :: (Registry -> IO a) -> IO a
  testWithReg action =
    withConnection "localhost" 61613 [] [] $ \c -> do
      withRegistry c "Test-1" "/q/registry1" (0,0) onerr action
                   
  onerr :: OnError
  onerr c e m = putStrLn $ show c ++ " error in " ++ m ++ show e

  nonemptyString :: NonEmptyList (NonEmptyList Char) -> [String]
  nonemptyString (NonEmpty ns) = map (\(NonEmpty c) -> c) ns

  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    putStrLn "========================================="
    putStrLn "   Stompl Patterns Library Test Suite"
    putStrLn "               Registry"
    putStrLn "========================================="
    r <- runTest "Basic insert"
                  (deepCheck prpInsertR)     ?>
         runTest "Get 1"
                  (deepCheck prpGet1)        ?>
         runTest "Get 1 n times"
                  (deepCheck prpGet1NTimes)  ?>
         runTest "Get n"
                  (deepCheck prpGetN)        ?> 
         runTest "Get nxn"
                  (deepCheck prpGetNxN)      ?>
         runTest "Upd"
                  (deepCheck prpUpd)         ?>
         runTest "mapR to services"
                  (deepCheck prpMapRService) ?> 
         runTest "mapR to services, some old"
                  (deepCheck prpMapRSrvOld)  ?>
         runTest "mapR to topic"
                  (deepCheck prpMapRTopic) 

    case r of
      Success {} -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
