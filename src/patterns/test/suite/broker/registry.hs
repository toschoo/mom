module Main
where

  import           Common
  import           System.Exit
  import           System.Timeout
  import           System.IO (stdout, hFlush)
  import qualified System.ZMQ as Z
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic

  import           Registry -- <--- SUT

  -- import           Network.Mom.Patterns
  import           Data.List (nub, delete, sort)
  import qualified Data.ByteString.Char8 as B
  import           Data.Time.Clock
  import           Data.Either
  import           Data.Maybe
  import qualified Data.Sequence as S
  import           Data.Foldable (toList)
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Monad
  import           Control.Monad.Trans (liftIO)
  import           Control.Exception (throwIO, AssertionFailed(..), SomeException)

  prpInsertFreeQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpInsertFreeQ ns = let is = nub $ nonemptyString ns
                       in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    let ws = map (makeW mq) is
    f <- run $ modifyMVar mq $ \q -> do
           let q' = mapQ ws q insertQ
           returnFree q'
    assert (f == nub is)

  prpRemoveAllQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpRemoveAllQ ns = let is = nub $ nonemptyString ns 
                      in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    let ws = map (makeW mq) is
    f <- run $ modifyMVar mq $ \q -> do
           let q1 = mapQ ws q  insertQ
           let q2 = mapQ ws q1 (removeQ . fst)
           returnFree q2
    assert (null f)

  prpRemoveQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpRemoveQ ns = let is = nub $ nonemptyString ns 
                   in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    let ws = map (makeW mq) is
    ix <- pick $ choose (0, (length is) - 1)
    f <- run $ modifyMVar mq $ \q -> do
           let q1 = mapQ ws q insertQ
           let w  = B.pack $ is!!ix
           let q2 = removeQ w q1
           returnFree q2
    assert (f == delete (is!!ix) is)

  prpSetStateFree2FreeQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpSetStateFree2FreeQ ns = let is = nub $ nonemptyString ns
                              in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    let ws = map (makeW mq) is
    f <- run $ modifyMVar mq $ \q -> do
            let q1 = mapQ ws q insertQ
            let q2 = mapQ ws q1 (\w q -> setStateQ (fst w) Free q)
            returnFree q2
    assert (f == is)

  prpSetStateFree2BusyQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpSetStateFree2BusyQ ns = let is = nub $ nonemptyString ns
                              in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    let ws = map (makeW mq) is
    (f, b) <- run $ modifyMVar mq $ \q -> do
                      let q1 = mapQ ws q insertQ
                      let q2 = mapQ ws q1 (\w q -> setStateQ (fst w) Busy q)
                      returnBoth q2
    assert (null f && b == is)

  prpSetStateBusy2FreeQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpSetStateBusy2FreeQ ns = let is = nub $ nonemptyString ns
                              in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    let ws = map (makeW mq) is
    (f, b) <- run $ modifyMVar mq $ \q -> do
                      let q1 = mapQ ws q insertQ
                      let q2 = mapQ ws q1 (\w qx -> setStateQ (fst w) Busy qx)
                      let q3 = mapQ ws q2 (\w qx -> setStateQ (fst w) Free qx)
                      returnBoth q3
    assert (null b && f == is)

  prpSetStateBusy2BusyQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpSetStateBusy2BusyQ ns = let is = nub $ nonemptyString ns
                              in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    let ws = map (makeW mq) is
    (f, b) <- run $ modifyMVar mq $ \q -> do
                      let q1 = mapQ ws q insertQ
                      let q2 = mapQ ws q1 (\w qx -> setStateQ (fst w) Busy qx)
                      let q3 = mapQ ws q2 (\w qx -> setStateQ (fst w) Busy qx)
                      returnBoth q3
    assert (null f && b == is)

  prpSetState1Free2BusyQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpSetState1Free2BusyQ ns = let is = nub $ nonemptyString ns
                              in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    let ws = map (makeW mq) is
    ix <- pick $ choose (0, length is - 1)
    (f, b) <- run $ modifyMVar mq $ \q -> do
                      let w  = B.pack $ is!!ix
                      let q1 = mapQ ws q insertQ
                      let q2 = setStateQ w Busy q1
                      returnBoth q2
    assert (f == delete (is!!ix) is && b == [is!!ix])

  prpFirstFreeQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpFirstFreeQ ns = let is = nub $ nonemptyString ns
                      in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    let ws = map (makeW mq) is
    ix <- pick $ choose (0, length is - 1)
    (f, _, w) <- run $ modifyMVar mq $ \q -> do
                      let q1 = mapQ ws q insertQ
                      let w  = firstFreeQ q1
                      returnBothMore q1 w
    case w of
      Nothing -> assert False
      Just x  -> assert (f == is && (B.unpack .fst) x == head is)

  prpFirstBusyQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpFirstBusyQ ns = let is = nub $ nonemptyString ns
                      in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    let ws = map (makeW mq) is
    ix <- pick $ choose (0, length is - 1)
    (_, b, w) <- run $ modifyMVar mq $ \q -> do
                      let q1 = mapQ ws q insertQ
                      let q2 = mapQ ws q1 (\w qx -> setStateQ (fst w) Busy qx)
                      let w  = firstBusyQ q2
                      returnBothMore q2 w
    case w of
      Nothing -> assert False
      Just x  -> assert (b == is && (B.unpack .fst) x == head is)

  prpInsertOne :: NonEmptyList Char -> Property
  prpInsertOne (NonEmpty is) = monadicIO $ do
    run $ clean
    run $ insert (B.pack is) (B.pack "Test")
    mbW <- run $ getWorker   (B.pack "Test") 
    case mbW of
      Nothing -> assert False
      Just w  -> assert (B.unpack w == is)

  prpInsertSize :: NonEmptyList (NonEmptyList Char) -> Property
  prpInsertSize ns = let is = nub $ nonemptyString ns
                     in monadicIO $ do
    run $ clean
    run $ mapM_ (\i -> insert (B.pack i) $ B.pack "Test") is
    n <- run size
    assert (n == length is) 

  prpInsertAll :: NonEmptyList (NonEmptyList Char) -> Property
  prpInsertAll ns = let is = nub $ nonemptyString ns
                     in monadicIO $ do
    run $ clean
    run $ mapM_ (\i -> insert (B.pack i) $ B.pack "Test") is
    ws <- run $ (map B.unpack . catMaybes) <$> 
                mapM (\_ -> getWorker $ B.pack "Test") is
    assert (ws == is)

  prpStatsPerService :: NonEmptyList (NonEmptyList Char) -> Property
  prpStatsPerService ns = let is = nub $ nonemptyString ns
                     in monadicIO $ do
    run $ clean
    run $ mapM_ (\i -> insert (B.pack i) $ B.pack "Test") is
    n <- pick $ choose (1, length is)
    _ <- run  $ mapM (\_ -> getWorker $ B.pack "Test") [1..n]
    sAll <- run stat
    sSrv <- run $ statPerService $ B.pack "Test"
    assert (not (null sAll)   &&
            length sAll == 1  &&
            head sAll == sSrv && 
            sSrv == (B.pack "Test", length is - n, n)) 

  returnFree :: Queue -> IO (Queue, [String])
  returnFree q = return (q, map (B.unpack . fst) $ toList $ qFree q)

  returnBusy :: Queue -> IO (Queue, [String])
  returnBusy q = return (q, map (B.unpack . fst) $ toList $ qBusy q)

  returnBoth :: Queue -> IO (Queue, ([String], [String]))
  returnBoth q = return (q, (map (B.unpack . fst) $ toList $ qFree q,
                             map (B.unpack . fst) $ toList $ qBusy q))

  returnBothMore :: Queue -> a -> 
                    IO (Queue, ([String], [String], a))
  returnBothMore q w = do (q, (f,b)) <- returnBoth q
                          return (q, (f,b,w))

  testQ :: NonEmptyList (NonEmptyList Char) -> Bool ->
           ([String] -> Queue -> IO (Queue, [String]))  -> Property
  testQ ns assertion act = let is = nub $ nonemptyString ns 
                            in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    let ws = map (makeW mq) is
    r <- run $ modifyMVar mq (act is)
    assert assertion

  nonemptyString :: NonEmptyList (NonEmptyList Char) -> [String]
  nonemptyString (NonEmpty ns) = map (\(NonEmpty c) -> c) ns
    
  mapQ :: [WrkNode] -> Queue -> (WrkNode -> Queue -> Queue) -> Queue
  mapQ []     q _ = q
  mapQ (w:ws) q f = mapQ ws (f w q) f

  makeW :: MVar Queue -> String -> WrkNode
  makeW mq s = let i = B.pack s
                in (i, Worker {
                         wrkId    = i,
                         wrkState = Free,
                         wrkQ     = mq})

  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    putStrLn "========================================="
    putStrLn "       Patterns Library Test Suite"
    putStrLn "            Broker - Registry"
    putStrLn "========================================="
    r <- runTest "All inserted nodes are in qFree"
                  (deepCheck prpInsertFreeQ)           ?>  
         runTest "Remove all"
                  (deepCheck prpRemoveAllQ)            ?>    
         runTest "Remove one"
                  (deepCheck prpRemoveQ)               ?>    
         runTest "Set State, all Free to Free"
                  (deepCheck prpSetStateFree2FreeQ)    ?>
         runTest "Set State, all Free to Busy"
                  (deepCheck prpSetStateFree2BusyQ)    ?>
         runTest "Set State, all Busy to Free"
                  (deepCheck prpSetStateBusy2FreeQ)    ?>
         runTest "Set State, all Busy to Busy"
                  (deepCheck prpSetStateBusy2BusyQ)    ?>
         runTest "Set State, one Free to Busy"
                  (deepCheck prpSetState1Free2BusyQ)   ?>
         runTest "First Free"
                  (deepCheck prpFirstFreeQ)            ?>
         runTest "First Busy"
                  (deepCheck prpFirstBusyQ)            ?>  
         runTest "Insert One"
                  (deepCheck prpInsertOne)             ?>
         runTest "Insert with count (same Service)"
                  (deepCheck prpInsertSize)            ?>
         runTest "Insert all (same Service)"
                  (deepCheck prpInsertAll)             ?> 
         runTest "Stats"
                  (deepCheck prpStatsPerService)    

    case r of
      Success {} -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
