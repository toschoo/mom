module Main
where

  import           Common
  import           System.Exit
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic

  import           Registry  -- <--- SUT
  import           Heartbeat -- <--- SUT

  import           Network.Mom.Patterns.Types 

  import           Data.List (nub, delete)
  import qualified Data.ByteString.Char8 as B
  import           Data.Maybe
  import qualified Data.Sequence as S
  import           Data.Foldable (toList)
  import           Control.Applicative ((<$>))
  import           Control.Concurrent

  prpInsertFreeQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpInsertFreeQ ns = let is = nub $ nonemptyString ns
                       in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    ws <- run $ mapM (makeW mq 0) is
    f  <- run $ modifyMVar mq $ \q -> do
           let q' = mapQ ws q insertQ
           returnFree q'
    assert (f == nub is)

  prpRemoveAllQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpRemoveAllQ ns = let is = nub $ nonemptyString ns 
                      in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    ws <- run $ mapM (makeW mq 0) is
    f  <- run $ modifyMVar mq $ \q -> do
           let q1 = mapQ ws q  insertQ
           let q2 = mapQ ws q1 (removeQ . fst)
           returnFree q2
    assert (null f)

  prpRemoveQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpRemoveQ ns = let is = nub $ nonemptyString ns 
                   in monadicIO $ do
    mq <- run  $ newMVar $ Q S.empty S.empty
    ws <- run  $ mapM (makeW mq 0) is
    ix <- pick $ choose (0, length is - 1)
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
    ws <- run $ mapM (makeW mq 0) is
    f <- run $ modifyMVar mq $ \q -> do
            let q1 = mapQ ws q insertQ
            let q2 = mapQ ws q1 (\w k -> setStateQ (fst w) Free id k)
            returnFree q2
    assert (f == is)

  prpSetStateFree2BusyQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpSetStateFree2BusyQ ns = let is = nub $ nonemptyString ns
                              in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    ws <- run $ mapM (makeW mq 0) is
    (f, b) <- run $ modifyMVar mq $ \q -> do
                      let q1 = mapQ ws q insertQ
                      let q2 = mapQ ws q1 (\w k -> setStateQ (fst w) Busy id k)
                      returnBoth q2
    assert (null f && b == is)

  prpSetStateBusy2FreeQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpSetStateBusy2FreeQ ns = let is = nub $ nonemptyString ns
                              in monadicIO $ do
    mq     <- run $ newMVar $ Q S.empty S.empty
    ws     <- run $ mapM (makeW mq 0) is
    (f, b) <- run $ modifyMVar mq $ \q -> do
                      let q1 = mapQ ws q insertQ
                      let q2 = mapQ ws q1 (\w qx -> setStateQ (fst w) Busy id qx)
                      let q3 = mapQ ws q2 (\w qx -> setStateQ (fst w) Free id qx)
                      returnBoth q3
    assert (null b && f == is)

  prpSetStateBusy2BusyQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpSetStateBusy2BusyQ ns = let is = nub $ nonemptyString ns
                              in monadicIO $ do
    mq     <- run $ newMVar $ Q S.empty S.empty
    ws     <- run $ mapM (makeW mq 0) is
    (f, b) <- run $ modifyMVar mq $ \q -> do
                      let q1 = mapQ ws q insertQ
                      let q2 = mapQ ws q1 (\w qx -> setStateQ (fst w) Busy id qx)
                      let q3 = mapQ ws q2 (\w qx -> setStateQ (fst w) Busy id qx)
                      returnBoth q3
    assert (null f && b == is)

  prpSetState1Free2BusyQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpSetState1Free2BusyQ ns = let is = nub $ nonemptyString ns
                              in monadicIO $ do
    mq <- run $ newMVar $ Q S.empty S.empty
    ws <- run $ mapM (makeW mq 0) is
    ix <- pick $ choose (0, length is - 1)
    (f, b) <- run $ modifyMVar mq $ \q -> do
                      let w  = B.pack $ is!!ix
                      let q1 = mapQ ws q insertQ
                      let q2 = setStateQ w Busy id q1
                      returnBoth q2
    assert (f == delete (is!!ix) is && b == [is!!ix])

  prpFirstFreeQ :: NonEmptyList (NonEmptyList Char) -> Property
  prpFirstFreeQ ns = let is = nub $ nonemptyString ns
                      in monadicIO $ do
    mq <- run  $ newMVar $ Q S.empty S.empty
    ws <- run  $ mapM (makeW mq 0) is
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
    mq <- run  $ newMVar $ Q S.empty S.empty
    ws <- run  $ mapM (makeW mq 0) is
    (_, b, w) <- run $ modifyMVar mq $ \q -> do
                      let q1 = mapQ ws q insertQ
                      let q2 = mapQ ws q1 (\w qx -> setStateQ (fst w) Busy id qx)
                      let w  = firstBusyQ q2
                      returnBothMore q2 w
    case w of
      Nothing -> assert False
      Just x  -> assert (b == is && (B.unpack .fst) x == head is)

  prpInsertOne :: NonEmptyList Char -> Property
  prpInsertOne (NonEmpty is) = monadicIO $ do
    run clean
    run $ insert (B.pack is) (B.pack "Test")
    mbW <- run $ getWorker   (B.pack "Test") 
    case mbW of
      Nothing -> assert False
      Just w  -> assert (B.unpack w == is)

  prpInsertSize :: NonEmptyList (NonEmptyList Char) -> Property
  prpInsertSize ns = let is = nub $ nonemptyString ns
                     in monadicIO $ do
    run clean
    run $ mapM_ (\i -> insert (B.pack i) $ B.pack "Test") is
    n <- run size
    assert (n == length is) 

  prpInsertAll :: NonEmptyList (NonEmptyList Char) -> Property
  prpInsertAll ns = let is = nub $ nonemptyString ns
                     in monadicIO $ do
    run clean
    run $ mapM_ (\i -> insert (B.pack i) $ B.pack "Test") is
    ws <- run $ (map B.unpack . catMaybes) <$> 
                mapM (\_ -> getWorker $ B.pack "Test") is
    assert (ws == is)

  prpStatsPerService :: NonEmptyList (NonEmptyList Char) -> Property
  prpStatsPerService ns = let is = nub $ nonemptyString ns
                           in monadicIO $ do
    run clean
    run $ mapM_ (\i -> insert (B.pack i) $ B.pack "Test") is
    n <- pick $ choose (1, length is)
    _ <- run  $ mapM (\_ -> getWorker $ B.pack "Test") [1..n]
    sAll <- run stat
    sSrv <- run $ statPerService $ B.pack "Test"
    assert (not (null sAll)   &&
            length sAll == 1  &&
            head sAll == sSrv && 
            sSrv == (B.pack "Test", length is - n, n)) 

  prpNextHB :: NonEmptyList (NonEmptyList Char) -> Property
  prpNextHB ns = let is = nub $ nonemptyString ns
                  in monadicIO $ do
    let sn = B.pack "Test"
    run clean
    run $ mapM_ (\i -> insert (B.pack i) sn) is
    xs <- run checkWorker
    assert (null xs)

  ------------------------------------------------------------------------
  -- who has to be sent?
  ------------------------------------------------------------------------
  prpCheckSnd :: NonEmptyList (NonEmptyList Char) -> Property
  prpCheckSnd ns = let is = map B.pack $ nub $ nonemptyString ns
                    in monadicIO $ do
    let sn = B.pack "Test"
    run $ do clean
             mapM_ (`insert` sn) is
             setMeBack (-10000) is
    xs <- run checkWorker
    assert (xs == is)

  ------------------------------------------------------------------------
  -- checkWorker buries the dead
  ------------------------------------------------------------------------
  prpCheckBuries :: NonEmptyList (NonEmptyList Char) -> Property
  prpCheckBuries ns = let is = map B.pack $ nub $ nonemptyString ns
                        in monadicIO $ do
    let sn = B.pack "Test"
    (xs, ys, zs, n1 , n2) <- run $ do 
      clean
      mapM_ (`insert` sn) is
      n1 <- size
      xs <- checkWorker
      setMeBack (-10000) is
      ys <- checkWorker
      setHimBack (-100000) is
      zs <- checkWorker
      n2 <- size
      return (xs, ys, zs, n1, n2)
    assert (null xs  &&    
            ys == is &&
            null zs  &&
            n1 > n2 {-   &&
            (n1 == length is && n2 == 0) -} )

  ------------------------------------------------------------------------
  -- getWorker buries the dead
  ------------------------------------------------------------------------
  prpGetBuries :: NonEmptyList (NonEmptyList Char) -> Property
  prpGetBuries ns = let is = map B.pack $ nub $ nonemptyString ns
                     in monadicIO $ do
    let sn = B.pack "Test"
    (xs, n1, n2) <- run $ do 
      clean
      mapM_ (`insert` sn) is
      n1 <- size
      setMeBack (-10000) is
      xs <- checkWorker
      setHimBack (-100000) is
      mapM_ (\_ -> getWorker sn) [1..length is] 
      n2 <- size
      return (xs, n1, n2)
    assert (xs == is &&
            n1 > n2 &&  
            (n1 == length is && n2 == 0))

  ------------------------------------------------------------------------
  -- Bury busy workers
  ------------------------------------------------------------------------
  prpBuryBusy :: NonEmptyList (NonEmptyList Char) -> Property
  prpBuryBusy ns = let is = map B.pack $ nub $ nonemptyString ns
                     in monadicIO $ do
    let sn = B.pack "Test"
    (xs, n1, n2) <- run $ do 
      clean
      mapM_ (`insert` sn) is
      n1 <- size
      mapM_ (\_ -> getWorker sn) is
      setHimBack (-100000) is
      xs <- checkWorker
      n2 <- size
      return (xs, n1, n2)
    assert (null xs  && 
            n1 > n2  &&  
            (n1 == length is && n2 == 0))

  prpDontBuryEarly :: NonEmptyList (NonEmptyList Char) -> Property
  prpDontBuryEarly ns = let is = map B.pack $ nub $ nonemptyString ns
                          in monadicIO $ do
    let sn = B.pack "Test"
    (xs, n1, n2) <- run $ do 
      clean
      mapM_ (`insert` sn) is
      n1 <- size
      mapM_ (\_ -> getWorker sn) is 
      setBack (-10) is
      xs <- checkWorker
      n2 <- size
      return (xs, n1, n2)
    assert (null xs && n1 == n2)

  prpDontBuryAlive :: NonEmptyList (NonEmptyList Char) -> Property
  prpDontBuryAlive ns = let is  = map B.pack $ nub $ nonemptyString ns
                            is2 = map (`B.append` (B.pack "x-")) is
                          in monadicIO $ do
    let sn = B.pack "Test"
    (xs, n1, n2, n3) <- run $ do 
      clean
      mapM_ (`insert` sn) is
      n1 <- size
      mapM_ (\_ -> getWorker sn) is
      setHimBack (-100000) is
      mapM_ (`insert` sn) is2
      n2 <- size
      xs <- checkWorker
      n3 <- size
      return (xs, n1, n2, n3)
    assert (null xs                      && 
            n1 == length is              &&
            n2 == length is + length is2 && 
            n3 == length is2)

  setBack :: Msec -> [B.ByteString] -> IO ()
  setBack ms is = setMeBack ms is >> setHimBack ms is

  setMeBack :: Msec -> [B.ByteString] -> IO ()
  setMeBack ms = mapM_ (`updWorker` setTime) 
    where setTime (i, w) = 
            let hb  = (wrkHB w){hbNextMe = timeAdd (hbNextMe $ wrkHB w) ms}
             in (i, w {wrkHB = hb})

  setHimBack :: Msec -> [B.ByteString] -> IO ()
  setHimBack ms = mapM_ (`updWorker` setTime) 
    where setTime (i, w) = 
            let hb  = (wrkHB w){hbNextHe = timeAdd (hbNextHe $ wrkHB w) ms}
             in (i, w {wrkHB = hb})

  returnFree :: Queue -> IO (Queue, [String])
  returnFree q = return (q, map (B.unpack . fst) $ toList $ qFree q)

  returnBusy :: Queue -> IO (Queue, [String])
  returnBusy q = return (q, map (B.unpack . fst) $ toList $ qBusy q)

  returnBoth :: Queue -> IO (Queue, ([String], [String]))
  returnBoth q = return (q, (map (B.unpack . fst) $ toList $ qFree q,
                             map (B.unpack . fst) $ toList $ qBusy q))

  returnBothMore :: Queue -> a -> 
                    IO (Queue, ([String], [String], a))
  returnBothMore q w = do (q', (f,b)) <- returnBoth q
                          return (q', (f,b,w))

  nonemptyString :: NonEmptyList (NonEmptyList Char) -> [String]
  nonemptyString (NonEmpty ns) = map (\(NonEmpty c) -> c) ns
    
  mapQ :: [WrkNode] -> Queue -> (WrkNode -> Queue -> Queue) -> Queue
  mapQ []     q _ = q
  mapQ (w:ws) q f = mapQ ws (f w q) f

  makeW :: MVar Queue -> Msec -> String -> IO WrkNode
  makeW mq ms s = do
     hb <- newHeartbeat	ms
     let i = B.pack s
     return (i, Worker {
                  wrkId    = i,
                  wrkState = Free,
                  wrkHB    = hb,
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
         runTest "Insert - NextHB = now + x" 
                  (deepCheck prpNextHB)                ?> 
         runTest "Check finds hb for send"   
                  (deepCheck prpCheckSnd)              ?> 
         runTest "Check buries the dead"
                  (deepCheck prpCheckBuries)          ?> 
         runTest "Get   buries the dead"
                  (deepCheck prpGetBuries)            ?> 
         runTest "Bury Busy"
                  (deepCheck prpBuryBusy)             ?> 
         runTest "Don't bury early"
                  (deepCheck prpDontBuryEarly)        ?> 
         runTest "Don't bury alive"
                  (deepCheck prpDontBuryAlive)        ?> 
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
