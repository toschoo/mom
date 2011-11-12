{-# OPTIONS -fno-cse #-}
module Main
where

  import Test.QuickCheck
  import Test.QuickCheck.Monadic
  import Data.List (find)
  import Control.Applicative((<$>))
  import Control.Concurrent
  import Control.Monad (when)
  import Data.Time.Clock
  import Data.Maybe

  import System.Exit
  import System.IO.Unsafe

  import State
  import Factory
  import qualified Protocol as P

  --------------------------------------------------------
  -- Arbitrary 
  --------------------------------------------------------
  instance Arbitrary Con where
    arbitrary = Con <$> choose (1,99999999)

  data Number = No Int
    deriving (Eq, Show)

  instance Arbitrary Number where
    arbitrary = No <$> choose (0,9)

  --------------------------------------------------------
  -- Just a test
  --------------------------------------------------------
  prp_simple :: Int -> Property
  prp_simple i = collect i $ monadicIO $ assert (i == i)

  --------------------------------------------------------
  -- When we do nothing, 
  -- length before/after shall be equal
  --------------------------------------------------------
  prp_addEqLCon :: NonEmptyList Con -> Property
  prp_addEqLCon = cmpBeforeAfter eqL
    where eqL b a = length b == length a 

  --------------------------------------------------------
  -- When we do nothing, 
  -- connections shall be equal
  --------------------------------------------------------
  prp_addEq :: NonEmptyList Con -> Property
  prp_addEq = cmpBeforeAfter eq
    where eq b a = b == a 

  --------------------------------------------------------
  -- logSend shall change my beat, but not his beat
  --------------------------------------------------------
  prp_logSend :: Property
  prp_logSend = monadicIO $
    testCon2 tstLogSend eqSend
    where eqSend  b a  = conMyBeat b  /= conMyBeat  a &&
                         conHisBeat b == conHisBeat a
          tstLogSend cid = do
            threadDelay 5
            logSend cid

  --------------------------------------------------------
  -- logReceive shall change his beat, but not my beat
  --------------------------------------------------------
  prp_logReceive :: Property
  prp_logReceive = monadicIO $ testCon2 tstLogReceive eqRcv
    where eqRcv b a  = conMyBeat b  == conMyBeat  a &&
                        conHisBeat b /= conHisBeat a
          tstLogReceive cid = do
            threadDelay 5
            logReceive cid

  --------------------------------------------------------
  -- addSid shall add an Sid to conSubs
  --------------------------------------------------------
  prp_addSid :: NonEmptyList Con -> Property
  prp_addSid = testCon tstAddSid (have hasSid)
    where hasSid (b, a) = (null   . conSubs) b &&
                          (length . conSubs) a == 1

  --------------------------------------------------------
  -- rmSid . addSid c == c
  --------------------------------------------------------
  prp_addRmSid :: NonEmptyList Con -> Property
  prp_addRmSid = testCon tstAddRmSid (have hasNoSid)
    where tstAddRmSid cid = tstAddSid cid >> tstRmSid cid
          hasNoSid (b, a) = (null . conSubs) b &&
                            (null . conSubs) a 

  --------------------------------------------------------
  -- addDest shall add a dst to conDests
  --------------------------------------------------------
  prp_addDest :: NonEmptyList Con -> Property
  prp_addDest = testCon tstAddDest (have hasDest)
    where hasDest (b, a) = (null   . conDests)   b &&
                           (length . conDests)   a == 1 

  --------------------------------------------------------
  -- rmDest . addDest c == c
  --------------------------------------------------------
  prp_addRmDest :: NonEmptyList Con -> Property
  prp_addRmDest = testCon tstAddRmDest (have noDest)
    where tstAddRmDest cid = tstAddDest cid >> tstRmDest cid
          noDest (b, a) = (null . conDests)   b &&
                          (null . conDests)   a 

  --------------------------------------------------------
  -- addTx shall add an entry to conThrds
  -- whose list of txs is not empty
  --------------------------------------------------------
  prp_addTx :: NonEmptyList Con -> Property
  prp_addTx = testCon tstAddTx (have hasTx)
    where hasTx (b, a) = (       null              . conThrds) b &&
                         (not  . null              . conThrds) a &&
                         (not  . null . snd . head . conThrds) a 

  --------------------------------------------------------
  -- rmTx . addTx c == c
  --------------------------------------------------------
  prp_addRmTx :: NonEmptyList Con -> Property
  prp_addRmTx = testCon tstAddRmTx (have hasNoTx)
    where tstAddRmTx cid = tstAddTx cid >> tstRmTx cid 
          hasNoTx (b, a) = (null . conThrds) b &&
                           (null . conThrds) a

  --------------------------------------------------------
  -- addThisTx shall add tx to c
  --------------------------------------------------------
  prp_addThisTx :: Property
  prp_addThisTx = monadicIO $ do
    tx <- run mkUniqueTxId
    testTx (addThisTx tx) (hasTx tx)
    where addThisTx tx cid = addTx (mkTrn tx []) cid
          hasTx tx b a     = (null(conThrds b) || curTx b /= tx) &&
                             (not (null $ conThrds a) && curTx a == tx)

  --------------------------------------------------------
  -- rmTx shall remove the first tx from c
  --------------------------------------------------------
  prp_rmTx :: Property
  prp_rmTx = monadicIO $ do
    tx <- run mkUniqueTxId
    testTx (tst2RmTx tx) (hasTx tx)
    where tst2RmTx tx cid = do
            addTx (mkTrn tx []) cid
            rmTx cid 
          hasTx tx b a = (null(conThrds b) || curTx b /= tx) &&
                         (null(conThrds a) ||  
                          findTx tx (txOfThrd a) == Nothing)

  --------------------------------------------------------
  -- rmThisTx shall remove tx from c
  --------------------------------------------------------
  prp_rmThisTx :: Property
  prp_rmThisTx = monadicIO $ do
    tx <- run mkUniqueTxId
    testTx (tstRmThisTx tx) (hasTx tx)
    where tstRmThisTx tx cid = do
            addTx (mkTrn tx []) cid
            rmThisTx tx cid 
          hasTx tx b a = (null(conThrds b) || curTx b /= tx) &&
                         (null(conThrds a) ||
                          findTx tx (txOfThrd a) == Nothing)

  --------------------------------------------------------
  -- Removing the last tx shall remove conThrds
  --------------------------------------------------------
  prp_rmThrds :: Property
  prp_rmThrds = monadicIO $ 
    testTx rmAllTx hasNoThrds
    where rmAllTx cid = do
            c <- getCon cid
            let ts = map txId $ snd $ head $ conThrds c
            mapM_ (\x -> rmThisTx x cid) ts
          hasNoThrds b a = not (null $ conThrds b) &&
                           null(conThrds a)

  -------------------------------------------------------------
  -- addAck without tx shall add ack to conAcks
  -------------------------------------------------------------
  prp_addOneAckCon :: Property
  prp_addOneAckCon = monadicIO $ do
    let msg = P.MsgId "100"
    testCon2 (tstAddAck msg) (conHas conAcks msg)
    where tstAddAck m cid = addAck cid m

  -------------------------------------------------------------
  -- addAck with tx shall add ack to txAcks and not to conAcks
  -------------------------------------------------------------
  prp_addOneAckTx :: Property
  prp_addOneAckTx = monadicIO $ do
    let msg = P.MsgId "100"
    testTx (tstAddAck msg) (txHas conAcks txAcks msg)
    where tstAddAck m cid = addAck cid m

  -------------------------------------------------------------
  -- rmAck . addAck c == c
  -------------------------------------------------------------
  prp_addRmOneAckCon :: Property
  prp_addRmOneAckCon = monadicIO $ do
    let msg = P.MsgId "100"
    testCon2 (tstAddRmAck msg) (conHasNo conAcks)
    where tstAddRmAck m cid = addAck cid m >> rmAck cid m

  -------------------------------------------------------------
  -- rmAck . addAck tx == tx
  -------------------------------------------------------------
  prp_addRmOneAckTx :: Property
  prp_addRmOneAckTx = monadicIO $ do
    let msg = P.MsgId "100"
    testTx (tstAddAck msg) (txHasNot conAcks txAcks msg)
    where tstAddAck m cid = addAck cid m >> rmAck cid m

  -------------------------------------------------------------
  -- addAck without tx shall add ack to conAcks with n acks
  -------------------------------------------------------------
  prp_addAckCon :: Number -> Property
  prp_addAckCon (No n) = monadicIO $ do
    let msg = P.MsgId "100"
    testCon2 (tstAddAck msg) (conHas conAcks msg)
    where tstAddAck msg cid = addAcks n cid >> addAck cid msg

  -------------------------------------------------------------
  -- rmAck addAck c == c without tx with n acks
  -------------------------------------------------------------
  prp_addRmAckCon :: Number -> Property
  prp_addRmAckCon (No n) = monadicIO $ do
    let msg = P.MsgId "100"
    testCon2 (tstAddAck msg) (conHasNot conAcks msg)
    where tstAddAck msg cid = addAck cid msg >> addAcks n cid >> rmAck cid msg

  -------------------------------------------------------------
  -- addAck with tx shall add ack to conAcks with n acks
  -------------------------------------------------------------
  prp_addAckTx :: Number -> Property
  prp_addAckTx (No n) = monadicIO $ do
    let msg = P.MsgId "100"
    testTx (tstAddAck msg) (txHas conAcks txAcks msg)
    where tstAddAck msg cid = addAcks n cid >> addAck cid msg

  -------------------------------------------------------------
  -- rmAck . addAck tx == tx with n acks
  -------------------------------------------------------------
  prp_addRmAckTx :: Number -> Property
  prp_addRmAckTx (No n) = monadicIO $ do
    let msg = P.MsgId "100"
    testTx (tstAddRmAck msg) (txHasNot conAcks txAcks msg)
    where tstAddRmAck msg cid = addAck cid msg >> addAcks n cid >> rmAck cid msg

  -------------------------------------------------------------
  -- addRec without tx shall add rc to conRecs
  -------------------------------------------------------------
  prp_addOneRecCon :: Property
  prp_addOneRecCon = monadicIO $ do
    rc <- run mkUniqueRecc
    testCon2 (tstAddRec rc) (conHas conRecs rc)
    where tstAddRec r cid = addRec cid r

  -------------------------------------------------------------
  -- addRec with tx shall add rc to txRecs
  -------------------------------------------------------------
  prp_addOneRecTx :: Property
  prp_addOneRecTx = monadicIO $ do
    rc <- run mkUniqueRecc
    testTx (tstAddRec rc) (txHas conRecs txRecs rc)
    where tstAddRec r cid = addRec cid r

  -------------------------------------------------------------
  -- rmRec . addRec c == c 
  -------------------------------------------------------------
  prp_addRmOneRecCon :: Property
  prp_addRmOneRecCon = monadicIO $ do
    rc <- run mkUniqueRecc
    testTx (tstAddRmRec rc) (conHasNot conRecs rc)
    where tstAddRmRec r cid = addRec cid r >> rmRec cid r

  -------------------------------------------------------------
  -- rmRec . addRec tx == tx 
  -------------------------------------------------------------
  prp_addRmOneRecTx :: Property
  prp_addRmOneRecTx = monadicIO $ do
    rc <- run mkUniqueRecc
    testTx (tstAddRmRec rc) (txHasNot conRecs txRecs rc)
    where tstAddRmRec r cid = addRec cid r >> rmRec cid r

  -------------------------------------------------------------
  -- addRec without tx shall add rc to conRecs with n recs
  -------------------------------------------------------------
  prp_addRecCon :: Number -> Property
  prp_addRecCon (No n) = monadicIO $ do
    rc <- run mkUniqueRecc
    testCon2 (tstAddRc rc) (conHas conRecs rc)
    where tstAddRc r cid = addRecs n cid >> addRec cid r

  -------------------------------------------------------------
  -- rmRec . addRec tx == tx with n Recs
  -------------------------------------------------------------
  prp_addRmRecCon :: Number -> Property
  prp_addRmRecCon (No n) = monadicIO $ do
    rc <- run mkUniqueRecc
    testCon2 (tstAddRmRec rc) (conHasNot conRecs rc)
    where tstAddRmRec r cid = addRec cid r >> addRecs n cid >> rmRec cid r

  -------------------------------------------------------------
  -- addRec with tx shall add rc to txRecs with n recs
  -------------------------------------------------------------
  prp_addRecTx :: Number -> Property
  prp_addRecTx (No n) = monadicIO $ do
    rc <- run mkUniqueRecc
    testTx (tstAddRec rc) (txHas conRecs txRecs rc)
    where tstAddRec r cid = addRecs n cid >> addRec cid r

  -------------------------------------------------------------
  -- rmRec . addRec tx == tx with n Recs
  -------------------------------------------------------------
  prp_addRmRecTx :: Number -> Property
  prp_addRmRecTx (No n) = monadicIO $ do
    rc <- run mkUniqueRecc
    testTx (tstAddRmRec rc) (txHasNot conRecs txRecs rc)
    where tstAddRmRec r cid = addRec cid r >> addRecs n cid >> rmRec cid r

  -- plural of has --------------------------------------------
  have :: ((Connection, Connection) -> Bool) -> 
          [Connection] -> [Connection] -> Bool
  have f x y = all f $ zip x y

  -------------------------------------------------------------
  -- connection has something
  -------------------------------------------------------------
  conHas :: Eq a => (Connection -> [a]) -> a -> 
                    Connection -> Connection -> Bool
  conHas get m b a = null (get b)        &&
                     not  (null $ get a) &&
                     head (       get a) == m

  -------------------------------------------------------------
  -- connection has something
  -------------------------------------------------------------
  conHasNot :: Eq a => (Connection -> [a]) -> a -> 
                       Connection -> Connection -> Bool
  conHasNot get m b a = null       (get b) &&
                        find (==m) (get a) == Nothing

  -------------------------------------------------------------
  -- connection has not something
  -------------------------------------------------------------
  conHasNo :: (Connection -> [a]) -> 
              Connection -> Connection -> Bool
  conHasNo get b a = null (get b) &&
                     null (get a)

  -------------------------------------------------------------
  -- transaction has something
  -------------------------------------------------------------
  txHas :: Eq a => (Connection  -> [a]) -> 
                   (Transaction -> [a]) -> a  -> 
                   Connection   -> Connection -> Bool
  txHas conGet txGet m b a = 
    not  (null $ conThrds a)                         &&
    not  (null $ txOfThrd a)                         &&
    find (==m) (conGet b)                 == Nothing &&
    find (==m) (conGet a)                 == Nothing &&
    find (==m) (txGet $ curTransaction b) == Nothing &&
    find (==m) (txGet $ curTransaction a) == Just m 

  -------------------------------------------------------------
  -- transaction has not something
  -------------------------------------------------------------
  txHasNot :: Eq a => (Connection  -> [a]) -> 
                      (Transaction -> [a]) -> a -> 
                      Connection -> Connection  -> Bool
  txHasNot conGet txGet m b a = 
    not (null $ conThrds a)                          &&
    find (==m) (conGet b)                 == Nothing &&
    find (==m) (conGet a)                 == Nothing &&
    find (==m) (txGet $ curTransaction b) == Nothing &&
    find (==m) (txGet $ curTransaction a) == Nothing

  -------------------------------------------------------------
  -- get current tx
  -------------------------------------------------------------
  curTransaction :: Connection -> Transaction
  curTransaction = head . txOfThrd

  -------------------------------------------------------------
  -- get current txid
  -------------------------------------------------------------
  curTx :: Connection -> Tx
  curTx = txId . curTransaction

  -------------------------------------------------------------
  -- get txs of thread
  -------------------------------------------------------------
  txOfThrd :: Connection -> [Transaction]
  txOfThrd = snd . head . conThrds

  -------------------------------------------------------------
  -- find tx in [Transaction]
  -------------------------------------------------------------
  findTx :: Tx -> [Transaction] -> Maybe Transaction
  findTx tx = find (\t -> txId t == tx)

  -------------------------------------------------------------
  -- addSid
  -------------------------------------------------------------
  tstAddSid :: Con -> IO ()
  tstAddSid cid = do
    sid <- mkUniqueSubId
    ch  <- newChan
    addSub cid (sid, ch)

  -------------------------------------------------------------
  -- rmSid
  -------------------------------------------------------------
  tstRmSid :: Con -> IO ()
  tstRmSid cid = do
    c <- getCon cid
    let (sid, _) = head $ conSubs c
    rmSub cid sid

  -------------------------------------------------------------
  -- addDest
  -------------------------------------------------------------
  tstAddDest :: Con -> IO ()
  tstAddDest cid = do
    let dst = "/q/test"
    ch   <- newChan
    addDest cid (dst, ch)

  -------------------------------------------------------------
  -- rmDest
  -------------------------------------------------------------
  tstRmDest :: Con -> IO ()
  tstRmDest cid = do
    c <- getCon cid
    let (dst, _) = head $ conDests c
    rmDest cid dst

  -------------------------------------------------------------
  -- addTx
  -------------------------------------------------------------
  tstAddTx :: Con -> IO ()
  tstAddTx cid = do
    tx <- mkUniqueTxId
    addTx (mkTrn tx []) cid

  -------------------------------------------------------------
  -- rmTx
  -------------------------------------------------------------
  tstRmTx :: Con -> IO ()
  tstRmTx cid = rmTx cid

  -------------------------------------------------------------
  -- before/after test, do not change anything
  -------------------------------------------------------------
  cmpBeforeAfter :: ([Connection] -> [Connection] -> Bool) -> 
                    NonEmptyList Con  -> Property
  cmpBeforeAfter = testCon (\_ -> return ()) 

  -------------------------------------------------------------
  -- some magic do get a connection
  -------------------------------------------------------------
  {-# NOINLINE con #-}
  con :: MVar P.Connection
  con = unsafePerformIO $ do
    c <- P.connect "127.0.0.1" 22222 1024 "guest" "guest" [(1,0), (1,1)] (0,0)
    _ <- P.disc c
    newMVar c

  -------------------------------------------------------------
  -- before/after apply action n connections
  -------------------------------------------------------------
  testCon :: (Con -> IO ()) ->
             ([Connection] -> [Connection] -> Bool) ->
             NonEmptyList Con -> Property
  testCon act prop (NonEmpty cids) = monadicIO $ do
    cs  <- run $ mkCs cids
    run $ mapM_ addCon cs
    run $ mapM_ act $ map conId cs
    cs' <- run $ mapM getCon $ map conId cs
    run $ mapM_ rmCon $ map conId cs 
    assert $ prop cs cs'

  -------------------------------------------------------------
  -- before/after apply action with one connection
  -------------------------------------------------------------
  testCon2 :: (Con -> IO ()) ->
             (Connection -> Connection -> Bool) -> PropertyM IO ()
  testCon2 act prop = do
    cid <- run mkUniqueConId
    c   <- run $ mkC cid
    run $ addCon c
    run $ act cid
    c'  <- run $ getCon cid
    run $ rmCon cid
    assert $ prop c c'

  -------------------------------------------------------------
  -- before/after with some background transactions
  -------------------------------------------------------------
  testTx :: (Con -> IO ()) ->
            (Connection    -> Connection -> Bool) -> PropertyM IO ()
  testTx act prop = do
    let cids = [Con 1, Con 2, Con 3, Con 4, Con 5]
    cs <- run $ mkCs cids
    run $ mapM_ addCon cs
    run $ mapM_ (\_ -> mapM_ tstAddTx cids) ([1..3]::[Int]) -- "Background Tx"
    let cid = conId $ fromJust $ find (\x -> conId x == Con 2) cs 
    c  <- run $ getCon cid
    run $ act cid 
    c' <- run $ getCon cid
    run $ mapM_ rmCon cids
    assert $ prop c c'

  -------------------------------------------------------------
  -- addAck
  -------------------------------------------------------------
  addAcks :: Int -> Con -> IO ()
  addAcks n = add addAck mkMsg n 
    where mkMsg = return . P.MsgId . show 

  -------------------------------------------------------------
  -- addRecs
  -------------------------------------------------------------
  addRecs :: Int -> Con -> IO ()
  addRecs = add addRec (\_ -> mkUniqueRecc)

  -------------------------------------------------------------
  -- generic add
  -------------------------------------------------------------
  add :: (Con -> a -> IO ()) -> (Int -> IO a) -> Int -> Con -> IO ()
  add put mk n cid = when (n > 0) $ do
                    x <- mk n
                    put cid x
                    add put mk (n-1) cid

  -------------------------------------------------------------
  -- make n connections
  -------------------------------------------------------------
  mkCs :: [Con] -> IO [Connection]
  mkCs cids = mapM mkC cids

  -------------------------------------------------------------
  -- make one connection
  -------------------------------------------------------------
  mkC :: Con -> IO Connection
  mkC cid = do
    me  <- myThreadId
    now <- getCurrentTime
    c   <- readMVar con
    return $ mkConnection cid c me now []

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
  -- The test battery
  -------------------------------------------------------------
  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    r <- deepCheck prp_addEqLCon      ?> 
         deepCheck prp_addEq          ?>
         deepCheck prp_logSend        ?>
         deepCheck prp_logReceive     ?>
         deepCheck prp_addSid         ?>
         deepCheck prp_addRmSid       ?>
         deepCheck prp_addDest        ?>
         deepCheck prp_addRmDest      ?>
         deepCheck prp_addTx          ?>
         deepCheck prp_addRmTx        ?>  
         oneCheck  prp_addThisTx      ?>
         oneCheck  prp_rmTx           ?>
         oneCheck  prp_rmThisTx       ?>
         oneCheck  prp_rmThrds        ?>
         oneCheck  prp_addOneAckCon   ?>
         oneCheck  prp_addRmOneAckCon ?>
         oneCheck  prp_addOneAckTx    ?>
         oneCheck  prp_addRmOneAckTx  ?>
         oneCheck  prp_addOneRecCon   ?>
         oneCheck  prp_addRmOneRecCon ?>
         oneCheck  prp_addOneRecTx    ?>
         oneCheck  prp_addRmOneRecTx  ?>
         deepCheck prp_addAckCon      ?>
         deepCheck prp_addRmAckCon    ?>
         deepCheck prp_addRecCon      ?>
         deepCheck prp_addRmRecCon    ?>
         deepCheck prp_addAckTx       ?>
         deepCheck prp_addRmAckTx     ?> 
         deepCheck prp_addRecTx       ?>
         deepCheck prp_addRmRecTx     
    case r of
      Success _ -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
  
