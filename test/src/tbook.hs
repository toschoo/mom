module Main
where

  import Book
  import Config
  import Types
  import Test.QuickCheck
  import Data.List (sort, nub, find)
  import Control.Applicative((<$>))
  import qualified Data.ByteString as B

  import System.Exit

  -- testTransaction :: [Trandaction] -> Property
  -- testTransaction ts = 

  cid1 :: Int
  cid1 = 1
  
  tid1 :: String
  tid1 = "tx-1"

  --------------------------------------------------------
  -- Transaction 
  --------------------------------------------------------
  instance Arbitrary Transaction where
    arbitrary = do
      cid <- choose (1,3)    :: Gen Int
      x   <- choose (1, 100) :: Gen Int
      let tid = "tx-" ++ (show x)
      return $ mkTx cid tid [] 

  testTxId :: (Int, String) -> Bool
  testTxId (cid, trn) = 
    let (cid', trn') = parseTrn $ mkTxId cid trn 
    in   cid' == cid &&
         trn' == trn

  insTxOrd :: [Transaction] -> Bool
  insTxOrd ts =
    let c   = quickConfig
        b   = mkBook c 
        b'  = foldr (\y x -> addTx y x) b ts
        ts' = bookTrans b'
    in ts' == (sort $ nub ts') 

  insTxGrows :: [Transaction] -> Bool
  insTxGrows ts =
    let c   = quickConfig
        b   = mkBook c 
        b'  = foldr (\y x -> addTx y x) b ts
        ts' = bookTrans b'
    in length (bookTrans b) == 0 &&
       length ts'           == (length $ nub ts)

  testTx :: [Transaction] -> Property
  testTx ts = txPre ts ==> insTxOrd   ts &&
                           insTxGrows ts

  txPre :: [Transaction] -> Bool
  txPre ts = length ts > 10

  --------------------------------------------------------------
  -- MsgStore
  --------------------------------------------------------------
  instance Arbitrary MsgStore where
    arbitrary = do
      x   <- choose (1,100) :: Gen Int
      y   <- choose (1,10)  :: Gen Int
      z   <- choose (1,100) :: Gen Int
      cid <- choose (1,3)   :: Gen Int
      pnd <- elements [Pending, Sent] 
      let sid = "/q/a:" ++ (show x) ++ ":" ++ (show y)
      let mid = "m-" ++ (show z)
      return $ MsgStore {strId = mid,
                         strMsg = B.empty,
                         strPending = [(cid, mkPnd sid pnd)]}

  testMsgFilter :: [MsgStore] -> Property
  testMsgFilter ms = (length ms > 2) ==> testMsgFilterFinds ms

  testMsgFilterFinds :: [MsgStore] -> Bool
  testMsgFilterFinds ms = 
    let m         = last ms
        m'        = m {strId = "man_" ++ strId m}
        ms'       = [head ms, m'] ++ tail ms
        ps        = head $ strPending m
        cid       = fst ps
        (sid, st) = snd ps
        f   = msgFilter cid sid st ms' 
    in length f >= 2 && checkEls m m' f
    where checkEls m1 m2 ms = 
            case find (\x -> strId x == strId m1) ms of
              Nothing -> False
              Just _  -> case find (\x -> strId x == strId m2) ms of
                           Nothing -> False
                           Just _  -> True

  quickConfig :: Config
  quickConfig = SenderCfg {
                  cfgName     = "Test",
                  cfgWriteSnd = (\_ -> return ()),
                  cfgReadSnd  = return $ UnRegMsg 1,
                  cfgWriteLog = (\_ -> return ())
                } 

  deepCheck :: (Testable p) => p -> IO Result
  deepCheck = quickCheckWithResult stdArgs{maxSuccess=100,
                                           maxDiscard=500}

  applyTest :: IO Result -> IO Result -> IO Result
  applyTest r f = do
    r' <- r
    case r' of
      Success _ -> f
      x         -> return x

  infixr ?>
  (?>) :: IO Result -> IO Result -> IO Result
  (?>) = applyTest

  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    r <- (
          (deepCheck testTxId) ?>
          (deepCheck testTx)   ?>
          (deepCheck testMsgFilter))
    case r of
      Success _ -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
  
