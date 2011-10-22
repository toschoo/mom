module Main
where

  import Test
  import Protocol (Message(..))

  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Parser 
  import           Network.Mom.Stompl.Client.Queue

  import           Control.Concurrent

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.Maybe

  import           System.Exit
  import           System.Timeout

  import           Codec.MIME.Type (nullType, Type)
  
  maxRcv :: Int
  maxRcv = 1024

  host :: String
  host = "127.0.0.1"

  port :: Int
  port = 61613

  vers :: [F.Version]
  vers = [(1,0), (1,1)]

  beat :: F.Heart
  beat = (0,0)

  tQ1 :: String
  tQ1 = "/q/test1"

  iconv :: Converter String
  iconv = InBound (\_ _ _ -> return . U.toString)

  oconv :: Converter String
  oconv = OutBound (return . B.pack)

  tmo :: IO a -> IO (Maybe a)
  tmo = timeout 500000

  text1, text2, text3, text4 :: String
  text1 = "whose woods these are I think I know"
  text2 = "his house is in the village though"
  text3 = "he will not see me stopping here"
  text4 = "to watch is woods fill up with snow"

  main :: IO ()
  main = do
    r <- runTests mkTests
    case r of
      Pass   -> exitSuccess
      Fail e -> exitFailure

  runTests :: TestGroup IO -> IO TestResult
  runTests g = do
    (r, s) <- execGroup g
    putStrLn s
    return r

  mkTests :: TestGroup IO
  mkTests = 
    let t10 = mkTest "Send and Receive       " testSndRcv
        t20 = mkTest "Transaction            " testTx1
        t30 = mkTest "Transaction pending ack" testPendingAcksFail
        t40 = mkTest "Transaction all ack'd  " testPendingAcksPass
        t50 = mkTest "Auto Ack               " testAutoAck
    in  mkGroup "Dialogs" (Stop (Fail "")) 
        [t10, t20, t30, t40, t50]

  testSndRcv :: IO TestResult
  testSndRcv = 
    stdCon $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      writeQ oQ nullType [] text1
      m <- readQ iQ
      if msgCont m == text1
        then return Pass
        else return $ Fail $ "Received: " ++ (msgCont m) ++ " - Expected: " ++ text1

  testTx1 :: IO TestResult
  testTx1 =
    stdCon $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv
      ok <- withTransaction c [] $ \_ -> do
        writeQ oQ nullType [] text1
        writeQ oQ nullType [] text2
        writeQ oQ nullType [] text3
        writeQ oQ nullType [] text4
        mbM <- tmo $ readQ iQ
        case mbM of
          Nothing -> return True
          Just _  -> return False
      if not ok then return $ Fail "obtained messages sent in TX before commit!"
        else do
          mbM1 <- tmo $ readQ iQ
          mbM2 <- tmo $ readQ iQ
          mbM3 <- tmo $ readQ iQ
          mbM4 <- tmo $ readQ iQ
          let ms = catMaybes [mbM1, mbM2, mbM3, mbM4]
          if (length ms) /= 4
            then return $ Fail $ "Did not receive all messages: " ++ (show $ length ms)
            else let zs = zip ms [text1, text2, text3, text4]
                     ts = map (\x -> (msgCont . fst) x == snd x) zs
                 in if and ts then return Pass
                              else return $ Fail $
                                     "Messages are in wrong order: " ++ 
                                     (show $ map msgCont ms)

  testPendingAcksFail :: IO TestResult
  testPendingAcksFail =
    stdCon $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive, OMode F.Client] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      writeQ oQ nullType [] text1    -- send m1
      ok <- withTransaction c [OAbortMissingAcks] $ \_ -> do
        m1 <- readQ iQ               -- get  m1
        writeQ oQ nullType [] text2  -- send m2
        mbM2 <- tmo $ readQ iQ       -- get  m2 (should be timeout)
        case mbM2 of 
          Nothing -> return True
          Just m  -> return False
      if not ok 
        then return $ Fail "obtained message sent in TX before commit!"
        else do
          mbM2 <- tmo $ readQ iQ
          case mbM2 of 
            Nothing -> return Pass
            Just m  -> return $ Fail "obtained message from aborted TX!"

  testPendingAcksPass :: IO TestResult
  testPendingAcksPass = do
    stdCon $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive, OMode F.Client] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      writeQ oQ nullType [] text1    -- send m1
      ok <- withTransaction c [OAbortMissingAcks] $ \_ -> do
        m1 <- readQ iQ               -- get  m1
        writeQ oQ nullType [] text2  -- send m2
        mbM2 <- tmo $ readQ iQ       -- get  m2 (should be timeout)
        case mbM2 of 
          Nothing -> do
            ack c m1
            return True
          Just m  -> return False
      if not ok 
        then return $ Fail "obtained message sent in TX before commit!"
        else do
          mbM2 <- tmo $ readQ iQ
          case mbM2 of
            Nothing -> return $ Fail "no message from committed TX received!"
            Just m2 -> 
              if (msgContent m2) == text2 
                then return Pass
                else return $ Fail "Wrong message!"

  testAutoAck :: IO TestResult
  testAutoAck = do
    stdCon $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive, OMode F.Client, OAck] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      writeQ oQ nullType [] text1    -- send m1
      ok <- withTransaction c [OAbortMissingAcks] $ \_ -> do
        m1 <- readQ iQ               -- get  m1
        writeQ oQ nullType [] text2  -- send m2
        mbM2 <- tmo $ readQ iQ       -- get  m2 (should be timeout)
        case mbM2 of 
          Nothing -> return True
          Just m  -> return False
      if not ok 
        then return $ Fail "obtained message sent in TX before commit!"
        else do
          mbM2 <- tmo $ readQ iQ
          case mbM2 of
            Nothing -> return $ Fail "no message from committed TX received!"
            Just m2 -> 
              if (msgContent m2) == text2 
                then return Pass
                else return $ Fail "Wrong message!"
       
  stdCon :: (Con -> IO TestResult) -> IO TestResult
  stdCon act =
    withConnection host port maxRcv "guest" "guest" beat act
      
