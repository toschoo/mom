module Main
where

  import Test
  import Protocol (Message(..))

  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Parser 
  import           Network.Mom.Stompl.Client.Queue
  import           Network.Mom.Stompl.Client.Exception (try)

  import           Control.Concurrent

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.Maybe
  import           Data.Char (isDigit)

  import           System.Exit
  import           System.Environment
  import           System.Timeout

  import           Codec.MIME.Type (nullType, Type)
  
  maxRcv :: Int
  maxRcv = 1024

  host :: String
  host = "127.0.0.1"

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
    os <- getArgs
    case os of
      [p] -> do
        if not $ (and . map isDigit) p
          then do
            putStrLn $ "Port number '" ++ p ++ "' is not numeric!"
            exitFailure
          else do
            r <- runTests $ mkTests (read p) 
            case r of
              Pass   -> exitSuccess
              Fail e -> exitFailure
      _   -> do
        putStrLn "I need a port number"
        exitFailure

  runTests :: TestGroup IO -> IO TestResult
  runTests g = do
    (r, s) <- execGroup g
    putStrLn s
    return r

  mkTests :: Int -> TestGroup IO
  mkTests p = 
    let t10 = mkTest "Create receiving Queue " $ testMkInQueue  p
        t20 = mkTest "Create sending   Queue " $ testMkOutQueue p 
        t30 = mkTest "Send and Receive       " $ testSndRcv p
        t40 = mkTest "Transaction            " $ testTx1 p
        t50 = mkTest "Transaction pending ack" $ testPendingAcksFail p
        t60 = mkTest "Transaction all ack'd  " $ testPendingAcksPass p
        t70 = mkTest "Auto Ack               " $ testAutoAck p
        t80 = mkTest "With Receipt1          " $ testQWithReceipt p
        t90 = mkTest "Wait on Receipt1       " $ testQWaitReceipt p
        -- with receipt, but don't wait (waitReceipt)
        -- with receipt, wait and use writeQ
        -- ackWith
        -- transaction with receipts
    in  mkGroup "Dialogs" (Stop (Fail "")) 
        [t10, t20, t30, t40, t50, t60, t70, t80, t90]

  testMkInQueue :: Int -> IO TestResult
  testMkInQueue p = do
    eiR <- try $ stdCon p $ \c -> do
      eiQ <- try $ newQueue c "IN"  tQ1 [OReceive] [] iconv
      case eiQ of
        Left  e -> return $ Fail $ show e
        Right _ -> return Pass
    case eiR of
       Left e  -> return $ Fail $ show e
       Right r -> return r

  testMkOutQueue :: Int -> IO TestResult
  testMkOutQueue p =
    stdCon p $ \c -> do
      eiQ <- try $ newQueue c "OUT"  tQ1 [OSend] [] oconv
      case eiQ of
        Left  e -> return $ Fail $ show e
        Right _ -> return Pass

  testSndRcv :: Int -> IO TestResult
  testSndRcv p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      writeQ oQ nullType [] text1
      m <- readQ iQ
      if msgCont m == text1
        then return Pass
        else return $ Fail $ "Received: " ++ (msgCont m) ++ " - Expected: " ++ text1
    case eiR of
       Left e  -> return $ Fail $ show e
       Right r -> return r

  testTx1 :: Int -> IO TestResult
  testTx1 p = do
    eiR <- try $ stdCon p $ \c -> do
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
    case eiR of
      Left  e -> return $ Fail $ show e
      Right r -> return r

  testPendingAcksFail :: Int -> IO TestResult
  testPendingAcksFail p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive, OMode F.Client] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      writeQ oQ nullType [] text1    -- send m1
      ok <- withTransaction c [OAbortMissingAcks] $ \_ -> do
        _ <- try $ readQ iQ
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
    case eiR of
      Left e  -> return $ Fail $ show e
      Right r -> return r

  testPendingAcksPass :: Int -> IO TestResult
  testPendingAcksPass p = do
    eiR <- try $ stdCon p $ \c -> do
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
    case eiR of
      Left e  -> return $ Fail $ show e
      Right r -> return r

  testAutoAck :: Int -> IO TestResult
  testAutoAck p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive, OMode F.Client, OAck] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      ok <- withTransaction c [OAbortMissingAcks] $ \_ -> do
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
    case eiR of
      Left e  -> return $ Fail $ show e
      Right r -> return r

  testQWithReceipt :: Int -> IO TestResult
  testQWithReceipt p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend, OWithReceipt, OWaitReceipt] [] oconv

      mbR <- tmo $ writeQWith oQ nullType [] text1
      case mbR of
        Nothing    -> return $ Fail "Broker sent no receipt!"
        Just NoRec -> return $ Fail "No receipt requested!"
        Just r     -> do
          mbM <- tmo $ readQ iQ
          case mbM of
            Nothing -> return $ Fail "No Message received"
            Just m  ->
              if msgCont m == text1
                then return Pass
                else return $ Fail $ 
                       "Received: " ++ (msgCont m) ++ " - Expected: " ++ text1
    case eiR of
       Left e  -> return $ Fail $ show e
       Right r -> return r

  testQWaitReceipt :: Int -> IO TestResult
  testQWaitReceipt p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend, OWithReceipt] [] oconv

      r   <- writeQWith oQ nullType [] text1
      mbM <- tmo $ readQ iQ
      case mbM of
        Nothing -> return $ Fail "No Message received"
        Just m  ->
          if msgCont m /= text1
            then return $ Fail $ 
                       "Received: " ++ (msgCont m) ++ " - Expected: " ++ text1
            else do
              mbOk <- tmo $ waitReceipt c r
              case mbOk of
                Nothing    -> return $ Fail "No receipt requested!"
                Just _     -> return Pass
    case eiR of
       Left  e -> return $ Fail $ show e
       Right r -> return r
       
  stdCon :: Int -> (Con -> IO TestResult) -> IO TestResult
  stdCon port act =
    withConnection host port maxRcv "guest" "guest" beat act
      
