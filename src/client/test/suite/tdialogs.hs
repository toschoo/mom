module Main
where

  import Test
  import Protocol (Message(..))

  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Parser 
  import           Network.Mom.Stompl.Client.Queue
  import           Network.Mom.Stompl.Client.Exception 

  import           Control.Exception (throwIO)
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
  oconv = OutBound (return . U.fromString)

  tmo :: IO a -> IO (Maybe a)
  tmo = timeout 100000

  text1, text2, text3, text4, utf8 :: String
  text1 = "whose woods these are I think I know"
  text2 = "his house is in the village though"
  text3 = "he will not see me stopping here"
  text4 = "to watch is woods fill up with snow"

  utf8  = "此派男野人です。\n" ++
          "此派男野人です。それ派個共です。\n" ++
          "ひと"

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
              Fail _ -> exitFailure
      _   -> do
        putStrLn "I need a port number"
        exitFailure

  runTests :: TestGroup IO -> IO TestResult
  runTests g = do
    (r, s) <- execGroup g
    putStrLn $ s 
    return r

  mkTests :: Int -> TestGroup IO
  mkTests p = 
    let t10  = mkTest "Create InQ               " $ testMkInQueue  p
        t20  = mkTest "Create OutQ              " $ testMkOutQueue p 
        t25  = mkTest "Create InQ wait Rc       " $ testMkInQueueWaitRc p 
        t26  = mkTest "Create InQ with Rc       " $ testMkInQueueWithRc p 
        t30  = mkTest "Send and Receive         " $ testSndRcv p
        t35  = mkTest "Unicode                  " $ testUTF8 p
        t40  = mkTest "Transaction              " $ testTx1 p
        t45  = mkTest "Abort                    " $ testAbort p
        t46  = mkTest "Abort on exception       " $ testTxException p
        t47  = mkTest "ForceTX                  " $ testForceTx p
        t50  = mkTest "Transaction pending ack  " $ testPendingAcksFail p
        t60  = mkTest "Transaction all ack'd    " $ testPendingAcksPass p
        t70  = mkTest "Auto Ack                 " $ testAutoAck p
        t80  = mkTest "With Receipt1            " $ testQWithReceipt p
        t90  = mkTest "Wait on Receipt1         " $ testQWaitReceipt p
        -- ackWith
        -- transaction with receipts
        t105 = mkTest "Pending Receipts no Tmo  " $ testTxReceiptsNoTmo p
        t110 = mkTest "Pending Receipts    Tmo  " $ testTxReceiptsTmo p
        t120 = mkTest "Nested Transactions      " $ testNestedTx p
        -- nested transaction with abort
        -- nested transaction with exception
    in  mkGroup "Dialogs" (Stop (Fail "")) 
        [t10, t20, t25, t26, t30, t35, t40, t45, t46, t47,
         t50, t60, t70, t80, t90, t105, t110, t120]

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

  testMkInQueueWaitRc :: Int -> IO TestResult
  testMkInQueueWaitRc p = do
    eiR <- try $ stdCon p $ \c -> do
      mbQ <- tmo $ newQueue c "IN"  tQ1 
                   [OReceive, OWaitReceipt] [] iconv
      case mbQ of
        Nothing -> return $ Fail $ "No Receipt"
        Just q  -> return Pass
    case eiR of
       Left e  -> return $ Fail $ show e
       Right r -> return r

  testMkInQueueWithRc :: Int -> IO TestResult
  testMkInQueueWithRc p = do
    eiR <- try $ stdCon p $ \c -> do
      mbQ <- tmo $ newQueue c "IN"  tQ1 
                   [OReceive, OWithReceipt] [] iconv
      case mbQ of
        Nothing -> return $ Fail $ "No Receipt"
        Just q  -> return Pass
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

  testUTF8 :: Int -> IO TestResult
  testUTF8 p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      writeQ oQ nullType [] utf8
      m <- readQ iQ
      if msgCont m == utf8
        then return Pass
        else return $ Fail $ "Received: " ++ (msgCont m) ++ " - Expected: " ++ utf8
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

  testAbort :: Int -> IO TestResult
  testAbort p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive, OMode F.Client] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv
      mbT <- try $ withTransaction c [OAbortMissingAcks] $ \_ -> do
        writeQ oQ nullType [] text1    -- send m1
        abort "Test Abort!!!"
      case mbT of
        Left (TxException _) -> do
          mbM <- tmo $ readQ iQ
          case mbM of
            Nothing -> return Pass
            Just _  -> return $ Fail $
                         "Aborted Transaction sends message!"
        Left  e -> return $ Fail $ "Unexpected exception: " ++ (show e)
        Right _ -> return $ Fail $ "Transaction not aborted!"
      
    case eiR of
      Left e  -> return $ Fail $ show e
      Right r -> return r

  testTxException :: Int -> IO TestResult
  testTxException p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive, OMode F.Client] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv
      mbT <- try $ withTransaction c [OAbortMissingAcks] $ \_ -> do
        writeQ oQ nullType [] text1    -- send m1
        throwIO $ AppException $ "Test Exception!!!"
      case mbT of
        Left (AppException _) -> do
          mbM <- tmo $ readQ iQ
          case mbM of
            Nothing -> return Pass
            Just _  -> return $ Fail $
                         "Aborted Transaction sends message!"
        Left  e -> return $ Fail $ "Unexpected exception: " ++ (show e)
        Right _ -> return $ Fail $ "Transaction not aborted!"
      
    case eiR of
      Left e  -> return $ Fail $ show e
      Right r -> return r

  testForceTx :: Int -> IO TestResult
  testForceTx p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend, OForceTx]    [] oconv
      withTransaction c [] $ \_ -> do
        writeQ oQ nullType [] text1    -- send m1
      mbM <- tmo $ readQ iQ
      case mbM of
        Nothing -> return $ Fail "No message received after commit"
        Just _  -> do
          eiX <- try $ writeQ oQ nullType [] text1
          case eiX of
            Left (QueueException _) -> return Pass
            Left e  -> return $ Fail $
                          "Unexpected exception: " ++ (show e)
            Right _ -> return $ Fail $
                          "No exception on writing to queue with OForceTX " ++
                          " outside transaction!"
    case eiR of
      Left e  -> return $ Fail $ show e
      Right r -> return r

  testPendingAcksFail :: Int -> IO TestResult
  testPendingAcksFail p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive, OMode F.Client] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      writeQ oQ nullType [] text1    -- send m1
      mbT <- try $ withTransaction c [OAbortMissingAcks] $ \_ -> do
        _ <- try $ readQ iQ
        return ()
      case mbT of
        Left (TxException _) -> return Pass
        Left  e              -> return $ Fail $ show e
        Right _              -> return $ Fail 
                                  "Transaction terminated with missing Acks!"
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
          Just _  -> return False
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
          Just _  -> return False
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

  testTxReceiptsNoTmo :: Int -> IO TestResult
  testTxReceiptsNoTmo p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      writeQ oQ nullType [] text1    -- send m1
      eiT <- try $ withTransaction c [OWithReceipts] $ \_ -> do
        _ <- readQ iQ               -- get  m1
        return ()
      case eiT of
        Left (TxException _) -> return Pass
        Left e               -> return $ Fail $ show e
        Right _              -> return $ Fail $ 
                                "Transaction terminated with missing receipts!"
    case eiR of
      Left e  -> return $ Fail $ show e
      Right _ -> return Pass

  testTxReceiptsTmo :: Int -> IO TestResult
  testTxReceiptsTmo p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      writeQ oQ nullType [] text1    -- send m1
      eiT <- try $ withTransaction c [OWithReceipts, OTimeout 100] $ \_ -> do
        _ <- readQ iQ               -- get  m1
        return ()
      case eiT of
        Left e               -> return $ Fail $ show e
        Right _              -> return $ Fail $ 
                                "Transaction terminated with Timeout!"
    case eiR of
      Left e  -> return $ Fail $ show e
      Right _ -> return Pass

  testQWithReceipt :: Int -> IO TestResult
  testQWithReceipt p = do
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend, OWithReceipt, OWaitReceipt] [] oconv

      mbR <- tmo $ writeQWith oQ nullType [] text1
      case mbR of
        Nothing    -> return $ Fail "Broker sent no receipt!"
        Just NoRec -> return $ Fail "No receipt requested!"
        Just _     -> do
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

  testNestedTx :: Int -> IO TestResult
  testNestedTx p = do 
    eiR <- try $ stdCon p $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv
      r  <- withTransaction c [] $ \_ -> do
        writeQ oQ nullType [] text1
        t <- ((testTx c iQ oQ) ?> 
              (testTx c iQ oQ) ?> 
              (testTx c iQ oQ))
        case t of
          Fail e -> return $ Fail e
          Pass   -> do
            mbM <- tmo $ readQ iQ 
            case mbM of
              Nothing -> return Pass
              Just _  -> return $ Fail "Message obtained before commit"
      case r of
        Fail e -> return $ Fail e
        Pass   -> do
          mbM <- tmo $ readQ iQ 
          case mbM of
            Nothing -> return $ Fail "Message not obtained after commit"
            Just m  -> 
              if msgCont m == text1 
                then return Pass
                else return $ Fail $ 
                       "Received: " ++ (msgCont m) ++ " - Expected: " ++ text1
    case eiR of
       Left  e -> return $ Fail $ show e
       Right r -> return r
      
  testTx :: Con -> Queue String -> Queue String -> IO TestResult
  testTx c iQ oQ = do
    eiT <- withTransaction c [] $ \_ -> do
      writeQ oQ nullType [] text2
      mbM <- tmo $ readQ iQ
      case mbM of
        Nothing -> return Pass
        Just _  -> return $ Fail "obtained messages sent in Tx before commit!"
    case eiT of
      Fail e -> return $ Fail e
      Pass   -> do
        mbM <- tmo $ readQ iQ
        case mbM of
          Nothing -> return $ Fail "Message not obtained after end of Tx!"
          Just m  -> 
            if msgCont m == text2
              then return Pass
              else return $ Fail $ 
                       "Received: " ++ (msgCont m) ++ " - Expected: " ++ text1

  stdCon :: Int -> (Con -> IO TestResult) -> IO TestResult
  stdCon port act =
    withConnection host port maxRcv "guest" "guest" beat act
      
