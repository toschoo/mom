module Main
where

  import Test
  import Protocol (Message(..))

  import           Network.Mom.Stompl.Client.Queue
  import           Network.Mom.Stompl.Client.Exception 

  import           Control.Exception (throwIO)
  import           Control.Concurrent

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.Maybe
  import           Data.Char (isDigit)
  import           Data.List (isSuffixOf)

  import           System.Exit
  import           System.Environment
  import           System.Timeout

  import           Codec.MIME.Type (nullType, Type)
  
  maxRcv :: Int
  maxRcv = 1024

  host :: String
  host = "127.0.0.1"

  beat :: Heart
  beat = (0,0)

  tQ1, tQ2 :: String
  tQ1 = "/q/test1"
  tQ2 = "/q/test2"

  iconv :: InBound String
  iconv _ _ _ = return . U.toString

  oconv :: OutBound String
  oconv = return . U.fromString

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
    let t10   = mkTest "Create Reader            " $ testWith p testMkInQueue 
        t20   = mkTest "Create Writer            " $ testWith p testMkOutQueue 
        t30   = mkTest "Create Reader wait Rc    " $ testWith p testMkInQueueWaitRc 
        t40   = mkTest "Create Reader with Rc    " $ testWith p testMkInQueueWithRc 
        t50   = mkTest "Send and Receive         " $ testWith p testSndRcv 
        t60   = mkTest "Unicode                  " $ testWith p testUTF8 
        t70   = mkTest "withReader               " $ testWith p testWithQueue 
        t80   = mkTest "Transaction              " $ testWith p testTx1 
        t90   = mkTest "Abort                    " $ testWith p testAbort 
        t100  = mkTest "Abort on exception       " $ testWith p testTxException 
        t110  = mkTest "ForceTX                  " $ testWith p testForceTx 
        t120  = mkTest "Transaction pending ack  " $ testWith p testPendingAcksFail 
        t130  = mkTest "Transaction all ack'd    " $ testWith p testPendingAcksPass 
        t140  = mkTest "Auto Ack                 " $ testWith p testAutoAck 
        t150  = mkTest "With Receipt1            " $ testWith p testQWithReceipt
        t160  = mkTest "Wait on Receipt1         " $ testWith p testQWaitReceipt
        -- t170 ackWith
        -- t180 nack
        -- t190 nackWith
        t200 = mkTest "Pending Receipts no Tmo  " $ testWith p testTxReceiptsNoTmo
        t210 = mkTest "Pending Receipts    Tmo  " $ testWith p testTxReceiptsTmo
        t220 = mkTest "Receipts not cleared     " $ testWith p testTxQRec
        t230 = mkTest "Receipts cleared         " $ testWith p testTxQRecWait
        t240 = mkTest "Recs not cleared + Tmo   " $ testWith p testTxQRecTmo
        t250 = mkTest "Nested Transactions      " $ testWith p testNestedTx
        t260 = mkTest "Nested Tx with foul one  " $ testWith p testNstTxAbort
        t270 = mkTest "Nested Tx one missing Ack" $ testWith p testNstTxMissingAck
        -- how to provoke an error at the broker?
        -- the library is designed to avoid that!
        t280 = mkTest "BrokerException          " $ testWith p testBrokerEx 
        -- t290 error handling: converter error
        -- t300 complex converter ok
        -- t310 share connection among threads
        -- t320 share queue among threads
        -- t330 share transaction among threads
        -- t340 multiple connections 
    in  mkGroup "Dialogs" (Stop (Fail "")) 
        [ t10,  t20,  t30,  t40,  t50,  t60,  t70, t80, t90, t100,
          t110, t120, t130, t140, t150, t160, t200,
          t210, t220, t230, t240, t250, t260, t270] 

  ------------------------------------------------------------------------
  -- Create a Reader without options
  ------------------------------------------------------------------------
  -- Shall create the reader
  -- Shall not preempt
  -- Shall not throw an exception
  ------------------------------------------------------------------------
  testMkInQueue :: Con -> IO TestResult
  testMkInQueue c = do
    eiQ <- try $ newReader c "IN"  tQ1 [] [] iconv
    case eiQ of
      Left  e -> return $ Fail $ show e
      Right _ -> return Pass

  ------------------------------------------------------------------------
  -- Create a Reader with option OWaitReceipt
  ------------------------------------------------------------------------
  -- Shall create the reader
  -- Shall preempt
  -- Shall not throw an exception
  ------------------------------------------------------------------------
  testMkInQueueWaitRc :: Con -> IO TestResult
  testMkInQueueWaitRc c = do
    mbQ <- tmo $ newReader c "IN"  tQ1 
                   [OWaitReceipt] [] iconv
    case mbQ of
      Nothing -> return $ Fail $ "No Receipt"
      Just _  -> return Pass

  ------------------------------------------------------------------------
  -- Create a Reader with otpion OWithReceipt
  ------------------------------------------------------------------------
  -- Shall create the reader
  -- Shall preempt
  -- Shall not throw an exception
  ------------------------------------------------------------------------
  testMkInQueueWithRc :: Con -> IO TestResult
  testMkInQueueWithRc c = do
    mbQ <- tmo $ newReader c "IN"  tQ1 
                   [OWithReceipt] [] iconv
    case mbQ of
      Nothing -> return $ Fail $ "No Receipt"
      Just _  -> return Pass

  ------------------------------------------------------------------------
  -- Create a Writer without otpions
  ------------------------------------------------------------------------
  -- Shall create the writer
  -- Shall not preempt
  -- Shall not throw an exception
  ------------------------------------------------------------------------
  testMkOutQueue :: Con -> IO TestResult
  testMkOutQueue c = do
    eiQ <- try $ newWriter c "OUT"  tQ1 [] [] oconv
    case eiQ of
      Left  e -> return $ Fail $ show e
      Right _ -> return Pass

  ------------------------------------------------------------------------
  -- Create Reader and a Writer without otpions
  -- Write and Receive a message
  ------------------------------------------------------------------------
  -- Shall create the writer
  -- Shall create the reader
  -- Shall not throw an exception
  -- Shall receive a message
  -- Received message shall equal sent message
  ------------------------------------------------------------------------
  testSndRcv :: Con -> IO TestResult
  testSndRcv c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 []    [] oconv
    writeQ oQ nullType [] text1
    m <- readQ iQ
    if msgCont m == text1
      then return Pass
      else return $ Fail $ "Received: " ++ (msgCont m) ++ " - Expected: " ++ text1

  ------------------------------------------------------------------------
  -- With Reader 
  ------------------------------------------------------------------------
  -- shall receive a message within its scope
  -- received message shall equal sent message
  -- shall throw an exception, when receiving out of its scope
  ------------------------------------------------------------------------
  testWithQueue :: Con -> IO TestResult
  testWithQueue c = do
    oQ  <- newWriter  c "OUT" tQ1 [] [] oconv
    eiQ <- withReader c "IN"  tQ1 [] [] iconv $ \q -> do
      writeQ oQ nullType [] text1 
      mbM1 <- tmo $ readQ q
      case mbM1 of
        Nothing -> return $ Left "Can't read from queue!"
        Just m  -> if msgContent m == text1 then return $ Right q
                     else return $ Left $ "Wrong message: " ++ 
                                            (msgContent m)
    case eiQ of
      Left  e -> return $ Fail e
      Right q -> do
        writeQ oQ nullType [] text2 
        mbM2 <- tmo $ try $ readQ q
        case mbM2 of
          Nothing   -> return Pass
          Just eiM  -> case eiM of
                         Left (QueueException _) -> return Pass
                         Left e -> return $ Fail $ 
                                       "Unexpected Exception: " ++
                                       (show e)
                         Right _ -> return $ Fail $                               
                                        "Received message from dead queue!" 

  ------------------------------------------------------------------------
  -- Unicode
  ------------------------------------------------------------------------
  -- shall create a reader
  -- shall create a writer
  -- shall receive a non-ASCII message 
  -- received message shall equal sent message
  ------------------------------------------------------------------------
  testUTF8 :: Con -> IO TestResult
  testUTF8 c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 []    [] oconv

    writeQ oQ nullType [] utf8
    m <- readQ iQ
    if msgCont m == utf8
      then return Pass
      else return $ Fail $ "Received: " ++ (msgCont m) ++ " - Expected: " ++ utf8

  ------------------------------------------------------------------------
  -- Transaction
  ------------------------------------------------------------------------
  -- shall create a reader
  -- shall create a writer
  -- shall not receive messages sent during transaction 
  --       during this transaction
  -- shall receive messages after transaction
  -- received messages shall equal sent messages
  -- messages shall be received in order in which they have been sent
  ------------------------------------------------------------------------
  testTx1 :: Con -> IO TestResult
  testTx1 c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 []    [] oconv
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

  ------------------------------------------------------------------------
  -- Abort
  ------------------------------------------------------------------------
  -- shall create a reader
  -- shall create a writer
  -- transaction shall throw AppException
  -- Abort message shall be suffix of AppException message
  -- message sent during transaction shall not be received
  ------------------------------------------------------------------------
  testAbort :: Con -> IO TestResult
  testAbort c = do
    let appmsg = "Test Abort!!!"
    iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
    oQ <- newWriter c "OUT" tQ1 []    [] oconv
    mbT <- try $ withTransaction c [] $ \_ -> do
        writeQ oQ nullType [] text1    -- send m1
        abort appmsg
    case mbT of
      Left (AppException e) -> do
        mbM <- tmo $ readQ iQ
        case mbM of
          Nothing -> if appmsg `isSuffixOf` e then return Pass
                       else return $ Fail $
                           "Exception text differs: " ++ e
          Just _  -> return $ Fail $
                         "Aborted Transaction sends message!"
      Left  e -> return $ Fail $ "Unexpected exception: " ++ (show e)
      Right _ -> return $ Fail $ "Transaction not aborted!"
      
  ------------------------------------------------------------------------
  -- Exception
  ------------------------------------------------------------------------
  -- shall create a reader
  -- shall create a writer
  -- transaction shall throw AppException
  -- app message shall be suffix of AppException message
  -- message sent during transaction shall not be received
  ------------------------------------------------------------------------
  testTxException :: Con -> IO TestResult
  testTxException c = do
    let appmsg = "Test Exception!!!"
    iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
    oQ <- newWriter c "OUT" tQ1 []    [] oconv
    mbT <- try $ withTransaction c [] $ \_ -> do
        writeQ oQ nullType [] text1    -- send m1
        throwIO $ AppException $ "Test Exception!!!"
    case mbT of
      Left (AppException e) -> do
        mbM <- tmo $ readQ iQ
        case mbM of
          Nothing -> if appmsg `isSuffixOf` e then return Pass
                       else return $ Fail $ 
                           "Exception text differs: " ++ e
          Just _  -> return $ Fail $
                         "Aborted Transaction sends message!"
      Left  e -> return $ Fail $ "Unexpected exception: " ++ (show e)
      Right _ -> return $ Fail $ "Transaction not aborted!"
      
  ------------------------------------------------------------------------
  -- Force Tx
  ------------------------------------------------------------------------
  -- shall create a reader
  -- shall create a writer with OForceTx
  -- message sent during transaction shall be received after tx
  -- sending message after tx shall throw QueueException
  ------------------------------------------------------------------------
  testForceTx :: Con -> IO TestResult
  testForceTx c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 [OForceTx]    [] oconv
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

  ------------------------------------------------------------------------
  -- Pending Acks Fail
  ------------------------------------------------------------------------
  -- shall create a reader with OMode Client
  -- message sent before transaction shall be received during tx
  -- tx shall throw TxException
  ------------------------------------------------------------------------
  testPendingAcksFail :: Con -> IO TestResult
  testPendingAcksFail c = do
    iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
    oQ <- newWriter c "OUT" tQ1 []    [] oconv
    writeQ oQ nullType [] text1    -- send m1
    mbT <- try $ withTransaction c [OAbortMissingAcks] $ \_ -> do
      _ <- try $ readQ iQ
      return ()
    case mbT of
      Left (TxException _) -> return Pass
      Left  e              -> return $ Fail $ show e
      Right _              -> return $ Fail 
                                  "Transaction terminated with missing Acks!"

  ------------------------------------------------------------------------
  -- Pending Acks Pass
  ------------------------------------------------------------------------
  -- shall create a reader with OMode Client
  -- message sent before transaction shall be received during tx
  -- tx shall not throw any exception
  ------------------------------------------------------------------------
  testPendingAcksPass :: Con -> IO TestResult
  testPendingAcksPass c = do
    iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
    oQ <- newWriter c "OUT" tQ1 []    [] oconv
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

  ------------------------------------------------------------------------
  -- Auto Ack 
  ------------------------------------------------------------------------
  -- shall create a reader with OMode Client and OAck
  -- message sent before transaction shall be received during tx
  -- tx shall not throw any exception
  ------------------------------------------------------------------------
  testAutoAck :: Con -> IO TestResult
  testAutoAck c = do
    iQ <- newReader c "IN"  tQ1 [OMode Client, OAck] [] iconv
    oQ <- newWriter c "OUT" tQ1 []    [] oconv
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

  ------------------------------------------------------------------------
  -- Tx with Receipts
  ------------------------------------------------------------------------
  -- tx started with OReceipts but without Timeout
  -- message sent before tx shall be received in tx
  -- tx shall throw TxException
  ------------------------------------------------------------------------
  testTxReceiptsNoTmo :: Con -> IO TestResult
  testTxReceiptsNoTmo c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
    writeQ oQ nullType [] text1    -- send m1
    eiT <- try $ withTransaction c [OWithReceipts] $ \_ -> do
        _ <- readQ iQ               -- get  m1
        return ()
    case eiT of
      Left (TxException _) -> return Pass
      Left e               -> return $ Fail $ show e
      Right _              -> return $ Fail $ 
                                "Transaction terminated with missing receipts!"

  ------------------------------------------------------------------------
  -- Tx with Receipts + Timeout
  ------------------------------------------------------------------------
  -- tx started with OReceipts and with Timeout
  -- message sent before tx shall be received in tx
  -- tx shall not throw any exception
  ------------------------------------------------------------------------
  testTxReceiptsTmo :: Con -> IO TestResult
  testTxReceiptsTmo c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
    writeQ oQ nullType [] text1    -- send m1
    eiT <- try $ withTransaction c [OWithReceipts, OTimeout 100] $ \_ -> do
        _ <- readQ iQ               -- get  m1
        return ()
    case eiT of
      Left e               -> return $ Fail $ show e
      Right _              -> return $ Pass

  ------------------------------------------------------------------------
  -- Tx with Receipts + pending receipt
  ------------------------------------------------------------------------
  -- shall create Writer with Receipt
  -- tx started with OReceipts and with Timeout
  -- message sent during tx shall be received in tx
  -- tx shall throw exception
  ------------------------------------------------------------------------
  testTxQRec :: Con -> IO TestResult
  testTxQRec c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 [OWithReceipt] [] oconv
    eiT <- try $ withTransaction c [OWithReceipts] $ \_ -> do
        writeQ oQ nullType [] text1    -- send m1
    case eiT of
      Left (TxException e) -> 
        if e == "Transaction aborted: Missing Receipts" 
          then do 
            mbM <- tmo $ readQ iQ
            case mbM of
              Nothing -> return Pass
              Just _  -> return $ Fail "message from aborted tx!"
          else return $ Fail $ "Wrong exception text: " ++ e
      Left e               -> return $ Fail $ show e
      Right _              -> return $ Fail 
                                "Transaction passed with missing receipts!"
  ------------------------------------------------------------------------
  -- Tx with Receipts + no pending receipt
  ------------------------------------------------------------------------
  -- shall create Writer with Receipt
  -- tx started with OReceipts and with Timeout
  -- message sent during tx shall be received in tx
  -- receipt shall be received by waitReceipt
  -- tx shall not throw any exception
  ------------------------------------------------------------------------
  testTxQRecWait :: Con -> IO TestResult
  testTxQRecWait c = do
    oQ <- newWriter c "OUT" tQ1 [OWithReceipt] [] oconv
    eiT <- try $ withTransaction c [OWithReceipts] $ \_ -> do
        r <- writeQWith oQ nullType [] text1 
        mbR <- tmo $ waitReceipt c r
        case mbR of 
          Nothing -> return $ Fail "No receipt!"
          Just _  -> return Pass
    case eiT of
      Left e  -> return $ Fail $ show e
      Right r -> return r

  ------------------------------------------------------------------------
  -- Tx with Receipts + Timeout and pending receipt
  ------------------------------------------------------------------------
  -- shall create Writer with Receipt
  -- tx started with OReceipts and with Timeout
  -- message sent during tx shall be received in tx
  -- tx shall not throw any exception
  ------------------------------------------------------------------------
  testTxQRecTmo :: Con -> IO TestResult
  testTxQRecTmo c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 [OWithReceipt] [] oconv
    eiT <- try $ withTransaction c [OWithReceipts, OTimeout 100] $ \_ -> do
        writeQ oQ nullType [] text1    -- send m1
    case eiT of
      Left e               -> return $ Fail $ show e
      Right _  -> do
        mbR <- tmo $ readQ iQ
        case mbR of
          Nothing -> return $ Fail "Did not receive message from tx!"
          Just _  -> return Pass

  ------------------------------------------------------------------------
  -- writeQWith
  ------------------------------------------------------------------------
  -- shall create reader with options OWithReceipt and OWaitReceipt
  -- writeQWith shall return receipt
  -- receipt shall not be NoRec
  -- message shall be received
  -- message shall equal sent message
  ------------------------------------------------------------------------
  testQWithReceipt :: Con -> IO TestResult
  testQWithReceipt c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 [OWithReceipt, OWaitReceipt] [] oconv

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

  ------------------------------------------------------------------------
  -- writeQWith and waitReceipt
  ------------------------------------------------------------------------
  -- shall create Reader with option OWithReceipt but without OWaitReceipt
  -- writeQWith shall return receipt
  -- receipt shall not be NoRec
  -- message shall be received
  -- message shall equal sent message
  -- waitReceipt shall not terminate with timeout
  ------------------------------------------------------------------------
  testQWaitReceipt :: Con -> IO TestResult
  testQWaitReceipt c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 [OWithReceipt] [] oconv
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

  ------------------------------------------------------------------------
  -- Broker Exception
  ------------------------------------------------------------------------
  testBrokerEx :: Con -> IO TestResult
  testBrokerEx c = do
    iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
    writeQ oQ nullType [] text1
    m <- readQ iQ
    ack c m
    eiA <- try $ ack c m
    case eiA of
      Left (BrokerException e) -> do
        putStrLn $ "BrokerException: " ++ (show e)
        return Pass
      Left e  -> return $ Fail $ "Unknown Exception: " ++ (show e)
      Right _ -> return $ Fail $ "Double Ack had no effect!"

  ------------------------------------------------------------------------
  -- NestedTx
  ------------------------------------------------------------------------
  -- shall start transaction
  -- shall run three tx within this tx 
  --   which shall different messages
  -- nested tx shall conclude successfully
  -- message sent during tx shall not be received during tx
  -- message sent during tx shall be received after tx
  -- message sent during tx shall equal sent message
  ------------------------------------------------------------------------
  testNestedTx :: Con -> IO TestResult
  testNestedTx c = do 
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
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

  ------------------------------------------------------------------------
  -- NestedTx with one foul
  ------------------------------------------------------------------------
  -- shall start transaction
  -- shall run three tx within this tx 
  -- the middle one shall abort
  -- the others shall conclude successfully
  -- message sent during tx shall not be received during tx
  -- message sent during tx shall be received after tx
  -- message sent during tx shall equal sent message
  ------------------------------------------------------------------------
  testNstTxAbort :: Con -> IO TestResult
  testNstTxAbort c = do 
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
    r  <- withTransaction c [] $ \_ -> do
        writeQ oQ nullType [] text1
        t <- ((testTx c iQ oQ)       ?> 
              (testNstAbort c iQ oQ) ?> 
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

  ------------------------------------------------------------------------
  -- NestedTx with foul one (missing ack)
  ------------------------------------------------------------------------
  -- shall start transaction
  -- shall run three tx within this tx 
  -- the middle one shall have missing acks
  -- the others shall conclude successfully
  -- message sent during tx shall not be received during tx
  -- message sent during tx shall be received after tx
  -- message sent during tx shall equal sent message
  ------------------------------------------------------------------------
  testNstTxMissingAck :: Con -> IO TestResult
  testNstTxMissingAck c = do 
    iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
    writeQ oQ nullType [] text2
    r  <- withTransaction c [] $ \_ -> do
        writeQ oQ nullType [] text1
        t <- ((testNstMissingAck c iQ) ?> -- note: this one has to run first
                                          --       it has to consume text2
                                          --       to get a missing ack!
              (testTx c iQ oQ)         ?> 
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
      
  ------------------------------------------------------------------------
  -- Simple Tx
  ------------------------------------------------------------------------
  -- shall start transaction
  -- message sent during tx shall not be received during tx
  -- message sent during tx shall be received after tx
  -- message sent during tx shall equal sent message
  ------------------------------------------------------------------------
  testTx :: Con -> Reader String -> Writer String -> IO TestResult
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

  ------------------------------------------------------------------------
  -- Nested Abort
  ------------------------------------------------------------------------
  -- transaction shall throw AppException
  -- Abort message shall be suffix of AppException message
  -- message sent during transaction shall not be received
  ------------------------------------------------------------------------
  testNstAbort :: Con -> Reader String -> Writer String -> IO TestResult
  testNstAbort c iQ oQ = do
    let appmsg = "Test Abort!!!"
    mbT <- try $ withTransaction c [] $ \_ -> do
      writeQ oQ nullType [] text1    -- send m1
      abort appmsg
    case mbT of
      Left (AppException e) -> do
        mbM <- tmo $ readQ iQ
        case mbM of
          Nothing -> if appmsg `isSuffixOf` e then return Pass
                       else return $ Fail $
                           "Exception text differs: " ++ e
          Just _  -> return $ Fail $
                         "Aborted Transaction sends message!"
      Left  e -> return $ Fail $ "Unexpected exception: " ++ (show e)
      Right _ -> return $ Fail $ "Transaction not aborted!"

  ------------------------------------------------------------------------
  -- Nested with pending acks
  ------------------------------------------------------------------------
  -- shall create a reader with OMode Client
  -- transaction shall throw TxException
  -- message sent during transaction shall not be received
  ------------------------------------------------------------------------
  testNstMissingAck :: Con -> Reader String -> IO TestResult
  testNstMissingAck c iQ = do
      eiT <- try $ withTransaction c [OAbortMissingAcks] $ \_ -> do
        mbM <- tmo $ readQ iQ
        case mbM of
          Nothing -> throwIO $ AppException "Message not received!"
          Just m  -> if msgContent m == text2 then return ()
                       else throwIO $ AppException $ "Wrong message: " 
                                      ++ (msgContent m)
      case eiT of
        Left (TxException _) -> return Pass
        Left  e              -> return $ Fail $ show e
        Right _              -> return $ Fail 
                                  "Transaction terminated with missing Acks!"

  ------------------------------------------------------------------------
  -- Standard Connection
  ------------------------------------------------------------------------
  -- shall connect with passed parameters
  ------------------------------------------------------------------------
  stdCon :: Int -> (Con -> IO TestResult) -> IO TestResult
  stdCon port act =
    withConnection host port maxRcv "guest" "guest" beat act

  testWith :: Int -> (Con -> IO TestResult) -> IO TestResult
  testWith port act = do
    eiR <- try $ stdCon port act
    case eiR of
       Left  e -> return $ Fail $ show e
       Right r -> return r
