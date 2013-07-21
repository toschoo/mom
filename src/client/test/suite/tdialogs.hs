{-# Language CPP #-}
module Main
where

  import Test

  import           Network.Mom.Stompl.Client.Queue

  import           Control.Exception (throwIO)
  import           Control.Concurrent
  import           Control.Monad (unless)

  import qualified Data.ByteString.UTF8  as U
  import           Data.Maybe
  import           Data.Char (isDigit)
  import           Data.List (isSuffixOf, isInfixOf)

  import           System.Exit
  import           System.Environment
  import           System.Timeout

  import           Codec.MIME.Type (nullType)
  
  maxRcv :: Int
  maxRcv = 1024

  host, nHost :: String
  host  = "127.0.0.1"
  nHost = "localhost"

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
#ifndef _DEBUG
  tmo = timeout 100000
#else
  tmo = timeout 500000
#endif

  text1, text2, text3, text4, utf8 :: String
  text1 = "whose woods these are I think I know"
  text2 = "his house is in the village though"
  text3 = "he will not see me stopping here"
  text4 = "to watch his woods fill up with snow"

  utf8  = "此派男野人です。\n" ++
          "此派男野人です。それ派個共です。\n" ++
          "ひと"

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [p] -> 
        if not (all isDigit p)
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
    putStrLn s 
    return r

  mkTests :: Int -> TestGroup IO
  mkTests p = 
    let t1    = mkTest "Connect with IP          " $ testIPConnect p
        t2    = mkTest "Connect with hostname    " $ testNConnect  p
        t3    = mkTest "Connect with Auth        " $ testAuth      p
        t10   = mkTest "Create Reader            " $ testWith p testMkInQueue 
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
        t120  = mkTest "Tx pending ack           " $ testWith p testPendingAcksFail 
        t130  = mkTest "Tx all ack'd             " $ testWith p testPendingAcksPass 
        t140  = mkTest "Auto Ack                 " $ testWith p testAutoAck 
        t150  = mkTest "With Receipt1            " $ testWith p testQWithReceipt
        t160  = mkTest "Wait on Receipt1         " $ testWith p testQWaitReceipt
        t170  = mkTest "ackWith                  " $ testWith p testTxAckWith
        -- stompserver and coilmq do not support nack!
        -- t180  = mkTest "Tx all nack'd            " $ testWith p testPendingNacksPass 
        -- t190  = mkTest "nackWith                 " $ testWith p testTxNackWith
        t200  = mkTest "Begin Rec no Tmo, fast   " $ testWith p testTxRecsNoTmoFail
        t205  = mkTest "Begin Rec no Tmo, slow   " $ testWith p testTxRecsNoTmoPass
        t210  = mkTest "Pending Receipts    Tmo  " $ testWith p testTxRecsTmo
        t220  = mkTest "Receipts not cleared     " $ testWith p testTxQRec
        t230  = mkTest "Receipts cleared         " $ testWith p testTxQRecWait
        t240  = mkTest "Recs not cleared + Tmo   " $ testWith p testTxQRecTmo
        t250  = mkTest "Nested Transactions      " $ testWith p testNestedTx
        t260  = mkTest "Nested Tx with foul one  " $ testWith p testNstTxAbort
        t270  = mkTest "Nested Tx one missing Ack" $ testWith p testNstTxMissingAck
        t280  = mkTest "BrokerException          " $ testWith p testBrokerEx 
        t290  = mkTest "Converter error          " $ testWith p testConvertEx
        t300  = mkTest "Cassandra Complex        " $ testWith p testConvComplex
        t310  = mkTest "Share connection         " $ testWith p testShareCon 
        t320  = mkTest "Share queue              " $ testWith p testShareQ 
        t330  = mkTest "Share tx                 " $ testWith p testShareTx
        t340  = mkTest "Share tx with abort      " $ testWith p testShareTxAbort
        t350  = mkTest "Nested con - independent " $ testNstConInd  p
        t360  = mkTest "Nested con - interleaved " $ testNstConX    p
        t370  = mkTest "Nested con - inner fails " $ testNstConFail p
        t375  = mkTest "Wait Broker              " $ testWaitBroker p
        t380  = mkTest "HeartBeat                " $ testBeat       p
        t390  = mkTest "HeartBeat Responder      " $ testBeatR      22222
        t400  = mkTest "HeartBeat Responder Fail " $ testBeatRfail  22222
    in  mkGroup "Dialogs" (Stop (Fail "")) 
        [ t1,   t2,   t3,
          t10,  t20,  t30,  t40,  t50,  t60,  t70,        t80,  t90, t100,
         t110, t120, t130, t140, t150, t160, t170,                   t200, t205,
         t210, t220, t230, t240, t250, t260, t270,       t280, t290, t300,
         t310, t320, t330, t340, t350, t360, t370, t375, t380, t390, t400] 

  ------------------------------------------------------------------------
  -- Connect with IP Address
  ------------------------------------------------------------------------
  -- Shall connect
  -- Shall not throw an exception
  ------------------------------------------------------------------------
  testIPConnect :: Int -> IO TestResult
  testIPConnect p = do
    eiC <- try $ withConnection "127.0.0.1" p [] (\_ -> return ())
    case eiC of
      Left  e -> return $ Fail $ show e
      Right _ -> return Pass

  ------------------------------------------------------------------------
  -- Connect with Host Name
  ------------------------------------------------------------------------
  -- Shall connect
  -- Shall not throw an exception
  ------------------------------------------------------------------------
  testNConnect :: Int -> IO TestResult
  testNConnect p = do
    eiC <- try $ withConnection "localhost" p [] (\_ -> return ())
    case eiC of
      Left  e -> return $ Fail $ show e
      Right _ -> return Pass

  ------------------------------------------------------------------------
  -- Connect with Authentication
  ------------------------------------------------------------------------
  -- Shall connect
  -- Shall not throw an exception
  ------------------------------------------------------------------------
  testAuth :: Int -> IO TestResult
  testAuth p = do
    eiC <- try $ withConnection "localhost" p 
                [OAuth "guest" "guest"] (\_ -> return ())
    case eiC of
      Left  e -> return $ Fail $ show e
      Right _ -> return Pass
  
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
      Nothing -> return $ Fail "No Receipt"
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
      Nothing -> return $ Fail "No Receipt"
      Just _  -> return Pass

  ------------------------------------------------------------------------
  -- Create a Writer without options
  ------------------------------------------------------------------------
  -- Shall create the writer
  -- Shall not preempt
  -- Shall not throw an exception
  ------------------------------------------------------------------------
  testMkOutQueue :: Con -> IO TestResult
  testMkOutQueue c = do
    eiQ <- try $ newWriter c "OUT"  tQ1 [] [] oconv
    -- putStrLn "Through!"
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
    if msgContent m == text1
      then return Pass
      else return $ Fail $ "Received: " ++ msgContent m ++ " - Expected: " ++ text1

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
                                            msgContent m
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
                                       "Unexpected Exception: " ++ show e
                         Right _ -> return $ Fail 
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
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
    writeQ oQ nullType [] utf8
    m <- readQ iQ
    if msgContent m == utf8
      then return Pass
      else return $ Fail $ "Received: " ++ msgContent m ++ " - Expected: " ++ utf8

  ------------------------------------------------------------------------
  -- Converter Error
  ------------------------------------------------------------------------
  -- shall create a reader
  -- shall create a writer
  -- shall receive message 
  -- converter shall throw ConvertException
  -- exception shall be caught
  ------------------------------------------------------------------------
  testConvertEx :: Con -> IO TestResult
  testConvertEx c = do
    let errmsg = "Bad Message!"
    let malconv _ _ _ _ = throwIO $ ConvertException errmsg
    iQ <- newReader c "IN"  tQ1 [] [] malconv
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
    writeQ oQ nullType [] utf8
    eiM <- try $ readQ iQ
    case eiM of
      Left (ConvertException e) -> 
        if errmsg `isSuffixOf` e then return Pass
            else return $ Fail $ "Wrong exception text: " ++ e 
      Left  e -> return $ Fail $ "Unexpected exception: " ++ show e
      Right m -> return $ Fail $ "Received message on exception: " ++ 
                                    msgContent m

  ------------------------------------------------------------------------
  -- Converter including more complex task
  ------------------------------------------------------------------------
  -- shall create a reader
  -- shall create a writer
  -- shall receive message 
  -- converter shall perform IO
  -- message shall be correct
  ------------------------------------------------------------------------
  testConvComplex :: Con -> IO TestResult
  testConvComplex c = do
    iQ <- newReader c "IN"  tQ1 [] [] cmpconv
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
    writeQ oQ nullType [] text1
    eiM <- try $ readQ iQ
    case eiM of
      Left  e -> return $ Fail $ "Unexpected exception: " ++ show e
      Right m -> 
        if msgContent m == text1 then return Pass
          else return $ Fail $ "Unexpected message: " ++ 
                                msgContent m
    where cmpconv _ _ _ b = do
            v <- newEmptyMVar 
            _ <- forkIO (putMVar v $ U.toString b)
            takeMVar v

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
        if length ms /= 4
          then return $ Fail $ "Did not receive all messages: " ++ show (length ms)
          else let zs = zip ms [text1, text2, text3, text4]
                   ts = map (\x -> (msgContent . fst) x == snd x) zs
               in if and ts then return Pass
                            else return $ Fail $
                                     "Messages are in wrong order: " ++ 
                                     show (map msgContent ms)

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
          Just _  -> return $ Fail
                         "Aborted Transaction sends message!"
      Left  e -> return $ Fail $ "Unexpected exception: " ++ show e
      Right _ -> return $ Fail   "Transaction not aborted!"
      
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
        throwIO $ AppException   "Test Exception!!!"
    case mbT of
      Left (AppException e) -> do
        mbM <- tmo $ readQ iQ
        case mbM of
          Nothing -> if appmsg `isSuffixOf` e then return Pass
                       else return $ Fail $ 
                           "Exception text differs: " ++ e
          Just _  -> return $ Fail
                         "Aborted Transaction sends message!"
      Left  e -> return $ Fail $ "Unexpected exception: " ++ show e
      Right _ -> return $ Fail   "Transaction not aborted!"
      
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
    withTransaction c [] $ \_ -> 
        writeQ oQ nullType [] text1    -- send m1
    mbM <- tmo $ readQ iQ
    case mbM of
      Nothing -> return $ Fail "No message received after commit"
      Just _  -> do
        eiX <- try $ writeQ oQ nullType [] text1
        case eiX of
          Left (QueueException _) -> return Pass
          Left e  -> return $ Fail $
                          "Unexpected exception: " ++ show e
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
            if msgContent m2 == text2 
              then return Pass
              else return $ Fail "Wrong message!"

  ------------------------------------------------------------------------
  -- Pending Nacks Pass
  ------------------------------------------------------------------------
  -- shall create a reader with OMode Client
  -- message sent before transaction shall be received during tx
  -- tx shall not throw any exception
  ------------------------------------------------------------------------
  testPendingNacksPass :: Con -> IO TestResult
  testPendingNacksPass c = do
    iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
    oQ <- newWriter c "OUT" tQ1 []    [] oconv
    writeQ oQ nullType [] text1    -- send m1
    ok <- withTransaction c [OAbortMissingAcks] $ \_ -> do
      m1 <- readQ iQ               -- get  m1
      writeQ oQ nullType [] text2  -- send m2
      mbM2 <- tmo $ readQ iQ       -- get  m2 (should be timeout)
      case mbM2 of 
        Nothing -> do
          nack c m1
          return True
        Just _  -> return False
    if not ok 
      then return $ Fail "obtained message sent in TX before commit!"
      else do
        mbM2 <- tmo $ readQ iQ
        case mbM2 of
          Nothing -> return $ Fail "no message from committed TX received!"
          Just m2 -> 
            if msgContent m2 == text2 
              then return Pass
              else return $ Fail "Wrong message!"

  ------------------------------------------------------------------------
  -- Pending Acks with Receipts Pass
  ------------------------------------------------------------------------
  -- shall create a reader with OMode Client
  -- message sent before transaction shall be received during tx
  -- tx shall not throw any exception
  ------------------------------------------------------------------------
  testTxAckWith :: Con -> IO TestResult
  testTxAckWith c = do
    iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
    oQ <- newWriter c "OUT" tQ1 []    [] oconv
    writeQ oQ nullType [] text1    -- send m1
    ok <- withTransaction c 
          [OAbortMissingAcks, OWithReceipts, OTimeout 100] $ \_ -> do
      m1 <- readQ iQ               -- get  m1
      writeQ oQ nullType [] text2  -- send m2
      mbM2 <- tmo $ readQ iQ       -- get  m2 (should be timeout)
      case mbM2 of 
        Nothing -> do
          ackWith c m1
          return True
        Just _  -> return False
    if not ok 
      then return $ Fail "obtained message sent in TX before commit!"
      else do
        mbM2 <- tmo $ readQ iQ
        case mbM2 of
          Nothing -> return $ Fail "no message from committed TX received!"
          Just m2 -> 
            if msgContent m2 == text2 
              then return Pass
              else return $ Fail "Wrong message!"

  ------------------------------------------------------------------------
  -- Pending Nacks with Receipts Pass
  ------------------------------------------------------------------------
  -- shall create a reader with OMode Client
  -- message sent before transaction shall be received during tx
  -- tx shall not throw any exception
  ------------------------------------------------------------------------
  testTxNackWith :: Con -> IO TestResult
  testTxNackWith c = do
    iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
    oQ <- newWriter c "OUT" tQ1 []    [] oconv
    writeQ oQ nullType [] text1    -- send m1
    ok <- withTransaction c 
          [OAbortMissingAcks, OWithReceipts, OTimeout 100] $ \_ -> do
      m1 <- readQ iQ               -- get  m1
      writeQ oQ nullType [] text2  -- send m2
      mbM2 <- tmo $ readQ iQ       -- get  m2 (should be timeout)
      case mbM2 of 
        Nothing -> do
          nackWith c m1
          return True
        Just _  -> return False
    if not ok 
      then return $ Fail "obtained message sent in TX before commit!"
      else do
        mbM2 <- tmo $ readQ iQ
        case mbM2 of
          Nothing -> return $ Fail "no message from committed TX received!"
          Just m2 -> 
            if msgContent m2 == text2 
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
            if msgContent m2 == text2 
              then return Pass
              else return $ Fail "Wrong message!"

  ------------------------------------------------------------------------
  -- Tx with Receipts, no Tmo fails (missing receipt for "begin")
  ------------------------------------------------------------------------
  -- tx started with OReceipts but without Timeout
  -- tx shall throw TxException
  -- Note: Relies on the fact that we terminate the transaction,
  --       before the broker is able to sent the commit receipt.
  ------------------------------------------------------------------------
  testTxRecsNoTmoFail :: Con -> IO TestResult
  testTxRecsNoTmoFail c = do
    eiT <- try $ withTransaction c [OWithReceipts] $ \_ -> return ()
    case eiT of
      Left (TxException e) -> 
        if e == "Transaction aborted: Missing Receipts" 
          then return Pass
          else return $ Fail $ "Wrong text in exception: " ++ e
      Left e               -> return $ Fail $ show e
      Right _              -> return $ Fail 
                                "Tx not aborted with pending receipt!"

  ------------------------------------------------------------------------
  -- Tx with Receipts, no Tmo passes
  ------------------------------------------------------------------------
  -- tx started with OReceipts but without Timeout
  -- tx shall throw TxException
  -- Note: Relies on the fact that we give some time to the broker
  --       to send the receipt, before we terminate the transaction,
  ------------------------------------------------------------------------
  testTxRecsNoTmoPass :: Con -> IO TestResult
  testTxRecsNoTmoPass c = do
    eiT <- try $ withTransaction c [OWithReceipts] $ \_ -> threadDelay 10000
    case eiT of
      Left e               -> return $ Fail $ show e
      Right _              -> return   Pass

  ------------------------------------------------------------------------
  -- Tx with Receipts + Tmo passes 
  ------------------------------------------------------------------------
  -- tx started with OReceipts and with Timeout
  -- tx shall not throw any exception
  ------------------------------------------------------------------------
  testTxRecsTmo :: Con -> IO TestResult
  testTxRecsTmo c = do
    eiT <- try $ withTransaction c [OWithReceipts, OTimeout 100] $ \_ -> return ()
    case eiT of
      Left e               -> return $ Fail $ show e
      Right _              -> return   Pass

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
    eiT <- try $ withTransaction c [OWithReceipts] $ \_ -> 
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
        r   <- writeQWith oQ nullType [] text1 
        mbR <- tmo $ waitReceipt c r
        case mbR of 
          Nothing -> return $ Fail "No receipt!"
          Just _  -> return Pass
    case eiT of
      Left  e -> return $ Fail $ show e
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
    eiT <- try $ withTransaction c [OWithReceipts, OTimeout 100] $ \_ -> 
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
            if msgContent m == text1
              then return Pass
              else return $ Fail $ 
                       "Received: " ++ msgContent m ++ " - Expected: " ++ text1

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
        if msgContent m /= text1
          then return $ Fail $ 
                       "Received: " ++ msgContent m ++ " - Expected: " ++ text1
          else do
            mbOk <- tmo $ waitReceipt c r
            case mbOk of
              Nothing    -> return $ Fail "No receipt requested!"
              Just _     -> return Pass

  ------------------------------------------------------------------------
  -- Broker Exception
  -- the trick is that some brokers do not support nack
  ------------------------------------------------------------------------
  testBrokerEx :: Con -> IO TestResult
  testBrokerEx c = do
    eiR <- try $ do 
      iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
      oQ <- newWriter c "OUT" tQ1 [] [] oconv
      writeQ oQ nullType [] text1
      m <- readQ iQ
      nack c m
      threadDelay 50000
    case eiR of
      Left (BrokerException   _) -> return Pass
      Left (ProtocolException e) -> 
        if "Peer disconnected" `isSuffixOf` e then return Pass
          else  
            return $ Fail $ "Not suffixOf: " ++ show e -- Unexpected Exception: " ++ (show e)
      Left e  -> return $ Fail $ "Unexpected Exception: " ++ show e
      Right _ -> return $ Fail "Double Ack had no effect!"

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
        t <- testTx c iQ oQ ?> 
             testTx c iQ oQ ?> 
             testTx c iQ oQ
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
            if msgContent m == text1 
              then return Pass
              else return $ Fail $ 
                       "Received: " ++ msgContent m ++ " - Expected: " ++ text1

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
        t <- testTx c iQ oQ       ?> 
             testNstAbort c iQ oQ ?> 
             testTx c iQ oQ
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
            if msgContent m == text1 
              then return Pass
              else return $ Fail $ 
                       "Received: " ++ msgContent m ++ " - Expected: " ++ text1

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
        t <- testNstMissingAck c iQ ?> -- note: this one has to run first
                                       --       it has to consume text2
                                       --       to get a missing ack!
             testTx c iQ oQ         ?> 
             testTx c iQ oQ
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
            if msgContent m == text1 
              then return Pass
              else return $ Fail $ 
                       "Received: " ++ msgContent m ++ " - Expected: " ++ text1

  ------------------------------------------------------------------------
  -- Share queue among threads
  ------------------------------------------------------------------------
  -- shall create reader and writer
  -- shall write two different messages
  -- shall fork a thread
  -- shall read in both threads
  -- read messages shall equal sent messages
  ------------------------------------------------------------------------
  testShareQ :: Con -> IO TestResult
  testShareQ c = do
    v  <- newEmptyMVar
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
    _  <- forkIO (readme v iQ)
    writeQ oQ nullType [] text1
    writeQ oQ nullType [] text2
    mbM <- tmo $ readQ iQ
    case mbM of
      Nothing -> return $ Fail "No message received!"
      Just m  -> do
        eiM2 <- takeMVar v
        case eiM2 of
          Left  e   -> return $ Fail e
          Right m2 -> 
            if (msgContent m  == text1  &&
                msgContent m2 == text2) ||
               (msgContent m  == text2  &&
                msgContent m2 == text1) 
              then return Pass
              else return $ Fail $ 
                            "Unexpected message: " ++
                            msgContent m
    where readme v q = do
            mbM <- tmo $ readQ q
            case mbM of
              Nothing -> putMVar v $ Left  "No message received!"
              Just m  -> putMVar v $ Right m

  ------------------------------------------------------------------------
  -- Share connection among threads
  ------------------------------------------------------------------------
  -- shall create a writer with OWithReceipt
  -- shall create a reader (to clean the queue)
  -- shall fork a thread
  -- thread shall receive the receipt
  ------------------------------------------------------------------------
  testShareCon :: Con -> IO TestResult
  testShareCon c = do
    v   <- newEmptyMVar
    oQ  <- newWriter c "OUT" tQ1 [OWithReceipt] [] oconv
    iQ  <- newReader c "IN"  tQ1 [] []             iconv
    rc  <- writeQWith oQ nullType [] text1
    _   <- forkIO (readme v rc)
    r   <- takeMVar v
    _   <- readQ iQ -- clean the queue
    return r
    where readme v r = do
            mbR <- tmo $ waitReceipt c r
            case mbR of
              Nothing -> putMVar v $ Fail "No Receipt!"
              Just _  -> putMVar v   Pass

  ------------------------------------------------------------------------
  -- Share tx among threads
  ------------------------------------------------------------------------
  -- shall create a writer 
  -- shall create a reader with OMode Client
  -- shall start tx 
  -- shall receive sent message
  -- shall fork a thread
  -- main thread shall wait for kid in tx
  -- kid shall try to ack message
  -- shall throw TxException
  ------------------------------------------------------------------------
  testShareTx :: Con -> IO TestResult
  testShareTx c = do
    v <- newEmptyMVar
    oQ  <- newWriter c "OUT" tQ1 []             [] oconv
    iQ  <- newReader c "IN"  tQ1 [OMode Client] [] iconv
    writeQ oQ nullType [] text1
    eiT <- try $ withTransaction c [OAbortMissingAcks] $ \_ -> do
      mbM <- tmo $ readQ iQ
      case mbM of
        Nothing -> return $ Fail "No message received!"
        Just m  -> do
          _ <- forkIO (ackMe v m)
          takeMVar v
    case eiT of
      Left  (TxException e) -> 
        if e == "Transaction aborted: Missing Acks" then return Pass
          else return $ Fail $ "Unexpected text in exception: " ++ e
      Left e  -> return $ Fail $ "Unexpected exception: " ++ show e
      Right _ -> return $ Fail "Transaction passed with missing acks!"
    where ackMe v m = do
            eiR <- try $ ack c m
            case eiR of
               Left e  -> putMVar v $ Fail $ 
                           "Unexpected exception: " ++ show e
               Right _ -> putMVar v Pass

  ------------------------------------------------------------------------
  -- Share tx with abort
  ------------------------------------------------------------------------
  -- shall create reader and writer
  -- shall start tx 
  -- shall sent message during tx
  -- shall fork a thread
  -- main thread shall wait for kid in tx
  -- kid shall try to abort
  -- kid shall receive AppException
  -- message sent in tx shall be received after tx in main thread
  ------------------------------------------------------------------------
  testShareTxAbort :: Con -> IO TestResult
  testShareTxAbort c = do
    let appmsg = "Abort!!!"
    v   <- newEmptyMVar
    oQ  <- newWriter c "OUT" tQ1 []             [] oconv
    iQ  <- newReader c "IN"  tQ1 [OMode Client] [] iconv
    eiT <- try $ withTransaction c [OAbortMissingAcks] $ \_ -> do
      writeQ oQ nullType [] text1
      mbM <- tmo $ readQ iQ
      case mbM of
        Nothing -> forkIO (abortMe v appmsg) >>= (\_ -> takeMVar v)
        Just _  -> return $ Fail "Message received before end of Tx!"
    case eiT of
      Left  e -> return $ Fail $ "Unexpected exception: " ++ show e
      Right r -> do 
        mbM <- tmo $ readQ iQ
        case mbM of
          Nothing -> return $ Fail "Transaction aborted!"
          Just m  -> if msgContent m == text1 then return r
                       else return $ Fail $ "Unexpected message: " ++
                                         msgContent m
    where abortMe v msg = do
            eiR <- try $ abort msg
            case eiR of
               Left (AppException e) ->
                 if msg `isSuffixOf` e then putMVar v Pass
                   else putMVar v $ Fail $ 
                           "Unexpected text in exception: " ++ e 
               Left e  -> putMVar v $ Fail $ 
                           "Unexpected exception: " ++ show e
               Right _ -> putMVar v $ Fail "Abort did not fire!"

  ------------------------------------------------------------------------
  -- Nested Connection Independent
  ------------------------------------------------------------------------
  -- shall create a connection
  -- shall create a reader and a writer
  -- shall send a message
  -- shall create a second connection
  -- shall create different readers and writers
  -- shall send message in second con
  -- shall receive message in second con
  -- shall receive message in first con
  ------------------------------------------------------------------------
  testNstConInd :: Int -> IO TestResult
  testNstConInd p = do
    eiR <- try $ stdCon p $ \c -> do
      oQ  <- newWriter c "OUT" tQ1 [] [] oconv
      iQ  <- newReader c "IN"  tQ1 [] [] iconv
      writeQ oQ nullType [] text1
      eiR2 <- try $ stdCon p $ \c2 -> do
        oQ2 <- newWriter c2 "OUT" tQ2 [] [] oconv
        iQ2 <- newReader c2 "IN"  tQ2 [] [] iconv
        writeQ oQ2 nullType [] text2
        mbM2 <- tmo $ readQ iQ2
        case mbM2 of
          Nothing -> return $ Fail "No message received (2)"
          Just m  -> if msgContent m == text2 then return Pass
                       else return $ Fail $ "Unexpected message: " ++ 
                                      msgContent m
      case eiR2 of
        Left  e -> return $ Fail $ "Unexpected exception: " ++ show e
        Right r -> case r of 
                     Pass -> do
                       mbM <- tmo $ readQ iQ
                       case mbM of
                         Nothing -> return $ Fail "No message received!"
                         Just m  -> if msgContent m == text1 
                                      then return Pass
                                      else return $ Fail $ 
                                             "Unexpected message " ++ 
                                             msgContent m
                     Fail e -> return $ Fail e
    case eiR of
       Left  e -> return $ Fail $ show e
       Right r -> return r

  ------------------------------------------------------------------------
  -- Nested Connection crossing
  ------------------------------------------------------------------------
  -- shall create a connection
  -- shall create a reader and a writer
  -- shall send a message
  -- shall create a second connection
  -- shall receive message in second con using first reader
  -- shall send message in second con using first writer
  -- shall receive message in first con
  ------------------------------------------------------------------------
  testNstConX :: Int -> IO TestResult
  testNstConX p = do
    eiR <- try $ stdCon p $ \c -> do
      oQ  <- newWriter c "OUT" tQ1 [] [] oconv
      iQ  <- newReader c "IN"  tQ1 [] [] iconv
      writeQ oQ nullType [] text1
      eiR2 <- try $ stdCon p $ \_ -> do
        mbM2 <- tmo $ readQ iQ
        case mbM2 of
          Nothing -> return $ Fail "No message received (2)"
          Just m  -> 
            if msgContent m /= text1 
              then return $ Fail $ "Unexpected message: " ++ 
                                   msgContent m
              else do
                writeQ oQ nullType [] text2
                return Pass
      case eiR2 of
        Left  e -> return $ Fail $ "Unexpected exception: " ++ show e
        Right r -> case r of 
                     Fail e -> return $ Fail e
                     Pass -> do
                       mbM <- tmo $ readQ iQ
                       case mbM of
                         Nothing -> return $ Fail "No message received!"
                         Just m  -> if msgContent m == text2 
                                      then return Pass
                                      else return $ Fail $ 
                                             "Unexpected message " ++ 
                                             msgContent m
    case eiR of
       Left  e -> return $ Fail $ show e
       Right r -> return r

  ------------------------------------------------------------------------
  -- Nested Connection with Failure
  ------------------------------------------------------------------------
  -- shall create a connection
  -- shall create a reader and a writer
  -- shall send a message
  -- shall create a second connection
  -- shall throw Exception in inner
  -- shall receive message in first con
  ------------------------------------------------------------------------
  testNstConFail :: Int -> IO TestResult
  testNstConFail p = do
    let appmsg = "Bad Connection!"
    eiR <- try $ stdCon p $ \c -> do
      oQ  <- newWriter c "OUT" tQ1 [] [] oconv
      iQ  <- newReader c "IN"  tQ1 [] [] iconv
      writeQ oQ nullType [] text1
      eiR2 <- try $ stdCon p $ \_ -> throwIO $ AppException appmsg
      case eiR2 of
        Left  (AppException e) -> 
          if not $ appmsg `isSuffixOf` e 
            then return $ Fail $ "Unexpected text in exception: " ++ e
            else do 
              mbM <- tmo $ readQ iQ
              case mbM of
                Nothing -> return $ Fail "No message received!"
                Just m  -> if msgContent m == text1
                             then return Pass
                             else return $ Fail $ 
                                           "Unexpected message " ++ 
                                           msgContent m
        Left  e -> return $ Fail $ "Unexpected exception: " ++ show e
        Right _ -> return $ Fail   "No exception was thrown!"
    case eiR of
       Left  e -> return $ Fail $ show e
       Right r -> return r

  ------------------------------------------------------------------------
  -- Test wait for Broker - probably fails, depending on broker
  ------------------------------------------------------------------------
  -- connect with WaitBroker
  -- shall not throw exception
  -- BUT: will probably throw an exception
  --      since most broker do not send the final receipt
  --      or close the socket immediately after sending the receipt
  --      so we accept the case, where the socket is closed 
  --      by the broker, instead of sending a receipt
  ------------------------------------------------------------------------
  testWaitBroker :: Int -> IO TestResult
  testWaitBroker p = do
    eiR <- try $ withConnection host p 
                               [OWaitBroker 100] $ \_ -> return Pass
    case eiR of
       Left (ProtocolException e) -> 
         if "Peer disconnected" `isSuffixOf` e then return Pass
           else return $ Fail $ "Wrong Exception: " ++  e
       Left  e -> return $ Fail $ show e
       Right r -> return r
  
  ------------------------------------------------------------------------
  -- Simple HeartBeat Test
  ------------------------------------------------------------------------
  -- connect with heartbeat
  -- delay thread longer than heartbeat
  -- then do something
  -- delay thread longer than heartbeat
  -- shall not throw exception
  ------------------------------------------------------------------------
  testBeat :: Int -> IO TestResult
  testBeat p = do
    let b = (500,500)
    eiR <- try $ withConnection host p [OHeartBeat b] $ \c -> do
      oQ  <- newWriter c "OUT" tQ1 [] [] oconv
      iQ  <- newReader c "IN"  tQ1 [] [] iconv
      threadDelay $ 1000 * 1000
      writeQ oQ nullType [] text1
      mbM <- tmo $ readQ iQ
      case mbM of
        Nothing -> return $ Fail "No message received!"
        Just m  -> if msgContent m /= text1 
                     then return $ Fail $ "Unexpected message: " ++ 
                                   msgContent m
                     else do threadDelay $ 1000 * 1000
                             return Pass
    case eiR of
       Left  e -> return $ Fail $ show e
       Right r -> return r

  ------------------------------------------------------------------------
  -- HeartBeat Test with Responder 
  -- (which will "mirror" heartbeats) 
  ------------------------------------------------------------------------
  -- connect with heartbeat
  -- delay thread longer than heartbeat
  -- shall not throw exception
  ------------------------------------------------------------------------
  testBeatR :: Int -> IO TestResult
  testBeatR p = do
    let b = (100,100)
    eiR <- try $ withConnection host p [OHeartBeat b] $ \_ -> do
      threadDelay $ 1000 * 1000
      return Pass
    case eiR of
       Left  e -> return $ Fail $ show e
       Right r -> return r

  ------------------------------------------------------------------------
  -- HeartBeat Test with Responder 
  -- (which will ignore heartbeats) 
  ------------------------------------------------------------------------
  -- connect with heartbeat
  -- delay thread longer than heartbeat
  -- shall throw exception
  ------------------------------------------------------------------------
  testBeatRfail :: Int -> IO TestResult
  testBeatRfail p = do
    let b = (50,50) -- this beat signals responder to ignore heartbeats
    eiR <- try $ withConnection host p [OHeartBeat b] $ \_ -> do
      threadDelay $ 1000 * 1000
      return Pass
    case eiR of
       Right _ -> return $ Fail "heartbeats do not matter!"
       Left (ProtocolException e) ->
         if "Missing HeartBeat," `isInfixOf` e then return Pass
           else return $ Fail $ "Unexpected Protocol Exception: " ++ e
       Left  e -> return $ Fail $ "Unexpected Exception: " ++ show e

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
            if msgContent m == text2
              then return Pass
              else return $ Fail $ 
                       "Received: " ++ msgContent m ++ " - Expected: " ++ text1

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
          Just _  -> return $ Fail "Aborted Transaction sends message!"
      Left  e -> return $ Fail $   "Unexpected exception: " ++ show e
      Right _ -> return $ Fail     "Transaction not aborted!"

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
          Just m  -> unless (msgContent m == text2) $
                       throwIO $ AppException $ "Wrong message: "
                                 ++ msgContent m
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
  stdCon port = withConnection host port []

  testWith :: Int -> (Con -> IO TestResult) -> IO TestResult
  testWith port act = do
    eiR <- try $ stdCon port act
    case eiR of
       Left  e -> return $ Fail $ show e
       Right r -> return r
