{-# Language CPP #-}
module Main
where

  import Test

  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Client.Queue

  import           Network.Connection 
  import           Data.Conduit.Network.TLS

  import           Control.Exception (catch, throwIO, SomeException)
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

  port, sPort :: Int
  port  = 61613
  sPort = 61619

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
    os  <- getArgs
    let eiP = case os of
               [p] -> 
                 if not (all isDigit p)
                   then Left $ "Port number '" ++ p ++ "' is not numeric!"
                   else Right (read p)
               _  -> Right port
    case eiP of
      Left  e -> error e
      Right p -> do
        r <- runTests $ mkTests p
        case r of
          Pass   -> exitSuccess
          Fail _ -> exitFailure

  runTests :: TestGroup IO -> IO TestResult
  runTests g = do
    (r, s) <- execGroup g
    putStrLn s 
    return r

  mkTests :: Int -> TestGroup IO
  mkTests p = 
    let t1    = mkTest "Connect with IP          " $ testIPConnect p
        t2    = mkTest "Connect with hostname    " $ testNConnect  F.Connect p
        -- stompserver does not support stomp frame!
        -- t3    = mkTest "Connect with Stomp       " $ testNConnect  F.Stomp   p
        t4    = mkTest "Connect with Auth        " $ testAuth      p
        t5    = mkTest "Connect with timeout     " $ testTmoOk     p
        -- the trick in this one was to provide a very short timeout (1ms)
        -- when I wrote the test it was enough to cause a timeout
        -- with 2020 hardware, that won't work anymore...
        -- t6    = mkTest "Connect with timeout fail" $ testTmoFail   p
        t10   = mkTest "Create Reader            " $ testWith p testMkInQueue 
        t510  = mkTest "Create Reader (secure)   " $ testSecure sPort testMkInQueue 
        t20   = mkTest "Create Writer            " $ testWith p testMkOutQueue 
        t520  = mkTest "Create Writer (secure)   " $ testSecure sPort testMkOutQueue 
        t30   = mkTest "Create Reader wait Rc    " $ testWith p testMkInQueueWaitRc 
        t530  = mkTest "Create Reader (secure)   " $ testSecure sPort testMkInQueueWaitRc
        t40   = mkTest "Create Reader with Rc    " $ testWith p testMkInQueueWithRc 
        t540  = mkTest "Create Reader with Rc    " $ testSecure sPort testMkInQueueWithRc 
        t50   = mkTest "Send and Receive         " $ testWith p testSndRcv 
        t550  = mkTest "Send and Receive (secure)" $ testSecure sPort testSndRcv 
        t60   = mkTest "Unicode                  " $ testWith p testUTF8 
        t560  = mkTest "Unicode (secure)         " $ testSecure sPort testUTF8 
        t70   = mkTest "withReader               " $ testWith p testWithQueue 
        t570  = mkTest "withReader (secure)      " $ testSecure sPort testWithQueue 
        t75   = mkTest "No Content Length        " $ testWith p testNoContentLen
        t575  = mkTest "No Content Length (sec.) " $ testSecure sPort testNoContentLen
        t76   = mkTest "Content Length           " $ testWith p testContentLen
        t576  = mkTest "Content Length (sec.)    " $ testSecure sPort testContentLen
        t80   = mkTest "Transaction              " $ testWith p testTx1 
        t580  = mkTest "Transaction (sec.)       " $ testSecure sPort testTx1 
        t90   = mkTest "Abort                    " $ testWith p testAbort 
        t100  = mkTest "Abort on exception       " $ testWith p testTxException 
        t110  = mkTest "ForceTX                  " $ testWith p testForceTx 
        t120  = mkTest "Tx pending ack           " $ testWith p testPendingAcksFail 
        t130  = mkTest "Tx all ack'd             " $ testWith p testPendingAcksPass 
        t140  = mkTest "Auto Ack                 " $ testWith p testAutoAck 
        t150  = mkTest "With Receipt1            " $ testWith p testQWithReceipt
        t590  = mkTest "With Receipt1 (sec.)     " $ testSecure sPort testQWithReceipt
        t160  = mkTest "Wait on Receipt1         " $ testWith p testQWaitReceipt
        t600  = mkTest "Wait on Receipt1 (sec.)  " $ testSecure sPort testQWaitReceipt
        t170  = mkTest "ackWith                  " $ testWith p testTxAckWith
        t610  = mkTest "ackWith (sec.)           " $ testSecure sPort testTxAckWith
        t175  = mkTest "frmToMsg with ack header " $ testWith p (testFrmToMsg True)
        t620  = mkTest "frmToMsg w ack h. (sec.) " $ testSecure sPort (testFrmToMsg True)
        t176  = mkTest "frmToMsg w/o  ack header " $ testWith p (testFrmToMsg False)
        t630  = mkTest "frmToMsg w/o ack h (sec.)" $ testSecure sPort (testFrmToMsg False)
        -- stompserver and coilmq do not support nack!
        -- t180  = mkTest "Tx all nack'd            " $ testWith p testPendingNacksPass 
        -- t190  = mkTest "nackWith                 " $ testWith p testTxNackWith
        t200  = mkTest "Begin Rec no Tmo, fast   " $ testWith p testTxRecsNoTmoFail
        t640  = mkTest "Begin Rec no Tmo, fast se" $ testSecure sPort testTxRecsNoTmoFail
        t205  = mkTest "Begin Rec no Tmo, slow   " $ testWith p testTxRecsNoTmoPass
        t650  = mkTest "Begin Rec no Tmo, slow se" $ testSecure sPort testTxRecsNoTmoPass
        t210  = mkTest "Pending Receipts    Tmo  " $ testWith p testTxRecsTmo
        t220  = mkTest "Receipts not cleared     " $ testWith p testTxQRec
        t230  = mkTest "Receipts cleared         " $ testWith p testTxQRecWait
        t240  = mkTest "Recs not cleared + Tmo   " $ testWith p testTxQRecTmo
        t250  = mkTest "Nested Transactions      " $ testWith p testNestedTx
        t660  = mkTest "Nested Transactions (sec)" $ testSecure sPort testNestedTx
        t260  = mkTest "Nested Tx with foul one  " $ testWith p testNstTxAbort
        t670  = mkTest "Nested Tx with foul (sec)" $ testSecure sPort testNstTxAbort
        t270  = mkTest "Nested Tx one missing Ack" $ testWith p testNstTxMissingAck
        t680  = mkTest "Nested Tx one m. Ack (se)" $ testSecure sPort testNstTxMissingAck
        t280  = mkTest "BrokerException          " $ testWith p testBrokerEx 
        t285  = mkTest "Error Handler            " $ testError1 p
        t286  = mkTest "No Error Handler         " $ testError2 p
        -- Nice test, but:
        -- since we are working with a bridge, the connection will not be closed
        -- after the error message -- and we only have one exception...
        -- t690  = mkTest "BrokerException (sec.)   " $ testSecure sPort testBrokerEx 
        t290  = mkTest "Converter error          " $ testWith p testConvertEx
        t700  = mkTest "Converter error (sec.)   " $ testSecure sPort testConvertEx
        t300  = mkTest "Cassandra Complex        " $ testWith p testConvComplex
        t710  = mkTest "Cassandra Complex (sec.) " $ testSecure sPort testConvComplex
        t310  = mkTest "Share connection         " $ testWith p testShareCon 
        t720  = mkTest "Share connection (sec.)  " $ testSecure sPort testShareCon 
        t320  = mkTest "Share queue              " $ testWith p testShareQ 
        t730  = mkTest "Share queue (sec.)       " $ testSecure sPort testShareQ 
        t330  = mkTest "Share tx                 " $ testWith p testShareTx
        t740  = mkTest "Share tx (sec.)          " $ testSecure sPort testShareTx
        t340  = mkTest "Share tx with abort      " $ testWith p testShareTxAbort
        t750  = mkTest "Share tx abort (sec.)    " $ testSecure sPort testShareTxAbort
        t350  = mkTest "Nested con - independent " $ testNstConInd  p
        t360  = mkTest "Nested con - interleaved " $ testNstConX    p
        t370  = mkTest "Nested con - inner fails " $ testNstConFail p
        -- Socket closed, before receipt is received... 
        -- t375  = mkTest "Wait Broker              " $ testWaitBroker p
        t380  = mkTest "HeartBeat                " $ testBeat       p
        t390  = mkTest "HeartBeat Responder      " $ testBeatR      22222
        t400  = mkTest "HeartBeat Responder Fail " $ testBeatRfail  22222
        t410  = mkTest "Exception in worker      " $ testExc        22222
        t420  = mkTest "Continue after exception " $ testExc2       22222
        t430  = mkTest "Worker terminates        " $ testWith p testDeadWorker
    in  mkGroup "Dialogs" (Stop (Fail "")) 
        [ t1,   t2,          t4,   t5,
          t10,  t20,  t30,  t40,  t50,  t60,  t70,  t75, t76,  t80,  t90, t100,
         t110, t120, t130, t140, t150, t160, t170, t175, t176,       t200, t205,
         t210, t220, t230, t240, t250, t260, t270, t280, t285, t286, t290, t300,
         t310, t320, t330, t340, t350, t360, t370, t380, t390, t400,
         t410, t420, t430,
         -- secure tests
         t510, t520, t530, t540, t550, t560, t570, t575, t576, t580, t590, t600,
         t610, t620, t630, t640, t650, t660, t670,             t680,       t700,
         t710, t720, t730, t740, t750] 

  ------------------------------------------------------------------------
  -- Connect with IP Address
  ------------------------------------------------------------------------
  -- Shall connect
  -- Shall not throw an exception
  ------------------------------------------------------------------------
  testIPConnect :: Int -> IO TestResult
  testIPConnect p = do
    eiC <- try $ withConnection "127.0.0.1" p [] [] (\_ -> return ())
    case eiC of
      Left  e -> return $ Fail $ show e
      Right _ -> return Pass

  ------------------------------------------------------------------------
  -- Connect with Host Name and either Connect or Stomp frame
  ------------------------------------------------------------------------
  -- Shall connect
  -- Shall not throw an exception
  ------------------------------------------------------------------------
  testNConnect :: F.FrameType -> Int -> IO TestResult
  testNConnect t p = do
    let os = case t of 
               F.Stomp -> [OStomp]
               _       -> []
    eiC <- try $ withConnection "localhost" p os [] (\_ -> return ())
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
                [OAuth "guest" "guest"] [] (\_ -> return ())
    case eiC of
      Left  e -> return $ Fail $ show e
      Right _ -> return Pass

  ------------------------------------------------------------------------
  -- Connect with Timeout
  ------------------------------------------------------------------------
  -- Shall connect
  -- Shall not throw an exception
  ------------------------------------------------------------------------
  testTmoOk :: Int -> IO TestResult
  testTmoOk p = do
    eiC <- try $ withConnection "localhost" p 
                 [OTmo 100] [] (\_ -> return ())
    case eiC of
      Left  e -> return $ Fail $ show e
      Right _ -> return Pass

  ------------------------------------------------------------------------
  -- Connect with Timeout, fail
  ------------------------------------------------------------------------
  -- Shall connect
  -- Shall throw an exception
  ------------------------------------------------------------------------
  testTmoFail :: Int -> IO TestResult
  testTmoFail p = do
    eiC <- try $ withConnection "localhost" p
                 [OTmo 1] [] (\_ -> return ())
    case eiC of
      Left  e -> case e of
                   ConnectException _ -> return Pass
                   _                  -> return $ Fail $ show e
      Right _ -> return $ Fail "timeout ignored"
  
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
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
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
  -- Send with ONoContentLen
  ------------------------------------------------------------------------
  -- shall create a reader
  -- shall create a writer
  -- shall receive message 
  -- message shall be correct
  -- message shall not contain "content-length" header
  ------------------------------------------------------------------------
  testNoContentLen :: Con -> IO TestResult
  testNoContentLen c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv
    oQ <- newWriter c "OUT" tQ1 [ONoContentLen] [] oconv
    writeQ oQ nullType [] text1
    eiM <- try $ readQ iQ
    case eiM of
      Left  e -> return $ Fail $ "Unexpected exception: " ++ show e
      Right m -> do
        if msgContent m == text1 && msgLen m <= 0
          then case lookup "content-length" $ msgHdrs m of
                 Nothing -> return Pass
                 Just l  -> return $ Fail $ "conent-length: " ++ l
          else return $ Fail $ "Unexpected message: " ++ 
                                msgContent m ++ " - expected: " ++ text1

  ------------------------------------------------------------------------
  -- Send without ONoContentLen
  ------------------------------------------------------------------------
  -- shall create a reader
  -- shall create a writer
  -- shall receive message 
  -- message shall be correct
  -- message shall contain "content-length" 
  ------------------------------------------------------------------------
  testContentLen :: Con -> IO TestResult
  testContentLen c = do
    iQ <- newReader c "IN"  tQ1 [] [] iconv 
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
    writeQ oQ nullType [] text1
    eiM <- try $ readQ iQ
    case eiM of
      Left  e -> return $ Fail $ "Unexpected exception: " ++ show e
      Right m -> do
        if msgContent m == text1 && msgLen m > 0
          then return Pass
          else return $ Fail $ "Unexpected message: " ++ 
                                msgContent m ++ " or message length: " ++
                                show (msgLen m)

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
    oQ <- newWriter c "OUT" tQ1 [] [] oconv
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

  testFrmToMsg :: Bool -> Con -> IO TestResult
  testFrmToMsg a c = 
    withReader c "IN" tQ1 [] [] (\_ _ _ -> return . U.toString) $ \r ->
      let frm = F.mkMessage "" tQ1 "msg-1" (if a then "ack-1" else "")
                            nullType 5 [] (U.fromString "hello")
       in do m <- frmToMsg r frm
             case msgAck m of
               "ack-1" | a         -> return Pass
                       | otherwise -> return $ 
                        Fail "there should be no ack header!"
               "msg-1" | a         -> return $ 
                           Fail "ack read from message-id!"
                       | otherwise -> return Pass
               k                   -> return $
                           Fail $ "unexpected ack header: '" ++ k ++ "'"


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
  ------------------------------------------------------------------------
  -- the trick is that some brokers create an error message on nack
  -- we need two exception handlers,
  -- because the broker will close the connection
  -- after creating an error message, which will cause
  -- a "receiver terminated" exception.
  -- But that is not the one, we want to handle!
  ------------------------------------------------------------------------
  testBrokerEx :: Con -> IO TestResult
  testBrokerEx c = do
      iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
      oQ <- newWriter c "OUT" tQ1 [] [] oconv
      x  <- newMVar (0::Int)
      catch (action iQ oQ x
               >> threadDelay 1000000 
               >> return (Fail "Nack had no effect"))
            (handler1 x)
    where action iQ oQ x = do
            writeQ oQ nullType [] text1
            m <- readQ iQ
            catch (nack c m >> threadDelay 1000000) (handler0 x)
          handler0 x e = 
            case e of
              WorkerException _ -> modifyMVar_ x (\_ -> return 1) 
              BrokerException _ -> modifyMVar_ x (\_ -> return 2)
              _                 -> putStrLn (
                                     "Unexpected Exception: " ++ show e) 
                                   >> modifyMVar_ x (\_ -> return 3)
          handler1 x e = 
            case e of
              BrokerException _ -> withMVar x $ \i ->
                                     if i == 1 then return Pass 
                                               else return (Fail
                                                "Two BrokerExceptions!")
              WorkerException _ -> withMVar x $ \i ->
                                     if i == 2 then return Pass
                                               else return (Fail 
                                                "Two WorkerExceptions!")
              _  -> return $ Fail $ "Unexpected Exception: " ++ show e

  ------------------------------------------------------------------------
  -- Receiver sends error message
  ------------------------------------------------------------------------
  -- connect 
  -- send anything (responder answers with error)
  -- error handler is activated
  ------------------------------------------------------------------------
  testError1 :: Int -> IO TestResult
  testError1 p = do
    m <- newMVar False
    withConnection host p [OWaitError 100,OEH (gotM m)] [] $ \c -> (
      withWriter c "test" "/q/out" [] [] (return . U.fromString) $ \w -> 
        withReader c "IN" "/q/out" [OMode Client] [] iconv $ \r -> do
        writeQ w nullType [] "some message"
        msg <- readQ r
        catch (nack c msg >> threadDelay 1000000) 
              (\e -> print (e::SomeException)))
    withMVar m (\i -> if i then return Pass 
                           else return (Fail "eh was not triggered!"))
    where gotM m _ _ = modifyMVar_ m (\_ -> return True)

  ------------------------------------------------------------------------
  -- Receiver sends error message, counter example
  ------------------------------------------------------------------------
  -- connect 
  -- send anything (responder answers with error)
  -- no error handler 
  ------------------------------------------------------------------------
  testError2 :: Int -> IO TestResult
  testError2 p = do
    m <- newMVar False
    withConnection host p [OWaitError 100] [] $ \c -> (
      withWriter c "test" "/q/out" [] [] (return . U.fromString) $ \w -> 
        withReader c "IN" "/q/out" [OMode Client] [] iconv $ \r -> do
        writeQ w nullType [] "some message"
        msg <- readQ r
        catch (catch (nack c msg >> threadDelay 1000000) 
                     (\e -> gotM m >> print (e::SomeException)))
                     (\e -> gotM m >> print (e::SomeException))) -- double exception...
    withMVar m (\i -> if i then return Pass 
                           else return (Fail "eh was not triggered!"))
    where gotM m = modifyMVar_ m (\_ -> return True)

  ------------------------------------------------------------------------
  -- Dead Worker
  -- Cause the receiver to terminate (using the nack trick)
  -- If we receive a WorkerException, we are done,
  -- but have to make sure that the following BrokerException is ignored
  ------------------------------------------------------------------------
  testDeadWorker :: Con -> IO TestResult
  testDeadWorker c = do
      iQ <- newReader c "IN"  tQ1 [OMode Client] [] iconv
      oQ <- newWriter c "OUT" tQ1 [] [] oconv
      t  <- action iQ oQ `catch` handler
      return t
    where action iQ oQ = do
            writeQ oQ nullType [] text1
            m <- readQ iQ
            (do nack c m 
                threadDelay 1000000
                return $ Fail "Nack had no effect!") `catch` handler
          handler e = 
            case e of
              WorkerException _   -> return Pass
              BrokerException _   -> return Pass
              _  -> return $ Fail $ "Unexpected Exception: " ++ show e

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
  --      since most brokers do not send the final receipt
  --      or close the socket immediately after sending the receipt
  --      so we accept the case, where the socket is closed 
  --      by the broker, instead of sending a receipt
  ------------------------------------------------------------------------
  testWaitBroker :: Int -> IO TestResult
  testWaitBroker p = do
    eiR <- try $ withConnection host p 
                               [OWaitBroker 1000] [] $ \_ -> return Pass
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
    eiR <- try $ withConnection host p [OHeartBeat b] [] $ \c -> do
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
    eiR <- try $ withConnection host p [OHeartBeat b] [] $ \_ -> do
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
    eiR <- try $ withConnection host p [OHeartBeat b] [] $ \_ -> do
      threadDelay $ 1000 * 1000
      return Pass
    case eiR of
       Right _ -> return $ Fail "heartbeats do not matter!"
       Left (ProtocolException e) ->
         if "Missing HeartBeat," `isInfixOf` e then return Pass
           else return $ Fail $ "Unexpected Protocol Exception: " ++ e
       Left  e -> return $ Fail $ "Unexpected Exception: " ++ show e

  ------------------------------------------------------------------------
  -- Cause Exception in receiver
  ------------------------------------------------------------------------
  -- connect 
  -- send anything (responder answers with nonsense)
  -- catch exception in main thread
  ------------------------------------------------------------------------
  testExc :: Int -> IO TestResult
  testExc p = 
    withConnection host p [] [] $ \c -> (
      withWriter c "test" "/q/out" [] [] (return . U.fromString) $ \w -> do
        writeQ w nullType [] "cause exception"
        threadDelay 1000000 
        return (Fail "No Exception")) `catch` (\e -> 
          case e of
            ProtocolException _ -> return Pass
            _                   -> return $ Fail $ show e)

  ------------------------------------------------------------------------
  -- Cause Exception in receiver and continue
  ------------------------------------------------------------------------
  -- connect 
  -- send anything (responder answers with nonsense)
  -- catch exception in main thread
  -- repeat
  ------------------------------------------------------------------------
  testExc2 :: Int -> IO TestResult
  testExc2 p = 
    withConnection host p [] [] $ \c -> 
      withWriter c "test" "/q/out" [] [] (return . U.fromString) $ \w -> do
        t <- action w
        case t of
          Fail e -> return (Fail $ "First failed: " ++ show e)
          Pass   -> action w
    where action w = (do
            writeQ w nullType [] "cause exception"
            threadDelay 1000000 
            return (Fail "No Exception")) `catch` (\e -> 
              case e of
                ProtocolException _ -> return Pass
                _                   -> return $ Fail $ show e)

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
  stdCon p = withConnection host p [] []

  ------------------------------------------------------------------------
  -- Secure Connection
  ------------------------------------------------------------------------
  secureCon :: Int -> (Con -> IO TestResult) -> IO TestResult
  secureCon p = withConnection host p [OTLS cfg] []
    where tlss = TLSSettingsSimple {
                    settingDisableCertificateValidation = True,
                    settingDisableSession               = False,
                    settingUseServerName                = False}
          cfg  = (tlsClientConfig p $ U.fromString host){
                  tlsClientTLSSettings=tlss}

  testWith :: Int -> (Con -> IO TestResult) -> IO TestResult
  testWith p act = do
    eiR <- try $ stdCon p act
    case eiR of
       Left  e -> return $ Fail $ show e
       Right r -> return r

  testSecure :: Int -> (Con -> IO TestResult) -> IO TestResult
  testSecure p act = do
    eiR <- try $ secureCon p act
    case eiR of
       Left  e -> return $ Fail $ show e
       Right r -> return r

  cleanQueue :: Con -> String -> IO ()
  cleanQueue c q = withReader c "Cleaner" q [] [] (\_ _ _ -> return) loop
    where loop r = do
            mbM <- tmo $ readQ r
            case mbM of
              Nothing -> return ()
              Just _  -> loop r
      
