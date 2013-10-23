{-# LANGUAGE NoMonomorphismRestriction, DoAndIfThenElse, BangPatterns, OverloadedStrings #-}

module Main where


import Control.Monad

import Control.Concurrent

import Control.Concurrent.STM

import qualified Data.ByteString.UTF8 as U

import qualified Data.ByteString.Lazy as UL -- mh?

import Data.Monoid

import Network.Mom.Stompl.Client.Queue

import Codec.MIME.Type (nullType)

import qualified Data.Aeson as JSON


import Data.Time


--import DataGenerator



--

-- Test direct queue consumer/reader

--


-- | An inbound converter that just converts any message content (bytestring) to a string

bytesToStringConverter :: InBound String

bytesToStringConverter _ _ _ = return . U.toString



-- | A simple queue reader that dumps all messages to the stdout

printIncoming :: Show a => {- Con -> -} InBound a -> String -> String -> IO ()

printIncoming {- connection -} !inConverter !hostName !queueName = do

    withConnection hostName 61613 [OAuth "guest" "guest"] [] $ \(!connection) -> do

        inQ  <- newReader connection "Q-IN"  queueName [] [] inConverter

        forever $ do

            readData <- try $ readQ inQ 

            case readData of

                Left  e -> do

                    putStrLn $ "Error: " ++ show e

                    -- error handling ...

                Right msg -> do

                    putStrLn $ show $ msgContent msg



--

-- Buffered queue consumer/reader

--


-- | Use a given TQueue to buffer received messages (which are read as quickly as possible from the queue on a separate thread)

bufferThroughTQueue :: TQueue (Message a) -> InBound a -> String -> String -> IO ()

bufferThroughTQueue !tq !inConverter !hostName !queueName = do

    threadID <- forkIO $ 

        withConnection hostName 61613 [OAuth "guest" "guest"] [] $ \(!connection) -> do

            inQ  <- newReader connection "Q-IN"  queueName [] [] inConverter

            forever $ do

                readData <- try $ readQ inQ 

                case readData of

                    Left  e -> do

                        putStrLn $ "Error: " ++ show e

                        -- error handling ...

                    Right msg -> do

                         atomically $ writeTQueue tq msg

    putStrLn $ "Sparked queue reader thread:" ++ show threadID




-- | A test queue consumer that buffers queue output via a TQueue

-- Just for giggles, and to test the buffering, read until queue is empty, then deliberately wait a bit, then read again until empty... etc.

printBufferedIncoming :: Show a => TQueue (Message a) -> IO ()

printBufferedIncoming !tq =

    let

        readUntilEmpty = do

            isQueueEmpty <- atomically $ isEmptyTQueue tq

            if isQueueEmpty then do

                --threadDelay 5000000  -- 5s

                return ()

            else do

                msg <- atomically $ readTQueue tq

                putStrLn $ show $ msgContent msg

                readUntilEmpty   

    in 

        forever $ readUntilEmpty

        


--

-- Test queue producer/writer

--

        

-- | Send messages generated from a given stream 

--sendMessages :: OutBound a -> [a] -> String -> String -> Int -> IO ()

sendMessages {- connection -} !outConverter !stream !hostName !queueName !messagePeriodMicroSeconds = do 

    threadID <- forkIO $  do

        withConnection hostName 61613 [OAuth "guest" "guest"] [] $ \(!connection) -> do

        -- withConnection hostName 61613 [] [] $ \(!connection) -> do  

            outQ <- newWriter connection "Q-OUT" queueName [] [] outConverter 

            forever $ do                                                                              -- (4) No reliance on stream, simple output 

                --now <- getCurrentTime; writeQ outQ nullType [] (QueueStatus now "hello" 1 2 3 4)

              writeQ outQ nullType [] "Everything is peachy?"

              threadDelay messagePeriodMicroSeconds   

            

              forM_ stream $ \(!message) -> do   

                  writeQ outQ nullType [] message                                                     -- (1) writeQ of message from a stream 
                  print message

                  --putStrLn "This is a test"                                                         -- (2) print string only

                  --now <- getCurrentTime; writeQ outQ nullType [] (QueueStatus now "hello" 1 2 3 4)  -- (3) Simple value out

                  threadDelay messagePeriodMicroSeconds

    putStrLn $ "Sparked queue writer thread:" ++ show threadID



-- | An outbound converter that just converts any value to a string and converts that to the required bytestream

toStringBytesConverter :: Show a => OutBound a

toStringBytesConverter = return . U.fromString . show


-- | An outbound converter that just converts any value to a JSON string 

toJSONBytesConverter :: (Monad m, JSON.ToJSON a) => a -> m U.ByteString

toJSONBytesConverter = return . mconcat . UL.toChunks . JSON.encode  


-- | Send a sequence of queue status messages to the given host/queue at the given period

sendQueueStatuses :: {- Con -> -} String -> String -> Int -> IO ()

sendQueueStatuses {- cid -} !hostName !queueName !messagePeriodMicroSeconds =

    sendMessages {- cid -} toStringBytesConverter [] {-queueStatuses-} hostName queueName messagePeriodMicroSeconds


-- | Send a sequence of contact segment messages to the given host/queue at the given period

sendContactSegments :: {- Con -> -} String -> String -> Int -> IO ()

sendContactSegments {- cid -} !hostName !queueName !messagePeriodMicroSeconds = 

    sendMessages {- cid -} toStringBytesConverter [] {-contactSegments-} hostName queueName messagePeriodMicroSeconds





main::IO()

main = do

-- withConnection "localhost" 61613 [OAuth "guest" "guest"] [] $ \(!cid) -> do

    putStrLn "Cranking up dem messages"

    

    -- Start writer for queue statuses

    -- sendQueueStatuses "singularity-rabbit" "/topic/queueStatus" 1000000  -- 10000
    sendQueueStatuses "localhost" "/topic/queueStatus" 100 -- 1000000  -- 10000

    --sendQueueStatuses "singularity-rabbit" "/topic/queueStatus" 0 

    

    -- Start writer for contact segments

    -- sendContactSegments "singularity-rabbit" "/topic/contactSegments" 1000000 -- 100
    -- sendContactSegments "localhost" "/topic/contactSegments" 100 -- 1000000 -- 100

    

    -- Log received messages 

    --printIncoming bytesToStringConverter  "singularity-rabbit" "/topic/contactSegments"

    --threadDelay 30000000  -- TTT

    

    -- Make buffered reader and read messages out

    -- tq <- atomically $ newTQueue

    -- bufferThroughTQueue tq bytesToStringConverter "singularity-rabbit" "/topic/queueStatus"
    -- bufferThroughTQueue tq bytesToStringConverter "localhost" "/topic/queueStatus"

    -- bufferThroughTQueue tq bytesToStringConverter "singularity-rabbit" "/topic/contactSegments"
    -- bufferThroughTQueue tq bytesToStringConverter "localhost" "/topic/contactSegments"

    

    

    putStrLn "Subscribing and logging messages to console"

    -- printBufferedIncoming tq
    printIncoming bytesToStringConverter "localhost" "/topic/queueStatus"





