{-# LANGUAGE ScopedTypeVariables, DeriveDataTypeable #-}
{- |

A client library for Stomp serevers implementing stomp 1.1 specification. See http://stomp.github.com/stomp-specification-1.1.html

/Example/:

>import Network.Stomp
>import qualified Data.ByteString.Lazy.Char8 as B
>
>main = do
>  -- connect to a stomp broker
>  con <- connect "stomp://guest:guest@127.0.0.1:61613" vers headers
>  putStrLn $ "Accepted versions: " ++ show (versions con)
>  
>  -- start consumer and subscribe to the queue
>  startConsumer con callback
>  subscribe con "/queue/test" "0" []
>
>  -- send the messages to the queue
>  send con "/queue/test" [] (B.pack "message1")
>  send con "/queue/test" [] (B.pack "message2")
>
>  -- wait
>  getLine
>  
>  -- unsubscribe and disconnect
>  unsubscribe con "0" []
>  disconnect con []
>  where 
>    vers = [(1,0),(1,1)]
>    headers = []
>
>callback :: Frame -> IO ()
>callback (Frame (SC MESSAGE) hs body) = do
>      putStrLn $ "received message: " ++ (B.unpack body) 
>      putStrLn $ "headers: " ++ show hs
>callback f = putStrLn $ "received frame: " ++ show f
 
-}

module Network.Stomp (
   Command (..),
   ClientCommand (..),
   ServerCommand (..),
   Frame (..),
   Connection,
   StompUri,
   Host,
   Destination,
   MessageId,
   Transaction,
   Subscription,
   Version,
   StompException (..),

   connect,
   stomp,
   connect',
   stomp',
   disconnect,
   send,
   send',
   subscribe,
   unsubscribe,
   ack,
   nack,
   begin,
   commit,
   abort,

   startConsumer,
   receiveFrame,
   setExcpHandler,
   startSendBeat,
   startRecvBeat,
   beat,
   sendTimeout,
   recvTimeout,
   lastSend,
   lastRecv,

   versions,
   session,
   server
)
where

import qualified Data.ByteString.UTF8 as BU
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.List (intercalate)
import Data.Time (UTCTime, getCurrentTime, diffUTCTime)
import Data.Typeable
import Data.Maybe
import Data.Char (toLower)
import qualified Data.Binary.Builder as B
import Data.Binary.Put

import Network.BSD
import Network.URI
import qualified Network.Socket as N

import qualified Control.Exception as E
import Control.Concurrent
import Control.Monad
import System.IO

---
import qualified Network.Socket.ByteString.Lazy as L

-- | Stomp frame record
data Frame = Frame {
   command :: Command, 
   headers :: [Header], 
   body :: BL.ByteString
} deriving Show

-- | Client frame commands
data ClientCommand 
   = SEND 
   | SUBSCRIBE 
   | UNSUBSCRIBE 
   | BEGIN 
   | COMMIT 
   | ABORT 
   | ACK 
   | NACK 
   | DISCONNECT 
   | CONNECT 
   | STOMP 
 deriving (Show, Read, Eq)

-- | Broker frame commands
data ServerCommand 
   = CONNECTED 
   | MESSAGE 
   | RECEIPT 
   | ERROR
 deriving (Show, Read, Eq)

-- | Stomp frame commands
data Command 
   = CC ClientCommand 
   | SC ServerCommand 
 deriving (Show, Read, Eq)

-- | Stomp header as a name/value pair
type Header = (String, String)

-- | A record used to communicate with Stomp brokers
data Connection = Connection {
   handle :: Handle,              
   versions :: [Version], -- ^ accepted stomp versions
   session :: String, -- ^ session identifier
   server :: String, -- ^ stomp server info
   listener :: MVar ThreadId, -- ^ frames consumer thread
   sendBeat :: MVar ThreadId, -- ^ send heartbeat thread
   recvBeat :: MVar ThreadId, -- ^ recieve heartbeat thread
   sendTimeout :: Int, -- ^ send heartbeat timeout
   recvTimeout :: Int, -- ^ receive heaertbeat timeout
   lastSend :: MVar UTCTime, -- ^ last frame sent time 
   lastRecv :: MVar UTCTime, -- ^ last frame received time
   sockLock :: MVar (), -- ^ used for atomic writes to the socket   
   closed :: MVar (Maybe StompException), -- ^ closed connection flag
   disconReq :: MVar String, -- ^ disconnect receipt request 
   disconResp :: MVar (), -- ^ disconnect receipt response 
   excpHandle :: MVar (Maybe (StompException -> IO ())) -- ^ excp handler invoked by the frames consumer
}

data StompException 
  = ConnectionError String
  | InvalidUri String
  | InvalidFrame String
  | BrokerError String
  | StompIOError E.IOException
 deriving (Typeable, Show, Eq)    

instance E.Exception StompException  

type StompUri = String
type Host = String
type Destination = String
type MessageId = String
type Transaction = String
type Subscription = String
type Version = (Int,Int)

------- Stomp API

-- | connect to the stomp (1.0, 1.1) broker using uri   
connect :: StompUri -> [Version] -> [Header] -> IO Connection
connect uri vs hs = do
     (host, port, hs') <- processUri uri
     connect' host port vs (hs ++ hs')

-- | connect to the stomp 1.1 broker using uri
stomp :: StompUri -> [Header] -> IO Connection
stomp uri hs = do
     (host, port, hs') <- processUri uri
     stomp' host port (hs ++ hs')

-- | connect to the stomp (1.0, 1.1) broker using hostname and port
connect' :: Host -> PortNumber -> [Version] -> [Header] -> IO Connection
connect' h p [] hs = mkConnection CONNECT h p (hs ++ [("accept-version","1.0")])
connect' h p vs hs = mkConnection CONNECT h p hs' 
   where hs' = hs ++ [("accept-version", vers)]
         vers = intercalate "," $ map go vs
         go (v,v') = show v ++ '.':show v'

-- | connect to the stomp 1.1 broker using hostname and port
stomp' ::  Host -> PortNumber -> [Header] -> IO Connection
stomp' = mkConnection STOMP

-- | closes stomp connection
disconnect :: Connection -> [Header] -> IO ()
disconnect con hs = do
   let receipt = lookup "receipt" hs
   case receipt of
     Nothing -> do -- stop con
                   sendFrame con (Frame (CC DISCONNECT) hs BL.empty)
                   stop con
     Just r ->  do putMVar (disconReq con) r
                   sendFrame con (Frame (CC DISCONNECT) hs BL.empty)
                   readMVar $ disconResp con
                   stop con
   hClose $ handle con
   modifyMVar_ (closed con) $ \x -> 
        return (Just $ fromMaybe (ConnectionError "Connection closed") x)

-- | send message to the destination.
-- | The header 'content-length' is automatically set by this module. 
send :: Connection -> Destination -> [Header] -> BL.ByteString -> IO ()
send con dest hs body = sendFrame con $ mkSendFrame (dest, hs, body)

-- | send group of messages to the destination.
-- | The header 'content-length' is automatically set by this module. 
send' :: Connection -> [(Destination, [Header], BL.ByteString)] -> IO ()
send' con xs = sendFrames con $ map mkSendFrame xs

-- | create SEND frame
mkSendFrame :: (Destination, [Header], BL.ByteString) -> Frame
mkSendFrame (dest, hs, body) = Frame (CC SEND) hs' body
   where hs' = hs ++ [("destination", dest)] ++ clh
         clh = maybe hdr (\_->[]) $ lookup "content-length" hs
         hdr = [("content-length", show $ (BL.length body))]

-- | subscribe to the destination to receive stomp frames
subscribe :: Connection -> Destination -> Subscription -> [Header] -> IO ()
subscribe con dest id hs = sendFrame con (Frame (CC SUBSCRIBE) hs' BL.empty)
   where hs' = hs ++ [("destination", dest), ("id", id)]

-- | unsubscribe from destination given the subscription id
unsubscribe :: Connection -> Subscription -> [Header] -> IO ()
unsubscribe con id hs = sendFrame con (Frame (CC UNSUBSCRIBE) hs' BL.empty)
   where hs' = hs ++ [("id", id)]

-- | acknowledge the consumption of a message from a subscription
ack :: Connection -> Subscription -> MessageId -> [Header] -> IO ()
ack con id msgid hs = sendFrame con (Frame (CC ACK) hs' BL.empty)
   where hs' = hs ++ [("subscription", id), ("message-id", msgid)]

-- | acknowledge the rejection of a message from a subscription
nack :: Connection -> Subscription -> MessageId -> [Header] -> IO ()
nack con id msgid hs = sendFrame con (Frame (CC NACK) hs' BL.empty)
   where hs' = hs ++ [("subscription", id), ("message-id", msgid)]

-- | start a transaction
begin :: Connection -> Transaction -> [Header] -> IO ()
begin con tid hs = sendFrame con (Frame (CC BEGIN) hs' BL.empty)
   where hs' = hs ++ [("transaction", tid)]

-- | commit a transaction
commit :: Connection -> Transaction -> [Header] -> IO ()
commit con tid hs = sendFrame con (Frame (CC COMMIT) hs' BL.empty)
   where hs' = hs ++ [("transaction", tid)]

-- | rollback a transaction
abort :: Connection -> Transaction -> [Header] -> IO ()
abort con tid hs = sendFrame con (Frame (CC ABORT) hs' BL.empty)
   where hs' = hs ++ [("transaction", tid)]

--------- Connection

-- | opens connection to the stomp boker
mkConnection :: ClientCommand -> Host -> PortNumber -> [Header] -> IO Connection
mkConnection cmd host port hs = do
    con <- newConn host port hs
    putStrLn "Connected!"
    sendFrame con (Frame (CC cmd) hs BL.empty)
    putStrLn "Connection Frame sent!"
    (Frame (SC cmd) hs' body) <- receiveFrame con
    when (cmd /= CONNECTED) $
       E.throwIO $ ConnectionError (BL.unpack body) 
    putStrLn "Connected Frame received!"
    let (sendBeat, recvBeat) = getBeats hs hs'
    return con { recvTimeout = recvBeat 
               , sendTimeout = sendBeat 
               , versions = maybe [(1,0)] parseVer $ lookup "version" hs'
               , session = fromMaybe [] $ lookup "session" hs' 
               , server = fromMaybe [] $ lookup "server" hs'
               } 
          
-- | parse versions supported by the broker 
parseVer :: String -> [Version]
parseVer vs 
   | null r = [mkV v]
   | otherwise = mkV v : parseVer (tail r)
  where (v,r) = break (==',') vs
        mkV s = let (m, m') = break (=='.') s in
          (read m, read (tail m'))
  
-- | open the server socket and convert it to handle
openSocket :: String -> PortNumber -> IO Handle
openSocket host port = do
   proto <- getProtocolNumber "tcp"
   sock <- N.socket N.AF_INET N.Stream proto
   addr <- N.inet_addr host
   N.connect sock (N.SockAddrInet port addr)
   h <- N.socketToHandle sock ReadWriteMode
   hSetBuffering h (BlockBuffering Nothing)
   return h

-- | initialize the new connection
newConn :: Host -> PortNumber -> [Header] -> IO Connection
newConn host port hs = do
   h <- openSocket host port
   lstnr <- newEmptyMVar 
   sBeat <- newEmptyMVar
   rBeat <- newEmptyMVar
   lSend <- newEmptyMVar
   lRecv <- newEmptyMVar
   sLock <- newMVar ()
   close <- newMVar Nothing
   dRcpt <- newEmptyMVar
   dLock <- newEmptyMVar
   eHndl <- newMVar Nothing   
   return $ Connection h [] [] [] lstnr sBeat rBeat  0 0 lSend 
                  lRecv sLock close dRcpt dLock eHndl
              
-- | create the authority value
stompAuth :: String -> Maybe URIAuth
stompAuth str = maybe Nothing auth (parseURI str)
    where auth (URI s a _ _ _) = 
            if map toLower s /= "stomp:" then Nothing else a  

-- | retrieve host, port and headers from the authority value
fromAuth :: PortNumber -> URIAuth -> (Host, PortNumber, [Header])
fromAuth defPort ua = 
   (host, portNum, [("login", user), ("passcode", pwd')])
   where  (user, pwd) = break (==':') (uriUserInfo ua)
          pwd' = if null pwd then [] else drop 1 $ init pwd
          port = drop 1 (uriPort ua)
          portNum = if null port then defPort 
                     else toEnum (read port :: Int)
          host = uriRegName ua

-- | parse stomp uri
processUri :: String -> IO (Host, PortNumber, [Header])
processUri uri = 
    maybe (E.throwIO $ InvalidUri uri)
          (return . fromAuth defaultPort)
          (stompAuth uri)  

-- | stop all children threads
stop :: Connection -> IO ()
stop c = mapM_ go [listener c, sendBeat c, recvBeat c]
   where go x = do 
           t <- tryTakeMVar x
           maybe (return ()) killThread t

defaultPort :: PortNumber
defaultPort = 61613

-------- Exceptions

-- | set exception handler callback to process the exception in the consumer/heartbeats threads 
setExcpHandler :: Connection -> (StompException -> IO ()) -> IO ()
setExcpHandler con fun = 
   modifyMVar_ (excpHandle con) $ \_ -> return $ Just fun

-- | invoke the exception handler and close the connection
onException :: Connection -> StompException -> IO ()
onException con e = 
  withMVar (excpHandle con) $ \h -> do
     maybe (return ()) (\f -> f e) h
     modifyMVar_ (closed con) $ \x -> 
        return $ mplus x (Just e)

-------- Consume frames

-- | create consume frames thread
startConsumer :: Connection -> (Frame -> IO ()) -> IO ()
startConsumer c fun = do
    t <- tryTakeMVar (listener c)
    when (isNothing t) $ do
       tid <- forkIO $ E.finally 
               (consumeFrames c fun)
               (tryPutMVar (disconResp c) ())
       _ <- tryPutMVar (listener c) tid
       return ()

-- | For any incoming frame the user callback will be invoked
consumeFrames :: Connection -> (Frame -> IO ()) -> IO ()
consumeFrames con fun = 
  E.catch 
    (do
      frame <- receiveFrame con
      fun frame
      rec <- tryTakeMVar (disconReq con)           
      unless (isJust rec && checkReceipt (fromJust rec) frame) $ do
         maybe (return ()) (putMVar (disconReq con)) rec
         consumeFrames con fun)
    (\(e::StompException) -> onException con e)

-- | check receipt value in the frame headers
checkReceipt :: String -> Frame -> Bool
checkReceipt rec (Frame (SC RECEIPT) hs _) =
    maybe False (== rec) $ lookup "receipt-id" hs 
checkReceipt _ _ = False

-- | send frame buffer to the server
sendFrame :: Connection -> Frame -> IO ()
sendFrame con f = sendBuf con (putFrames [f]) 

-- | send batch of frames to the server
sendFrames :: Connection -> [Frame] -> IO ()
sendFrames con fs = sendBuf con (putFrames fs)

-- | serialize frames
putFrames :: [Frame] -> BL.ByteString
putFrames fs = runPut $ mapM_ put fs
   where 
   put (Frame (CC cmd) hs body) = do
      mapM_ (putByteString . BU.fromString)
         [show cmd, "\n", hdrToStr cmd hs, "\n"]
      unless (BL.null body) $ putLazyByteString body 
      putWord8 0x00

-- | unparse frame headers. 
-- | All frames except the CONNECT and CONNECTED frames will also escape any colon or newline octets
hdrToStr :: ClientCommand -> [Header] -> String
hdrToStr _ [] = []
hdrToStr CONNECT hs = hdrToStr' id hs
hdrToStr cmd hs = hdrToStr' esc hs

hdrToStr' f xs = unlines $ map go xs
  where go (x,y) = f x ++ ":" ++ f y 

-- | send buffer to the handle
sendBuf :: Connection -> BL.ByteString -> IO ()
sendBuf con bs = 
  withMVar (closed con) $ \c ->
      if isJust c then E.throwIO (fromJust c)
       else
         E.catch
           (withMVar (sockLock con) $ \_ -> do
               BS.hPut (handle con) (BS.concat $ BL.toChunks bs)
               hFlush (handle con)
               beatTime (lastSend con))
           (\(e :: E.IOException) -> E.throwIO $ StompIOError e)

--------- Receive frame

-- | receives incoming frame
receiveFrame :: Connection -> IO Frame
receiveFrame con = do
   eof <- hIsEOF (handle con)
   if eof then
      E.throwIO $ ConnectionError "Connection closed by broker"
    else do
       frame <- getFrame (handle con)
       beatTime (lastRecv con)
       maybe (receiveFrame con) return frame

-- | reads frame from the handle
getFrame :: Handle -> IO (Maybe Frame)
getFrame h = do
   line <- liftM (dropWhile (=='\x00')) (readLine h) 
   if null line then return Nothing
     else do
       let cmd = SC (read line :: ServerCommand)
       headers <- readHeaders h cmd
       body <- readBody h headers
       return (Just $ Frame cmd headers body)

-- | reads stomp headers
readHeaders :: Handle -> Command -> IO [Header]
readHeaders h cmd = do
   l <- readLine h
   if null l then return []
     else do hs <- readHeaders h cmd
             return (header (unesc l):hs)
             case cmd of
               (SC CONNECTED) -> return (header l:hs)
               _ -> return (header (unesc l):hs)
   where
     header x = let (name, val) = break (==':') x
                in (name, tail val)

-- reads stomp message with length 'content-length' or until frame terminator
readBody :: Handle -> [Header] -> IO BL.ByteString
readBody h hs = maybe (readTill h) (readBuf h) len
   where 
    len = lookup "content-length" hs
    readBuf h x = do
       bs <- BL.hGet h (read x :: Int)
       tr <- BL.hGet h 1 
       when (tr /= term) $ 
          E.throwIO $ InvalidFrame "Missing frame terminator"
       return bs  
    readTill h = liftM B.toLazyByteString (readTill' h)     
    readTill' h = do
       ch <- BL.hGet h 1
       if ch == term then return B.empty 
        else liftM (B.append $ B.fromLazyByteString ch) (readTill' h)
    term = BL.singleton '\x00'

-- | read line from handle
readLine :: Handle -> IO String
readLine h = fmap BU.toString (BS.hGetLine h)

-------- Escaping

-- | escape header name and value
esc :: String -> String
esc = concatMap (\c -> fromMaybe [c] (code c))
  where code x = lookup x escMap
        escMap = zip ":\n\\" ["\\c","\\n","\\\\"] 

-- | unescape header name and value
unesc :: String -> String
unesc [] = []
unesc [x] = [x]
unesc (x:x':xs) 
  | [x,x'] == "\\n" = '\n' : unesc xs
  | [x,x'] == "\\c" = ':' : unesc xs
  | [x,x'] == "\\\\" = '\\' : unesc xs
  | otherwise = x:unesc(x':xs) 

-------- Heartbeat

-- | get heartbeats values from the client and broker headers. The receiver tolerance is 100%.
getBeats :: [Header] -> [Header] -> (Int, Int)
getBeats xs ys = (getBeat cs sr, 2 * getBeat cr ss)
   where     
   getBeat x y 
      | x /= 0 && y /= 0 = max x y
      | otherwise = 0                 
   (cs,cr) = mkBeat xs
   (ss,sr) = mkBeat ys
 
-- | parse heartbeat
mkBeat :: [Header] -> (Int,Int)
mkBeat = maybe (0,0) parseBeat . lookup "heart-beat" 
  where
    parseBeat = parse . break (==',')
    parse (x,y) = (read x :: Int, read (tail y) :: Int) 

-- | update heartbeat time
beatTime :: MVar UTCTime -> IO ()
beatTime v = do
  now <- getCurrentTime
  b <- tryPutMVar v now
  unless b $ modifyMVar_ v $ \_ -> return now

-- | send a single newline byte to the server
beat :: Connection -> IO ()
beat con = sendBuf con (BL.fromChunks [BU.fromString "\n\x00"])

-- | periodically send heartbeat data to the server 
clientBeat :: Connection -> IO ()
clientBeat con = 
  E.catch 
    (do
      prev <- readMVar (lastSend con)
      now <- getCurrentTime
      let diff = floor $ diffUTCTime now prev * 1000
      let delay = sendTimeout con
      if diff >= delay then do
         beat con
         threadDelay (1000 * delay)
        else threadDelay (1000 * (delay - diff))  
      clientBeat con)
    (\(e::StompException) -> onException con e)

-- | periodically check a last frame receive time and mark connection as dead if needed 
brokerBeat :: Connection -> IO ()
brokerBeat con = 
  E.catch 
    (do
      prev <- readMVar (lastRecv con)
      now <- getCurrentTime
      let diff = floor $ diffUTCTime now prev * 1000
      let delay = recvTimeout con
      if diff >= delay then 
         E.throwIO $ BrokerError "Broker timeout expired"
        else threadDelay (1000 * (delay - diff))  
      brokerBeat con)
    (\(e::StompException) -> onException con e)

-- | fork send heartbeat thread
startSendBeat :: Connection -> IO ()
startSendBeat c = do
   svar <- tryTakeMVar (sendBeat c)
   when (sendTimeout c > 0 && isNothing svar) $ do
      tid <- forkIO $ E.finally
               (clientBeat c)
               (tryPutMVar (disconResp c) ())
      _ <- tryPutMVar (sendBeat c) tid
      return ()

-- | fork receive heartbeat thread
startRecvBeat :: Connection -> IO ()
startRecvBeat c = do
   rvar <- tryTakeMVar (recvBeat c)
   when (recvTimeout c > 0 && isNothing rvar) $ do
      tid <- forkIO $ E.finally
               (brokerBeat c)
               (tryPutMVar (disconResp c) ())
      _ <- tryPutMVar (recvBeat c) tid
      return ()

