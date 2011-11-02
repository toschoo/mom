{-# Language CPP #-}
module Socket (Writer, Receiver, 
               initWriter, initReceiver,
               connect, disconnect, send, receive)
where

  import qualified Network.Socket            as S
  import qualified Network.Socket.ByteString as BS
  import           Network.BSD (getProtocolNumber)

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U

  import           Network.Mom.Stompl.Parser (stompParser)
  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Client.Exception

  import           Control.Concurrent.MVar
  import           Control.Applicative ((<$>))
  import           Control.Monad (unless)
  import           Control.Exception (throwIO, finally)

  import qualified Data.Attoparsec as A (Result(..), feed, parse)

  type Result = A.Result F.Frame

  maxStep :: Int
  maxStep = 100

  data Receiver = Receiver {
                    getBuffer :: IO (Maybe B.ByteString),
                    putBuffer :: B.ByteString -> IO ()
                  }

  data Writer = Writer {
                  lock    :: IO (),
                  release :: IO ()
                }

  get :: MVar B.ByteString -> IO (Maybe B.ByteString)
  get buf = do
    e <- isEmptyMVar buf
    if e
      then return Nothing
      else Just <$> takeMVar buf

  put :: MVar B.ByteString -> B.ByteString -> IO ()
  put buf s = do
    e <- isEmptyMVar buf
    if e 
      then putMVar buf s
      else do 
        _ <- takeMVar buf
        putMVar buf s

  initReceiver :: IO Receiver
  initReceiver = do
    buf <- newEmptyMVar
    return Receiver {
               getBuffer = get buf,
               putBuffer = put buf}

  lockSock :: MVar Bool -> IO ()
  lockSock l = putMVar l True

  releaseSock :: MVar Bool -> IO ()
  releaseSock l = do
    _ <- takeMVar l
    return ()

  initWriter :: IO Writer
  initWriter = do
    l <- newEmptyMVar
    return Writer {
               lock    = lockSock l,
               release = releaseSock l
             }
        
  connect :: String -> Int -> IO S.Socket
  connect host port = do
    let p = fromIntegral port :: S.PortNumber
    prot <- getProtocolNumber "tcp" 
    sock <- S.socket S.AF_INET S.Stream prot
    addr <- S.inet_addr host
    S.connect sock (S.SockAddrInet p addr)
    return sock

  disconnect :: S.Socket -> IO ()
  disconnect = S.sClose 

  send :: Writer -> S.Socket -> F.Frame -> IO ()
  send wr sock f = do
    let s = F.putFrame f
    lock wr
#ifdef _DEBUG
    putStrLn $ "Sending: " ++ U.toString s
#endif
    n <- finally (BS.send sock s)
                 (release wr)
    unless (n == B.length s) $ throwIO $ SocketException $
                                  "Could not send complete buffer. " ++
                                  "Bytes sent: " ++ show n

  receive :: Receiver -> S.Socket -> Int -> IO (Either String F.Frame)
  receive rec sock mx = handlePartial rec sock mx Nothing 0

  handlePartial :: Receiver -> S.Socket -> Int -> 
                   Maybe Result -> Int -> IO (Either String F.Frame)
  handlePartial rec sock mx mbR step = do
    eiS <- getInput rec sock mx
    case eiS of 
      Left  e -> return $ Left e
      Right s -> do
        let prs = case mbR of 
                    Just r -> A.feed r
                    _      -> A.parse stompParser
        case prs s of
          A.Fail _ _   e   -> return $ Left $
                                       U.toString s ++ 
                                       ": " ++ e
          r@(A.Partial _)  -> 
            if step > maxStep 
              then return $ Left "Message too long!" 
              else handlePartial rec sock mx (Just r) (step + 1)
                     
          A.Done str f     -> do
            unless (B.null str) $ putBuffer rec str
            return $ Right f
          
  getInput :: Receiver -> S.Socket -> Int -> IO (Either String B.ByteString)
  getInput rec sock mx = do
    mbB <- getBuffer rec
    case mbB of
      Just s  -> 
#ifdef _DEBUG
        do putStrLn $ "In Buffer: " ++ U.toString s
#endif
           return $ Right s
      Nothing -> do
        s <- BS.recv sock mx
        if B.null s 
          then return $ Left "Peer disconnected"
          else 
#ifdef _DEBUG
            do putStrLn $ "Receiving: " ++ U.toString s
#endif
               if B.null s
                 then getInput rec sock mx
                 else return $ Right s
