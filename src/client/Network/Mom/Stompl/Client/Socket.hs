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
  import           Control.Monad (unless, when)
  import           Control.Exception (throwIO, finally, SomeException)
  import qualified Control.Exception as Ex (try)

  import qualified Data.Attoparsec.ByteString as A (
                                   Result, IResult(..), feed, parse)

  import System.IO (stderr, hPutStrLn)

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

  lockSock :: MVar () -> IO ()
  lockSock l = putMVar l ()

  releaseSock :: MVar () -> IO ()
  releaseSock = takeMVar 

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
    let hints = S.defaultHints {S.addrSocketType = S.Stream,
                                S.addrProtocol   = prot}
    adds <- S.getAddrInfo (Just hints) (Just host) Nothing
    tryConnect p adds

  tryConnect :: S.PortNumber -> [S.AddrInfo] -> IO S.Socket
  tryConnect _ [] = throwIO $ SocketException 
                              "Give up: no more address info!"
  tryConnect p (i:is) = 
    case i of
      S.AddrInfo _ f t prot addr _ -> 
        let mbA = case addr of
                    S.SockAddrInet  _ a      -> Just $ S.SockAddrInet p a
                    S.SockAddrInet6 _ fl a s -> Just $ S.SockAddrInet6 p fl a s
                    _                        -> Nothing
         in case mbA of
              Nothing -> tryConnect p is
              Just a  -> do
                sock <- S.socket f t prot
                eiS  <- Ex.try $ S.connect sock a
                case eiS of
                  Left  e -> do
                    hPutStrLn stderr $ "Network.Mom.Stompl.Socket - " ++
                                       "Warning: " ++ show (e::SomeException)
                    tryConnect p is
                  Right _ -> return sock

  disconnect :: S.Socket -> IO ()
  disconnect = S.sClose 

  send :: Writer -> S.Socket -> F.Frame -> IO ()
  send wr sock f = do
    let s = F.putFrame f
    lock wr
#ifdef _DEBUG
    putStrLn $ "Sending: " ++ U.toString s
#endif
    finally (resend sock s) (release wr)

  resend :: S.Socket -> B.ByteString -> IO ()
  resend s b | B.length b == 0 = return ()
             | otherwise       = do
    let t = B.length b
    n <- BS.send s b
    if n == 0
      then throwIO $ SocketException "Could not send buffer -- unknown error"
      else when (n < t) $ resend s $ B.drop n b 

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
