module Socket (Writer, Receiver, 
               initWriter, initReceiver,
               connect, disconnect, send, receive)
where

  import qualified Network.Socket            as S
  import qualified Network.Socket.ByteString as BS
  import           Network.BSD (getProtocolNumber)

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import qualified Data.Word             as W

  import           Network.Mom.Stompl.Parser (stompParser)
  import qualified Network.Mom.Stompl.Frame as F

  import           Control.Concurrent.MVar
  import           Control.Applicative ((<$>))
  import           Control.Exception (Exception)

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
    return $ Receiver {
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
    return $ Writer {
               lock    = lockSock l,
               release = releaseSock l
             }
        
  connect :: String -> Int -> IO (Either String S.Socket)
  connect host port = do
    let p = fromIntegral port :: S.PortNumber
    prot <- getProtocolNumber "tcp" 
    sock <- S.socket S.AF_INET S.Stream prot
    addr <- S.inet_addr host
    eiS  <- catch (conSck sock p addr)
                   errMsg
    case eiS of
      Right _ -> return $ Right sock
      Left  e -> return $ Left  e
 
    where errMsg e = return $ Left $ show e
          conSck s p h = do
            S.connect s (S.SockAddrInet p h)
            return $ Right 0

  disconnect :: S.Socket -> IO ()
  disconnect sock = 
    catch (S.sClose sock)
           ignore

  errMsg :: Exception e => e -> IO (Either String Int)
  errMsg e = return $ Left $ show e

  ignore :: Exception e => e -> IO ()
  ignore _ = return ()

  send :: Writer -> S.Socket -> F.Frame -> IO (Either String Bool)
  send wr sock f = do
    let s = F.putFrame f
    lock wr
    eiN <- catch (sendSock sock s)
                 errMsg
    release wr
    case eiN of
      Left  s -> return $ Left s
      Right i -> 
        if B.length s == i 
          then return $ Right True
          else return $ Left $
                 "Could not send complete buffer. Bytes sent: " ++ (show i)

    where sendSock s b = Right <$> BS.send s b

  receive :: Receiver -> S.Socket -> Int -> IO (Either String F.Frame)
  receive rec sock max = handlePartial rec sock max Nothing 0

  handlePartial :: Receiver -> S.Socket -> Int -> 
                   Maybe Result -> Int -> IO (Either String F.Frame)
  handlePartial rec sock max mbR step = do
    putStrLn $ "partial " ++ (show step)
    eiS <- getInput rec sock max
    case eiS of -- heart beats!
      Left  e -> return $ Left e
      Right s -> 
        let prs = case mbR of 
                    Just r -> A.feed r
                    _      -> A.parse stompParser
        in case prs s of
          A.Fail str ctx e -> return $ Left $
                                       (U.toString s) ++ 
                                       ": " ++ e
          r@(A.Partial x)  -> 
            if step > maxStep 
              then return $ Left "Message too long!" 
              else handlePartial rec sock max (Just r) (step + 1)
                     
          A.Done str f     -> do
            let str' = B.dropWhile white str
            if B.null str' 
              then return $ Right f
              else do
                putBuffer rec str'
                return $ Right f
          
  getInput :: Receiver -> S.Socket -> Int -> IO (Either String B.ByteString)
  getInput rec sock max = do
    mbB <- getBuffer rec
    case mbB of
      Just s  -> return $ Right s
      Nothing -> do
        eiS <- catch (Right <$> BS.recv sock max)
                     (\e -> return $ Left $ show e)
        case eiS of
          Left  e -> return $ Left e
          Right s -> 
            if B.null s 
              then return $ Left "Peer disconnected"
              else do 
                let s' = B.dropWhile white s
                if B.null s 
                  then getInput rec sock max
                  else return $ Right s

  white :: Char -> Bool
  white c = c == '\n'
