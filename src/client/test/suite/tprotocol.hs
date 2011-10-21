module Main
where

  import Test

  import qualified Protocol as P
  import qualified Socket   as Sock
 
  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Parser 

  import qualified Network.Socket as S
  import           Network.BSD (getProtocolNumber) 
  import           Control.Concurrent

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import qualified Network.Socket.ByteString as SB

  import qualified Data.Attoparsec as A

  import           System.Exit
  
  maxRcv :: Int
  maxRcv = 1024

  host :: String
  host = "127.0.0.1"

  port :: Int
  port = 1111

  vers :: [F.Version]
  vers = [(1,0), (1,1)]

  beat :: F.Heart
  beat = (0,0)

  main :: IO ()
  main = do
    r <- runTests mkTests
    case r of
      Pass   -> exitSuccess
      Fail e -> exitFailure

  runTests :: TestGroup IO -> IO TestResult
  runTests g = do
    (r, s) <- execGroup g
    putStrLn s
    return r

  mkTests :: TestGroup IO
  mkTests = 
    let t10 = mkTest "Socket Connect " testConFrame
        t20 = mkTest "Protocl Connect" testConnect
    in  mkGroup "Protocol Tests" (Stop (Fail "")) 
          [t10, t20]
    
  testConFrameThere :: S.Socket -> B.ByteString -> IO TestResult
  testConFrameThere _ b = case parse b of
                            Left  e -> return $ Fail e
                            Right _ -> return Pass

  testConFrame :: IO TestResult
  testConFrame = 
    case F.mkConFrame [F.mkLogHdr  "guest",
                       F.mkPassHdr "guest",
                       F.mkAcVerHdr $ F.versToVal vers, 
                       F.mkBeatHdr  $ F.beatToVal beat] of
      Left  e -> return $ Fail e
      Right f -> do
        (s, rc, wr) <- baseConnect host port testConFrameThere
        Sock.send wr s f
        b <- SB.recv s maxRcv
        S.sClose s
        threadDelay 100000
        return $ streamToResult b

  testConnectThere :: S.Socket -> B.ByteString -> IO TestResult
  testConnectThere s b = 
    case parse b of
      Left  e  -> return $ Fail e
      Right c ->
        case F.typeOf c of
          F.Connect ->
            case F.mkCondFrame [F.mkVerHdr  "1.1",
                                F.mkBeatHdr "0.0"] of
              Left  e -> return $ Fail e
              Right f -> do
                wr <- Sock.initWriter
                Sock.send wr s f
                return Pass

  testConnect :: IO TestResult
  testConnect = do
    case F.mkConFrame [F.mkLogHdr  "guest",
                       F.mkPassHdr "guest",
                       F.mkAcVerHdr $ F.versToVal vers, 
                       F.mkBeatHdr  $ F.beatToVal beat] of
      Left  e -> return $ Fail e
      Right c -> do
        (s, rc, wr) <- baseConnect host port testConFrameThere
        Sock.send wr s c
        mbF <- Sock.receive rc s maxRcv
        case mbF of
          Left e  -> do
            S.sClose s
            return $ Fail e
          Right f -> do
            b <- SB.recv s maxRcv
            S.sClose s
            return $ streamToResult b

  baseConnect :: String -> Int -> 
                 (S.Socket -> B.ByteString -> IO TestResult) 
                 -> IO (S.Socket, Sock.Receiver, Sock.Writer)
  baseConnect h p t = do
    startListener h ((fromIntegral p)::S.PortNumber) t
    s  <- Sock.connect h p
    rc <- Sock.initReceiver
    wr <- Sock.initWriter
    return (s, rc, wr)

  parse :: B.ByteString -> Either String F.Frame
  parse b = 
    case A.parse stompParser b of
      A.Fail str ctx e  -> Left $ 
                             (U.toString str) ++ " - " ++ e
      A.Partial x       -> Left "Partial Result"
      A.Done str f      -> 
        case F.typeOf f of
          F.Connect -> 
            Right f
          x         -> Left $ "Unexpected Frame: " ++ (show x)

  mkSocket :: String -> S.PortNumber -> IO S.Socket
  mkSocket h p = do
    proto <- getProtocolNumber "tcp"
    sock  <- S.socket S.AF_INET S.Stream proto
    addr  <- S.inet_addr h
    S.bindSocket sock (S.SockAddrInet p addr)
    return sock

  startListener :: String -> S.PortNumber -> 
                   (S.Socket -> B.ByteString -> IO TestResult) -> IO ()
  startListener h p test = do
    s <- mkSocket h p
    S.listen s 32 
    _ <- forkIO (listen s test)
    threadDelay 1000
    return ()

  listen :: S.Socket -> (S.Socket -> B.ByteString -> IO TestResult) -> IO ()
  listen s test = do
    (s', _) <- S.accept s
    m <- SB.recv s' maxRcv
    if B.length m == 0
      then do
        putStrLn "listen - peer disconnected!"
        S.sClose s
        return ()
      else do
        r <- test s' m
        let b = resultToStream r
        l <- SB.send s' b
        threadDelay 100000
        S.sClose s'
        S.sClose s
        if l /= B.length b
          then putStrLn "Cannot send!"
          else return ()

  resultToStream :: TestResult -> B.ByteString
  resultToStream = B.pack . show 

  streamToResult :: B.ByteString -> TestResult
  streamToResult = read . U.toString 
  
