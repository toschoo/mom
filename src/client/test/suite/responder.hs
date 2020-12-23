module Main
where

  import qualified Socket   as Sock
 
  import qualified Network.Mom.Stompl.Frame as F

  import qualified Network.Socket as S
  import qualified Network.Socket.ByteString as SB
  import           Network.BSD (getProtocolNumber) 
  import           Control.Monad (forever, void)
  import           Control.Exception (bracket, SomeException, throwIO)
  import qualified Control.Exception as Ex (catch) 
  import qualified Data.ByteString.Char8 as B
  import           Data.Word (Word8)

  maxRcv :: Int
  maxRcv = 1024

  type Host = (Word8, Word8, Word8, Word8)

  host :: Host
  host = (127,0,0,1)

  port :: Int
  port = 22222

  ver :: F.Version
  ver = (1,1)

  main :: IO ()
  main = bracket (do s <- mkSocket host (fromIntegral port::S.PortNumber)
                     S.listen s 32
                     return s)
                 S.close
                 (forever . listen)

  mkSocket :: Host -> S.PortNumber -> IO S.Socket
  mkSocket h p = do
    proto <- getProtocolNumber "tcp"
    sock  <- S.socket S.AF_INET S.Stream proto
    Ex.catch (S.bind sock (S.SockAddrInet p $ S.tupleToHostAddress h)) -- addr))
             (\e -> S.close sock >> throwIO (e::SomeException))
    return sock

  listen :: S.Socket -> IO ()
  listen s = do
    (s', _) <- S.accept s
    rc      <- Sock.initReceiver
    session True s' rc

  session :: Bool -> S.Socket -> Sock.Receiver -> IO ()
  session beats s rc = do
    mbF <- Sock.receive rc s maxRcv
    case mbF of
      Left e  -> do
        putStrLn $ "Error: " ++ e
        S.close s
      Right f ->
        case F.typeOf f of
          F.Connect    -> connect s rc f
          F.Disconnect -> S.close s
          F.Send       -> handleSend s f >> session beats s rc
          _ | not beats && F.typeOf f == F.HeartBeat -> session beats s rc
            | otherwise -> do
                let b = F.putFrame f
                l <- SB.send s b
                if l == B.length b then session beats s rc
                  else do
                    putStrLn "Cannot send Frame!"
                    S.close s

  handleSend :: S.Socket -> F.Frame -> IO ()
  handleSend s _ = let nonsense = B.pack "MESSAGE\n\nrubbish\n\NUL"
                    in void $ SB.send s nonsense
                        

  connect :: S.Socket -> Sock.Receiver -> F.Frame -> IO ()
  connect s rc fc = 
     case F.mkCondFrame [F.mkVerHdr  "1.1",
                         F.mkBeatHdr $ getBeat fc] of
       Left  e -> do
         putStrLn $ "Cannot make CondFrame: " ++ e
         S.close s
       Right f -> do
         let b = F.putFrame f
         l <- SB.send s b
         if l == B.length b
           then let beats = F.getBeat fc /= (50,50)
                in  session beats s rc
           else do
             putStrLn "Cannot send Connected!"
             S.close s

  getBeat :: F.Frame -> String
  getBeat = F.beatToVal . F.getBeat 
