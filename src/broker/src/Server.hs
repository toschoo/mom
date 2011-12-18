module Server (startServer)
where

  import           Types
  import           Config
  import           Session
  import           Sender
  import           Logger
  import qualified Socket as S
  import           Exception

  import qualified Network.Socket as Sock

  import           Control.Concurrent
  import           Prelude hiding (catch)
  import           Control.Exception hiding (try)
  import qualified Control.Exception as Exc (try)
  import           Control.Monad
  import           Control.Monad.State
  import           Control.Applicative ((<$>))

  import           Data.List  (insert, delete, find)
  import           Data.Maybe (catMaybes)
  import qualified Data.ByteString           as B

  logger :: String
  logger = "Server"

  startServer :: FilePath -> IO ()
  startServer f = Sock.withSocketsDo $ do
    mkConfig f
    startLogger
    report logger INFO "Starting..."
    snds <- srvStartSenders -- start n senders !
    let host = "localhost" -- "127.0.0.1"
    s <- S.bind host 61618
    Sock.listen s 256
    listen s

  listen :: Sock.Socket -> IO ()
  listen s = forever $ do
    (s', _) <- Sock.accept s
    eiT  <- Exc.try (startSession s')
    case eiT of
      Left  e -> report logger ALERT $ 
                        "Cannot start session: " ++ 
                        show (e::SomeException)
      Right _ -> return ()
                

  srvStartSenders :: IO [ThreadId]
  srvStartSenders = replicateM 1 srvStartSender
    where srvStartSender = startSender

