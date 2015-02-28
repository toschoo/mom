{-# Language BangPatterns #-}
module Main
where

  import qualified Data.ByteString.Char8 as B
  import           Data.Char (isDigit)
  import           Data.String (fromString)
  import qualified Data.Conduit as C
  import           Data.Conduit (($$),(=$),(=$=))
  import           Data.Conduit.Network
  import           Data.Conduit.Network.TLS
  import           Control.Monad (forever)
  import           Control.Monad.Trans (liftIO)
  import           Control.Concurrent
  import           System.Environment
  import           Network.Socket
  import           Filesystem.Path.CurrentOS

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [c,s] -> if (all isDigit c) && (all isDigit s)
                 then withSocketsDo $ bridge (read c) (read s)
                 else error ("At least one of the ports is not numeric: " ++ 
                             show c ++ ", " ++ show s)
      _      -> error ("I need a bridge port and a server port " ++
                       "and nothing else.")

  bridge :: Int -> Int -> IO ()
  bridge c s = let hp   = fromString "*"
                   cfg  = tlsConfig hp c (decodeString "test/keys/broker.crt") 
                                         (decodeString "test/keys/broker.ks")
                in runTCPServerTLS cfg session
    where session ad = do
            op <- newChan
            ip <- newChan
            _  <- forkIO (client s op ip)
            _  <- forkIO (response ad ip)
            appSource ad $$ pipeSink op

  client :: Int -> Chan B.ByteString -> Chan B.ByteString -> IO ()
  client p op ip = let cl = clientSettings p (B.pack "localhost")
                    in runTCPClient cl $ \ad -> do
    _ <- forkIO (request ad op)
    appSource ad $$ pipeSink ip

  request :: AppData -> Chan B.ByteString -> IO ()
  request ad op = pipeSource op $$ appSink ad

  response :: AppData -> Chan B.ByteString -> IO ()
  response ad ip = pipeSource ip $$ appSink ad

  pipeSink :: Chan B.ByteString -> C.Sink B.ByteString IO ()
  pipeSink ch = C.awaitForever (\i -> liftIO (writeChan ch i))

  pipeSource :: Chan B.ByteString -> C.Source IO B.ByteString 
  pipeSource ch = forever (liftIO (readChan ch) >>= C.yield)
