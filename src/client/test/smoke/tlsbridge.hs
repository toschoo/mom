{-# Language BangPatterns #-}
module Main
where

  import qualified Data.ByteString.Char8 as B
  import           Data.Char (isDigit)
  import           Data.String (fromString)
  import qualified Data.Conduit as C
  import           Data.Conduit ((.|))
  import           Data.Conduit.Network
  import           Data.Conduit.Network.TLS
  import           Control.Monad (forever)
  import           Control.Monad.Trans (liftIO)
  import           Control.Monad.IO.Class (MonadIO)
  import           Control.Concurrent
  import           System.Environment
  import           Network.Socket
  import           Network.Connection

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
                   cfg  = serverSettings c hp 
                in runTCPServer cfg session
    where session ad = do
            op <- newChan
            ip <- newChan
            _  <- forkIO (client s op ip)
            _  <- forkIO (response ad ip)
            C.runConduitRes (appSource ad .| pipeSink op)

  client :: Int -> Chan B.ByteString -> Chan B.ByteString -> IO ()
  client p op ip = let tlss = TLSSettingsSimple {
                                   settingDisableCertificateValidation = True,
                                   settingDisableSession               = False,
                                   settingUseServerName                = False}
                       cfg  = (tlsClientConfig p $ B.pack "localhost"){
                               tlsClientTLSSettings=tlss}
                    in runTLSClient cfg $ \ad -> do
    _ <- forkIO (request ad op)
    C.runConduitRes (appSource ad .| pipeSink ip)

  request :: AppData -> Chan B.ByteString -> IO ()
  request ad op = C.runConduitRes (pipeSource op .| appSink ad)

  response :: AppData -> Chan B.ByteString -> IO ()
  response ad ip = C.runConduitRes (pipeSource ip .| appSink ad)

  pipeSink :: MonadIO m => Chan B.ByteString -> C.ConduitT B.ByteString C.Void m ()
  pipeSink ch = C.awaitForever (\i -> liftIO (writeChan ch i))

  pipeSource :: MonadIO m => Chan B.ByteString -> C.ConduitT () B.ByteString m ()
  pipeSource ch = forever (liftIO (readChan ch) >>= C.yield)
