module Network.TLS.Bridge.Bridge
where

  import qualified Data.ByteString.Char8 as B
  import qualified Data.Conduit as C
  import           Data.Conduit (($$),(=$))
  import           Data.Conduit.Network
  import           Data.Conduit.Network.TLS
  import           Control.Monad (forever)
  import           Control.Monad.Trans (liftIO)
  import           Control.Concurrent
  import           Control.Exception (finally)

  tcpClient :: Int -> Chan B.ByteString -> Chan B.ByteString -> IO ()
  tcpClient p sp cp = let cl = clientSettings p (B.pack "localhost")
                       in runTCPClient cl $ \ad -> 
    withThread (request ad sp) (appSource ad $$ pipeSink cp)

  tlsClient :: TLSClientConfig -> Chan B.ByteString -> 
                                  Chan B.ByteString -> IO ()
  tlsClient cfg sp cp = runTLSClient cfg $ \ad -> 
    withThread (request ad sp) (appSource ad $$ pipeSink cp)

  request :: AppData -> Chan B.ByteString -> IO ()
  request ad sp = pipeSource sp $$ out =$ appSink ad

  response :: AppData -> Chan B.ByteString -> IO ()
  response ad cp = pipeSource cp $$ out =$ appSink ad

  out :: C.ConduitM B.ByteString B.ByteString IO ()
  out = C.awaitForever (\i -> liftIO (print i) >> C.yield i)

  pipeSink :: Chan B.ByteString -> C.Sink B.ByteString IO ()
  pipeSink ch = C.awaitForever (\i -> liftIO (writeChan ch i))

  pipeSource :: Chan B.ByteString -> C.Source IO B.ByteString 
  pipeSource ch = forever (liftIO (readChan ch) >>= C.yield)

  ------------------------------------------------------------------------
  -- Juggle with threads
  ------------------------------------------------------------------------
  withThread :: IO () -> IO r -> IO r
  withThread thrd act = do 
    stp <- newEmptyMVar 
    th  <- forkIO (thrd `finally` putMVar stp ())
    act `finally` (killThread th >> takeMVar stp)

  ------------------------------------------------------------------------
  -- TLS Client to unsecure Server
  ------------------------------------------------------------------------
  tls2Server :: Int -> TLSConfig -> IO r -> IO r
  tls2Server s cfg = withThread (runTCPServerTLS cfg session) 
    where session ad = do
            sp <- newChan -- server pipe
            cp <- newChan -- client pipe
            withThread (tcpClient s sp cp) $
              withThread  (response ad cp) (appSource ad $$ pipeSink sp)

  ------------------------------------------------------------------------
  -- Unsecure Client to TLS Server
  ------------------------------------------------------------------------
  client2TLS :: ServerSettings -> TLSClientConfig -> IO r -> IO r
  client2TLS srv cli = withThread (runTCPServer srv session)
    where session ad = do
            sp <- newChan -- server pipe
            cp <- newChan -- client pipe
            withThread (tlsClient cli sp cp) $ 
              withThread (response ad cp) (appSource ad $$ pipeSink sp)

