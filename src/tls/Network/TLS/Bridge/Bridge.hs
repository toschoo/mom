module Network.TLS.Bridge.Bridge (Intercept, out, ignore,
                                  tls2Server, client2TLS)
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

  ------------------------------------------------------------------------
  -- Non-secure client connection
  ------------------------------------------------------------------------
  tcpClient :: Int -> Chan B.ByteString -> 
                      Chan B.ByteString -> Intercept -> IO ()
  tcpClient p sp cp icp = let cl = clientSettings p (B.pack "localhost")
                           in runTCPClient cl $ \ad -> 
    withThread (fwd ad sp icp) 
               (appSource ad $$ pipeSink cp) 

  ------------------------------------------------------------------------
  -- TLS-secured client connection
  ------------------------------------------------------------------------
  tlsClient :: TLSClientConfig -> Chan B.ByteString -> 
                                  Chan B.ByteString -> Intercept -> IO ()
  tlsClient cfg sp cp icp = runTLSClient cfg $ \ad -> 
    withThread (fwd ad sp icp) 
               (appSource ad $$ pipeSink cp)

  ------------------------------------------------------------------------
  -- Forward from channel to appSink
  ------------------------------------------------------------------------
  fwd :: AppData -> Chan B.ByteString -> Intercept -> IO ()
  fwd ad chn icp = pipeSource chn $$ icp =$ appSink ad

  ------------------------------------------------------------------------
  -- Ann Intercept is a conduit from ByteString to ByteString
  ------------------------------------------------------------------------
  type Intercept = C.ConduitM B.ByteString B.ByteString IO ()

  ------------------------------------------------------------------------
  -- Intercept to print to stdout
  ------------------------------------------------------------------------
  out :: Intercept
  out = C.awaitForever (\i -> liftIO (print i) >> C.yield i)

  ------------------------------------------------------------------------
  -- Intercept doing nothing
  ------------------------------------------------------------------------
  ignore :: Intercept
  ignore = C.awaitForever C.yield

  ------------------------------------------------------------------------
  -- Write to Pipe
  ------------------------------------------------------------------------
  pipeSink :: Chan B.ByteString -> C.Sink B.ByteString IO ()
  pipeSink ch = C.awaitForever (\i -> liftIO (writeChan ch i))

  ------------------------------------------------------------------------
  -- Read from Pipe
  ------------------------------------------------------------------------
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
  tls2Server :: Int -> Intercept -> TLSConfig -> IO r -> IO r
  tls2Server s icp cfg = withThread (runTCPServerTLS cfg session) 
    where session ad = do
            sp <- newChan -- server pipe
            cp <- newChan -- client pipe
            withThread (tcpClient s sp cp icp) $
              withThread  (fwd ad cp icp) (appSource ad $$ pipeSink sp)

  ------------------------------------------------------------------------
  -- Unsecure Client to TLS Server
  ------------------------------------------------------------------------
  client2TLS :: Intercept -> ServerSettings -> TLSClientConfig -> IO r -> IO r
  client2TLS icp srv cli = withThread (runTCPServer srv session)
    where session ad = do
            sp <- newChan -- server pipe
            cp <- newChan -- client pipe
            withThread (tlsClient cli sp cp icp) $ 
              withThread (fwd ad cp icp) (appSource ad $$ pipeSink sp)

