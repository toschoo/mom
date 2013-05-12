module Network.Mom.Patterns.Broker.Broker
where

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
  import           Network.Mom.Patterns.Broker.Common

  import qualified Registry as R

  import qualified Data.ByteString        as B
  import qualified Data.ByteString.Char8  as BC
  import qualified Data.Conduit           as C
  import           Data.Conduit ((=$), ($$))

  import           Control.Concurrent 
  import           Control.Applicative ((<$>))
  import           Control.Monad (when, unless)
  import           Control.Monad.Trans (liftIO)
  import           Prelude hiding (catch)
  import           Control.Exception (SomeException, throwIO,
                                      catch, try, finally)

  withBroker :: Context  -> Service -> String -> String -> 
                OnError_ -> (Controller -> IO ())       -> IO ()
  withBroker ctx srv aClients aServers onerr = 
    withStreams ctx srv 10000
                   [Poll "servers" aServers RouterT Bind [] [],
                    Poll "clients" aClients RouterT Bind [] []]
                   onTmo onerr handleStream 
    where onTmo s = do
            is <- R.checkWorker
            mapM_ (sndHb s) is
          hbM i = [i, B.empty, mdpW01, xHeartBeat]
          sndHb s i = C.runResourceT $ streamList (hbM i) $$  
                                       passAll s ["servers"]
 
  ------------------------------------------------------------------------
  -- Handle incoming Streams
  ------------------------------------------------------------------------
  handleStream :: StreamSink
  handleStream s | getSource s == "clients" = recvClient s
                 | otherwise                = recvWorker s

  ------------------------------------------------------------------------
  -- Receive stream from client
  ------------------------------------------------------------------------
  recvClient :: StreamSink
  recvClient s = do
    (i, sn) <- mdpCRcvReq
    mbW     <- liftIO $ R.getWorker sn
    case mbW of
      Nothing -> noWorker sn
      Just w  -> let trg = filterStreams s (== "servers")
                  in mdpWSndReq w [i] =$ passAll s trg
    where noWorker sn = liftIO $ putStrLn $ 
                                   "No Worker for service " ++ 
                                                  BC.unpack sn

  ------------------------------------------------------------------------
  -- Receive stream from worker 
  ------------------------------------------------------------------------
  recvWorker :: StreamSink 
  recvWorker s = do
    f <- mdpWRcvRep
    case f of
      WBeat  w    -> liftIO (putStrLn "beat") >> liftIO (R.updWorkerHb w)
      WReady w sn -> liftIO $ R.insert w sn
      WReply w is -> handleReply w is s 
      WDisc  w    -> liftIO $ R.remove w
      _           -> liftIO $ throwIO $ Ouch "Unexpected Frame from Worker!"

  ------------------------------------------------------------------------
  -- Handle reply
  ------------------------------------------------------------------------
  handleReply :: Identity -> [Identity] -> StreamSink
  handleReply w is s = do
    mbS <- liftIO $ R.getServiceName w
    case mbS of
      Nothing -> liftIO $ throwIO $ ProtocolExc "Unknown Worker"
      Just sn -> do sendReply sn is s
                    liftIO $ R.freeWorker w
    where sendReply sn is s = let trg = filterStreams s (== "clients")
                               in mdpCSndRep sn is =$ passAll s trg
