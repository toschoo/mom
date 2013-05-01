module Network.Mom.Patterns.Broker.Broker
where

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
  import           Network.Mom.Patterns.Broker.Common

  import qualified Registry as R

  import qualified Data.ByteString        as B
  import qualified Data.ByteString.Char8  as BC
  import qualified Data.Conduit           as C

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
    withStreams ctx srv 1000000 -- (-1)
                   [Poll "servers" aServers RouterT Bind [] [],
                    Poll "clients" aClients RouterT Bind [] []]
                   (\_ -> return ()) onerr
                   handleStream
 
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
    is  <- identities
    protocol
    sn  <- getChunk   -- service name
    mbW <- liftIO $ R.getWorker sn
    case mbW of
      Nothing -> noWorker sn
      Just w  -> let trg = filterStreams s (== "servers")
                  in do part    s trg (c2w w is) 
                        passAll s trg
    where c2w w is = [w, B.empty, -- identity
                      B.empty,
                      mdpW01,
                      xRequest] ++ toIs is ++ [B.empty]
          noWorker sn = liftIO $ putStrLn $ 
                                   "No Worker for service " ++ 
                                                  BC.unpack sn
          protocol    = chunk mdpC01 "Unknown Protocol"

  ------------------------------------------------------------------------
  -- Worker frames
  ------------------------------------------------------------------------
  data WFrame = WBeat  Identity
              | WReady Identity B.ByteString
              | WReply Identity [Identity]
              | WDisc  Identity
    deriving (Eq, Show)

  ------------------------------------------------------------------------
  -- Receive stream from worker 
  ------------------------------------------------------------------------
  recvWorker :: StreamSink 
  recvWorker s = do
    f <- getFrame 
    case f of
      WBeat  w    -> return () -- heartbeating
      WReady w sn -> liftIO $ R.insert w sn
      WReply w is -> handleReply w is s 
      WDisc  w    -> liftIO $ R.remove w

  ------------------------------------------------------------------------
  -- Stream to Frame
  ------------------------------------------------------------------------
  getFrame :: SinkR WFrame
  getFrame = do
    w <- getId
    protocol
    t <- frameType
    case t of
      HeartBeatT  -> return $ WBeat w
      DisconnectT -> return $ WDisc w
      ReadyT      -> WReady w <$> getSrvName
      ReplyT      -> getRep w
      x           -> liftIO $ throwIO $ ProtocolExc $
                       "Unexpected Frame from Worker: " ++ show x
    where protocol   = chunk mdpW01 "Unknown Protocol"
          getSrvName = getChunk
          getId      = do ws <- identities
                          when (null ws) $ liftIO $ throwIO $ 
                                           ProtocolExc "No Worker Id!"
                          return $ head ws
          getRep w   = do is <- identities
                          return $ WReply w is 

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

  ------------------------------------------------------------------------
  -- Send reply to client
  ------------------------------------------------------------------------
  sendReply :: B.ByteString -> [Identity] -> StreamSink
  sendReply sn is s = let trg = filterStreams s (== "clients")
                       in do part    s trg w2c 
                             passAll s trg
    where w2c = toIs is ++ [B.empty,
                            mdpC01,
                            sn]
