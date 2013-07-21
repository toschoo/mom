-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Broker/Broker.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Majordomo Broker
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Broker.Broker (withBroker)
where

  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams
  import           Network.Mom.Patterns.Broker.Common

  import qualified Registry as R

  import qualified Data.ByteString        as B
  import qualified Data.ByteString.Char8  as BC
  import qualified Data.Conduit           as C
  import           Data.Conduit ((=$), ($$))

  import           Control.Applicative ((<$>))
  import           Control.Monad.Trans (liftIO)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO)

  ------------------------------------------------------------------------
  -- | Start a broker as a background process
  -- 
  --   * 'Context'   - The zeromq context
  --  
  --   * 'Service'   - Service name -
  --                   the service name is for debugging only,
  --                   there is no relation whatsoever
  --                   to services clients request.
  --
  --   * 'Timeout'   - The heartbeat interval
  --
  --   * 'String'    - The address clients connect to
  --
  --   * 'String'    - The address servers connect to
  --
  --   * 'OnError_'  - Error handler
  --  
  --   * 'Control' a - Control action
  ------------------------------------------------------------------------
  withBroker :: Context  -> Service -> Timeout -> String -> String -> 
                OnError_ -> (Controller -> IO r)        -> IO r
  withBroker ctx srv tmo aClients aServers onerr ctrl = 
    withStreams ctx srv tmo
                   [Poll "servers" aServers RouterT Bind [] [],
                    Poll "clients" aClients RouterT Bind [] []]
                   onTmo onerr handleStream $ \c -> R.clean >> ctrl c
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
                 | getSource s == "servers" = recvWorker s
                 | otherwise                = return ()

  ------------------------------------------------------------------------
  -- Receive stream from client
  ------------------------------------------------------------------------
  recvClient :: StreamSink
  recvClient s = mdpCRcvReq >>= uncurry go
    where go i sn | hdr sn == mmiHdr = handleMMI i sn
                  | otherwise        = handleReq i sn
          handleReq i sn = do
            mbW <- liftIO $ R.getWorker sn
            case mbW of
              Nothing -> noWorker sn
              Just w  -> sendRequest w [i] s
          handleMMI i sn | srvc sn /= mmiSrv = C.yield mmiNimpl =$ 
                                                 sendReply sn [i] s
                         | otherwise = do
            mbX <- C.await
            case mbX of
              Nothing -> liftIO (throwIO $ ProtocolExc 
                                   "No ServiceName in mmi.service request")
              Just x  -> do
                m <- bool2MMI <$> liftIO (R.lookupService x)
                C.yield m =$ sendReply sn [i] s
          bool2MMI True  = mmiFound
          bool2MMI False = mmiNotFound
          hdr            = B.take 4
          srvc           = B.drop 4
          noWorker sn    = liftIO (throwIO $ ProtocolExc $
                                  "No Worker for service " ++ BC.unpack sn)

  ------------------------------------------------------------------------
  -- Receive stream from worker 
  ------------------------------------------------------------------------
  recvWorker :: StreamSink 
  recvWorker s = do
    f <- mdpWRcvRep
    case f of
      WBeat  w    -> liftIO (R.updWorkerHb w)
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

  ------------------------------------------------------------------------
  -- Send request to worker
  ------------------------------------------------------------------------
  sendRequest :: Identity -> [Identity] -> StreamSink
  sendRequest w is s = let trg = filterStreams s (== "servers")
                        in mdpWSndReq w is =$ passAll s trg

  ------------------------------------------------------------------------
  -- Send reply to client
  ------------------------------------------------------------------------
  sendReply :: B.ByteString -> [Identity] -> StreamSink
  sendReply sn is s = let trg = filterStreams s (== "clients")
                       in mdpCSndRep sn is =$ passAll s trg

