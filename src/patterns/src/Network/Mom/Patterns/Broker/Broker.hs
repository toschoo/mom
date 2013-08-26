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

  import qualified Registry  as R
  import           Heartbeat (hbPeriodReached)     

  import qualified Data.ByteString        as B
  import qualified Data.ByteString.Char8  as BC
  import qualified Data.Conduit           as C
  import           Data.Conduit ((=$), ($$))
  import           Data.Time.Clock

  import           Control.Applicative ((<$>))
  import           Control.Monad.Trans (liftIO)
  import           Control.Monad       (when)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO)
  import           Control.Concurrent.MVar

  ------------------------------------------------------------------------
  -- | Start a broker as a background process
  -- 
  --   * 'Context'   - The zeromq context
  --  
  --   * 'Service'   - Service name -
  --                   the service name is for debugging only,
  --                   there is no relation whatsoever
  --                   to the service of the Majordomo Protocol.
  --
  --   * 'Msec'      - The heartbeat interval in milliseconds,
  --                   which should be equal 
  --                   for all workers and the broker 
  --
  --   * 'String'    - The address clients connect to
  --
  --   * 'String'    - The address servers connect to
  --
  --   * 'OnError_'  - Error handler
  --  
  --   * 'Control' a - Control action
  ------------------------------------------------------------------------
  withBroker :: Context  -> Service -> Msec -> String -> String -> 
                OnError_ -> (Controller -> IO r)                -> IO r
  withBroker ctx srv tmo aClients aServers onerr ctrl | tmo <= 0  = 
    throwIO $ MDPExc "Heartbeat is mandatory"
                                                      | otherwise = do
    r <- R.newReg tmo
    t <- getCurrentTime
    m <- newMVar t
    withStreams ctx srv (1000 * fromIntegral tmo)
                  [Poll "servers" aServers RouterT Bind [] [],
                   Poll "clients" aClients RouterT Bind [] []]
                  (handleTmo    r m tmo) onerr 
                  (handleStream r m tmo) ctrl 

  ------------------------------------------------------------------------
  -- handle heartbeat:
  -- - remove unresponsive workers 
  -- - heartbeat those that have been inactive for at least one hb period
  ------------------------------------------------------------------------
  handleTmo :: R.Registry -> MVar UTCTime -> Msec -> Streamer -> IO ()
  handleTmo r m tmo s = hbPeriodReached m tmo >>= \x -> 
                          when x $ R.checkWorker r >>= mapM_ sndHb
    where hbM   i   = [i, B.empty, mdpW01, xHeartBeat]
          sndHb i   = C.runResourceT $ streamList (hbM i) $$  
                                       passAll s ["servers"]
 
  ------------------------------------------------------------------------
  -- Handle incoming Streams
  ------------------------------------------------------------------------
  handleStream :: R.Registry -> MVar UTCTime -> Msec -> StreamSink
  handleStream r m x s = 
    let action | getSource s == "clients" = recvClient r s
               | getSource s == "servers" = recvWorker r s
               | otherwise                = return ()
        -- and handle heartbeat afterwards--------------------------------
     in action >> liftIO (handleTmo r m x s)

  ------------------------------------------------------------------------
  -- Receive stream from client
  ------------------------------------------------------------------------
  recvClient :: R.Registry -> StreamSink
  recvClient r s = mdpCRcvReq >>= uncurry go
    where go i sn | hdr sn == mmiHdr = handleMMI i sn
                  | otherwise        = handleReq i sn
          handleReq i sn = do
            mbW <- liftIO $ R.getWorker r sn
            case mbW of
              Nothing -> noWorker sn
              Just w  -> sendRequest w [i] s
          handleMMI i sn | srvc sn /= mmiSrv = C.yield mmiNimpl =$ 
                                                 sendReply sn [i] s
                         | otherwise = do
            mbX <- C.await
            case mbX of
              Nothing -> liftIO (throwIO $ MMIExc 
                                   "No ServiceName in mmi.service request")
              Just x  -> do
                m <- bool2MMI <$> liftIO (R.lookupService r x)
                C.yield m =$ sendReply sn [i] s
          bool2MMI True  = mmiFound
          bool2MMI False = mmiNotFound
          hdr            = B.take 4
          srvc           = B.drop 4
          noWorker sn    = liftIO (throwIO $ BrokerExc $
                                  "No Worker for service " ++ BC.unpack sn)

  ------------------------------------------------------------------------
  -- Receive stream from worker 
  ------------------------------------------------------------------------
  recvWorker :: R.Registry -> StreamSink 
  recvWorker r s = do
    f <- mdpWRcvRep
    case f of
      WBeat  w    -> liftIO (R.updWorkerHb r w) -- update his heartbeat 
      WReady w sn -> liftIO $ R.insert r w sn   -- insert new worker
      WReply w is -> handleReply r w is s       -- handle reply from worker
      WDisc  w    -> liftIO $ R.remove r w      -- disconnect from worker
      _           -> liftIO (throwIO $ Ouch "Unexpected Frame from Worker!")

  ------------------------------------------------------------------------
  -- Handle reply
  ------------------------------------------------------------------------
  handleReply :: R.Registry -> Identity -> [Identity] -> StreamSink
  handleReply r w is s = do
    mbS <- liftIO $ R.getServiceName r w
    case mbS of
      Nothing -> liftIO (throwIO $ ServerExc "Unknown Worker")
      Just sn -> sendReply sn is s >> liftIO (R.freeWorker r w)

  ------------------------------------------------------------------------
  -- Send request to worker
  ------------------------------------------------------------------------
  sendRequest :: Identity -> [Identity] -> StreamSink
  sendRequest w is s = mdpWSndReq w is =$ passAll s ["servers"]

  ------------------------------------------------------------------------
  -- Send reply to client
  ------------------------------------------------------------------------
  sendReply :: B.ByteString -> [Identity] -> StreamSink
  sendReply sn is s = mdpCSndRep sn is =$ passAll s ["clients"]

