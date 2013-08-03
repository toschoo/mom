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
  import           Control.Exception (throwIO, finally)
  import           Control.Concurrent.MVar

  import           System.IO.Unsafe

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
                                                      | otherwise = 
    lockBroker $ \_ -> do
      R.clean
      R.setHbPeriod tmo
      t <- getCurrentTime
      m <- newMVar t
      withStreams ctx srv (1000 * fromIntegral tmo)
                  [Poll "servers" aServers RouterT Bind [] [],
                  Poll "clients" aClients RouterT Bind [] []]
                  (handleTmo    m tmo) onerr 
                  (handleStream m tmo) ctrl 

  -- handle heartbeat -------------------------------------------------------
  handleTmo :: MVar UTCTime -> Msec -> Streamer -> IO ()
  handleTmo m tmo s = hbPeriodReached m tmo >>= \x -> 
                         when x $ R.checkWorker >>= mapM_ sndHb
    where hbM   i   = [i, B.empty, mdpW01, xHeartBeat]
          sndHb i   = C.runResourceT $ streamList (hbM i) $$  
                                       passAll s ["servers"]
 
  ------------------------------------------------------------------------
  -- Handle incoming Streams
  ------------------------------------------------------------------------
  handleStream :: MVar UTCTime -> Msec -> StreamSink
  handleStream m x s = 
    let action | getSource s == "clients" = recvClient s
               | getSource s == "servers" = recvWorker s
               | otherwise                = return ()
        -- and handle heartbeat afterwards--------------------------------
     in action >> liftIO (handleTmo m x s)

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
              Nothing -> liftIO (throwIO $ MMIExc 
                                   "No ServiceName in mmi.service request")
              Just x  -> do
                m <- bool2MMI <$> liftIO (R.lookupService x)
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
  recvWorker :: StreamSink 
  recvWorker s = do
    f <- mdpWRcvRep
    case f of
      WBeat  w    -> liftIO (R.updWorkerHb w) -- update his heartbeat 
      WReady w sn -> liftIO $ R.insert w sn   -- insert new worker
      WReply w is -> handleReply w is s       -- handle reply from worker
      WDisc  w    -> liftIO $ R.remove w      -- disconnect from worker
      _           -> liftIO (throwIO $ Ouch "Unexpected Frame from Worker!")

  ------------------------------------------------------------------------
  -- Handle reply
  ------------------------------------------------------------------------
  handleReply :: Identity -> [Identity] -> StreamSink
  handleReply w is s = do
    mbS <- liftIO $ R.getServiceName w
    case mbS of
      Nothing -> liftIO (throwIO $ ServerExc "Unknown Worker")
      Just sn -> sendReply sn is s >> liftIO (R.freeWorker w)

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

  ------------------------------------------------------------------------
  -- Mechanism to exclude two brokers 
  -- from running in the same process
  ------------------------------------------------------------------------
  {-# NOINLINE _brk #-}
  _brk :: MVar () 
  _brk = unsafePerformIO $ newMVar ()

  lockBroker :: (() -> IO b) -> IO b
  lockBroker act = do
    mb_ <- tryTakeMVar _brk -- withMVar _brk 
    case mb_ of
      Nothing -> throwIO $ SingleBrokerExc "Another broker is running!"
      Just _  -> finally (act ()) (putMVar _brk ())


