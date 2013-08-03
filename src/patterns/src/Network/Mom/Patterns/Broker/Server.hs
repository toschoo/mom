-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Broker/Server.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Majordomo Server
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Broker.Server (withServer)
where

  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, finally)
  import           Control.Monad.Trans (liftIO)
  import           Control.Monad (when)
  import           Control.Concurrent.MVar
  import           Data.Conduit ((=$), (=$=), ($$))
  import qualified Data.Conduit    as C
  import qualified Data.ByteString as B

  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams
  import           Network.Mom.Patterns.Broker.Common
  import           Heartbeat -- time arithmetics
  import           Data.Time.Clock 

  ------------------------------------------------------------------------
  -- | Start a server as a background process
  -- 
  --   * 'Context'   - The zeromq context
  --  
  --   * 'Service'   - Service name; 
  --                   the service name is used to register
  --                   at the broker.
  -- 
  --   * 'Msec'      - Heartbeat in Milliseconds;
  --                   must be synchronised with the broker heartbeat
  --
  --   * 'String'    - The address to link to
  --
  --   * 'OnError_'  - Error handler
  --  
  --   * 'Conduit_'  - The application-defined stream transformer;
  --                   the conduit receives the request as input stream
  --                   and should create the output stream that is
  --                   internally sent back to the client
  --
  --   * 'Control' a - Control action
  ------------------------------------------------------------------------
  withServer :: Context     ->
                Service     -> 
                Msec        ->
                String      ->
                OnError_    ->
                Conduit_    ->
                (Control a) -> IO a
  withServer ctx srv tmo add onErr serve act | tmo <= 0  =
    throwIO $ MDPExc "Heartbeat is mandatory"
                                             | otherwise = do
    t <- getCurrentTime
    m <- newMVar t                             -- my  heartbeat 
    h <- newMVar (timeAdd t (tolerance * tmo)) -- his heartbeat
    withStreams ctx srv (1000 * fromIntegral tmo)
                [Poll "client" add DealerT Connect [] []]
                (handleTmo m h tmo) onErr
                (job       m h) $ \c ->
      -- connect message, main loop, disconnect message ------------------
      finally (send c ["client"] (mdpWConnect srv) >> act c)
              (send c ["client"]  mdpWDisconnect)

          -- receiv message -----------------------------------------------    
    where job m h s 
            | getSource s == "client" = mdpServe m h s 
            | otherwise               = return ()
          mdpServe m h s = do
            f <- mdpWRcvReq
            case f of
              WRequest is -> liftIO (updBeat h) >>
                             serve =$= mdpWSndRep is =$ passAll s ["client"]
              WBeat    _  -> liftIO (updBeat h)
              WDisc    _  -> liftIO (throwIO $ BrokerExc 
                                           "Broker disconnects")
              _           -> liftIO (throwIO $ Ouch 
                                   "Unknown frame from Broker!")
            liftIO (handleTmo m h tmo s) -- send heartbeat if it's time

          -- update his heartbeat -----------------------------------------
          updBeat h = modifyMVar_ h $ \_ -> do
                        now <- getCurrentTime
                        return (now `timeAdd` (tolerance * tmo))

  ------------------------------------------------------------------------
  -- Send heartbeat if it's time and check broker's state
  ------------------------------------------------------------------------
  handleTmo :: MVar UTCTime -> MVar UTCTime -> Msec -> Streamer -> IO ()
  handleTmo m h tmo s = do hbPeriodReached m tmo >>= \x -> when x sndHb
                           hbDelay               >>= \x -> when x $ throwIO $
                                                     BrokerExc $ "Missing heartbeat"
    where sndHb   = C.runResourceT $ 
                      streamList [B.empty, mdpW01, xHeartBeat] $$  
                      passAll s ["client"]
          hbDelay = getCurrentTime >>= \now -> 
                        readMVar h >>= \t   -> return (now > t)
