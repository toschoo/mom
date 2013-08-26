-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Broker/Client.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Majordomo Client
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Broker.Client (
                   Client, withClient, checkService,
                   request, checkResult)
where

  import qualified Data.ByteString.Char8  as B

  import           Control.Monad.Trans (liftIO)
  import           Control.Exception   (throwIO)
  import           Data.Conduit (($=), (=$))
  import qualified Data.Conduit          as C
  import qualified System.ZMQ as Z

  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams
  import           Network.Mom.Patterns.Broker.Common 

  -------------------------------------------------------------------------
  -- | Client data type
  -------------------------------------------------------------------------
  data Client = Client {clSock    :: Z.Socket Z.XReq,
                        clService :: Service}

  ------------------------------------------------------------------------
  -- | Create a client and start the action, 
  --   in whose scope the client lives;

  --   * 'Context'        - The zeromq context
  --
  --   * 'Service'        - The client service name;
  --                        the client will be specialised
  --                        for requesting this service.
  --
  --   * 'String'         - The address to connect to.
  --
  --   * 'LinkType'       - This parameter is ignored;
  --                        the majordomo protocol expects clients
  --                        to connect to the broker.
  --
  --   * 'Client' -> IO a - The action loop
  ------------------------------------------------------------------------
  withClient :: Context          ->
                Service          -> 
                String           ->
                LinkType         ->
                (Client -> IO a) -> IO a
  withClient ctx srv add _ act =
    Z.withSocket ctx Z.XReq $ \s -> do
      link Connect s add []
      act $ Client s srv

  ------------------------------------------------------------------------
  -- | Service discovery:
  --   The function checks whether the client's service 
  --   is provided by the broker.
  --   
  --   Return values:
  --
  --   * Nothing: The broker timed out
  --
  --   * Just False: The service is not available
  --
  --   * Just True: The service is available
  ------------------------------------------------------------------------
  checkService :: Client -> Timeout -> IO (Maybe Bool)
  checkService c tmo = do
    runSender   (clSock c) $ mdpSrc mmi (C.yield $ B.pack $ clService c)
    runReceiver (clSock c) tmo $ mdpSnk mmi mmiResponse 
    where mmi = B.unpack $ mmiHdr `B.append` mmiSrv
          mmiResponse = do
            mbX <- C.await
            case mbX of
              Nothing -> liftIO (putStrLn "Timeout!") >> return Nothing
              Just x  | x == mmiFound    -> return $ Just True
                      | x == mmiNotFound -> return $ Just False
                      | x == mmiNimpl    -> liftIO (throwIO $
                          MMIExc "MMI Service Request not available")
                      | otherwise        -> liftIO (throwIO $
                          MMIExc $ "Unexpected response code "  ++
                                   "from mmi.service request: " ++
                                        B.unpack x)
                        
  ------------------------------------------------------------------------
  -- | Request a service:
  --
  --   * 'Client' - The client, through which the service is requested
  --
  --   * 'Timeout' - Timeout in microseconds, -1 to wait eternally.
  --                 With timeout = 0, the function returns immediately
  --                 with 'Nothing'.
  --                 When the timeout expires, request is abandoned. 
  --                 In this case, the result of the request
  --                 is Nothing.
  --               
  --   * 'Source'  - The source of the request stream;
  --                 the format of the request will probably comply
  --                 with some communication protocol,
  --                 as, for instance, in the majordomo pattern.
  --
  --   * 'SinkR'   - The sink receiving the reply. The result of the sink
  --                 is returned as the request's overall result.
  --                 Note that the sink may perform different 
  --                 actions on the segments of the resulting stream,
  --                 /e.g./ storing data in a database,
  --                 and return the number of records received.
  ------------------------------------------------------------------------
  request :: Client -> Timeout -> Source -> SinkR (Maybe a) -> IO (Maybe a)
  request c tmo src snk = do
    runSender   (clSock c)     $ mdpSrc (clService c) src
    if tmo /= 0 then runReceiver (clSock c) tmo $ mdpSnk (clService c) snk
                else return Nothing

  ------------------------------------------------------------------------
  -- | Check for a of a previously requested result;
  --   use case: request with timout 0, do some work
  --             and check for a result later.
  --   Do not use this function without having requested the service
  --      previously.
  --   The parameters equal those of 'request',
  --   but do not include a 'Source'.
  ------------------------------------------------------------------------
  checkResult :: Client -> Timeout -> SinkR (Maybe a) -> IO (Maybe a)
  checkResult c tmo snk | tmo == 0  = return Nothing
                        | otherwise = 
    runReceiver (clSock c) tmo $ mdpSnk (clService c) snk
    
  ------------------------------------------------------------------------
  -- Add MDP headers to a stream created by source
  ------------------------------------------------------------------------
  mdpSrc :: Service -> Source -> Source
  mdpSrc sn src = src $= mdpCSndReq sn 

  ------------------------------------------------------------------------
  -- Parse and remove the MDP headers 
  -- before the stream is passed over to a sink
  ------------------------------------------------------------------------
  mdpSnk :: Service -> SinkR (Maybe a) -> SinkR (Maybe a)
  mdpSnk sn snk = mdpCRcvRep sn =$ snk
