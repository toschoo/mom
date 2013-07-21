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

  import           Control.Monad.Trans (liftIO)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, finally)
  import           Data.Conduit ((=$), (=$=))

  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams
  import           Network.Mom.Patterns.Broker.Common 

  ------------------------------------------------------------------------
  -- | Start a server as a background process
  -- 
  --   * 'Context'   - The zeromq context
  --  
  --   * 'Service'   - Service name; 
  --                   the service name is used to register
  --                   at the broker.
  --
  --   * 'String'    - The address to link to
  --
  --   * 'LinkType'  - The link type;
  --                   this parameter is ignored,
  --                   since the majordomo protocol
  --                   expects servers to connect to the broker; 
  --                   the parameter is only provided to maintain
  --                   the ordinary server interface
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
                String      ->
                LinkType    ->
                OnError_    ->
                Conduit_    ->
                (Control a) -> IO a
  withServer ctx srv add _ onErr serve act = 
    withStreams ctx srv (-1) 
                [Poll "client" add DealerT Connect [] []]
                (\_ -> return ())
                onErr
                job $ \c ->
      finally (send c ["client"] (mdpWConnect srv) >> act c)
              (send c ["client"]  mdpWDisconnect)
    where job s | getSource s == "client" = mdpServe =$ passAll s ["client"]
                | otherwise               = return ()
          mdpServe = do
            f <- mdpWRcvReq
            case f of
              WRequest is -> serve =$= mdpWSndRep is
              WBeat    _  -> mdpWBeat
              WDisc    _  -> liftIO (throwIO $ ProtocolExc 
                                           "Broker disconnects")
              _           -> liftIO (throwIO $ Ouch 
                                       "Unknown frame from Broker!")
                        
