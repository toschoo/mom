-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Basic/Server.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Server side of \'Client\/Server\'
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Basic.Server (

         -- * Server

         withServer, 

         -- * Queue
         
         withQueue)
where

  import Data.Conduit ((=$))
  import Network.Mom.Patterns.Types
  import Network.Mom.Patterns.Streams

  ------------------------------------------------------------------------
  -- | Start a server as a background process
  -- 
  --   * 'Context'   - The zeromq context
  --  
  --   * 'Service'   - Service name
  --
  --   * 'String'    - The address to link to
  --
  --   * 'LinkType'  - Whether to connect to or to bind the address
  --   
  --   * 'OnError_'  - Error handler
  --  
  --   * 'Conduit_'  - The application-defined stream transformer;
  --                   the conduit receives the request as input stream
  --                   and should create the output stream that is
  --                   internally sent back to the client
  --
  --   * 'Control' a - Control action
  --
  -- A very simple example, which just sends the incoming stream
  -- back to the client ('bounce'):
  --
  -- >  import           Control.Monad (forever)
  -- >  import           Control.Concurrent
  -- >  import           Network.Mom.Patterns.Basic.Server
  -- >  import           Network.Mom.Patterns.Types
  --
  -- >  main :: IO ()
  -- >  main = withContext 1 $ \ctx -> 
  -- >             withServer ctx "Bouncer" "tcp://*:5555" Bind
  -- >                        (\_ _ _ -> return ()) -- ignore error
  -- >                        bounce $ \_ -> forever $ threadDelay 100000
  -- >    where bounce = passThrough
  ------------------------------------------------------------------------
  withServer :: Context    ->
                Service    -> 
                String     ->
                LinkType   ->
                OnError_   ->
                Conduit_   ->
                Control a  -> IO a
  withServer ctx srv add lt onErr serve =
    withStreams ctx srv (-1) 
                [Poll "client" add DealerT lt [] []]
                igTmo
                onErr
                job 
    where job s   = serve =$ passAll s ["client"]
          igTmo _ = return ()

  ------------------------------------------------------------------------
  -- | A simple load balancer device to link clients and servers.
  --
  --   * 'Context'            - The zeromq context
  --   
  --   * 'Service'            - The service name of this queue
  --
  --   * (String, 'LinkType') - Address and link type, to where clients
  --                            connect. Note if clients connect,
  --                            the queue must bind the address!
  --
  --   * (String, 'LinkType') - Address and link type, to where servers
  --                            connect. Note, again, that 
  --                            if servers connect, the queue must
  --                            bind the address!
  --
  --   * 'OnError_'           - Error handler
  --
  --   * 'Control' a          - 'Controller' action
  ------------------------------------------------------------------------
  withQueue :: Context            ->
               Service            ->
               (String, LinkType) ->
               (String, LinkType) ->
               OnError_           ->
               Control a          -> IO a
  withQueue ctx srv (rout, routl)
                    (deal, deall) onErr =
    withStreams ctx srv (-1) 
                [Poll "client" rout RouterT routl [] [],
                 Poll "server" deal DealerT deall [] []]
                onTmo
                onErr
                job
    where job s = let target | getSource s == "client" = "server"
                             | otherwise               = "client"
                   in passAll s [target]
          onTmo _ = return ()

