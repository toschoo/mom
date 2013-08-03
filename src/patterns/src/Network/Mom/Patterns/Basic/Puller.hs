-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Basic/Puller.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Puller side of \'Pipeline\'
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Basic.Puller (
          -- * Puller

          withPuller, 

          -- * Pipeline
 
          withPipe)
where

  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams

  ------------------------------------------------------------------------
  -- | Start a puller as a background service:
  --
  --   * 'Context'   - The zeromq context
  --
  --   * 'Service'   - Service name of this worker
  --
  --   * 'String'    - The address to link to
  --
  --   * 'LinkType'  - Whether to connect to or to bind the address;
  --                   usually you want to connect many workers
  --                   to one pusher
  --   
  --   * 'OnError_'  - Error handler
  --  
  --   * 'Sink'      - The application-defined sink
  --                   that does the job sent down the pipeline
  --
  --   * 'Control' a - Control loop
  ------------------------------------------------------------------------
  withPuller :: Context              ->
                Service              -> 
                String               ->
                LinkType             ->
                OnError_             ->
                Sink                 ->
                (Controller -> IO a) -> IO a
  withPuller ctx srv add lt onErr snk =
    withStreams ctx srv (-1) 
                [Poll "pusher" add PullT lt [] []]
                (\_ -> return())
                onErr
                (\_ -> snk)

  ------------------------------------------------------------------------
  -- | A pipeline extends the capacity of the 
  --   pusher-puller chain;
  --   a pipeline connects to a pusher
  --   and provides an access point to a set of pullers.
  --  
  --   * 'Context'            - The zeromq context
  --   
  --   * 'Service'            - The service name of this queue
  --
  --   * (String, 'LinkType') - Address and link type, to where pullers
  --                            connect. Note: if pullers connect,
  --                            the pipeline must bind the address!
  --
  --   * (String, 'LinkType') - Address and link type that pushers bind.
  --                            Note, again, that 
  --                            if pusher bind, the pipeline must
  --                            connect to the address!
  --
  --   * 'OnError_'           - Error handler
  --
  --   * 'Control' a          - 'Controller' action
  ------------------------------------------------------------------------
  withPipe :: Context              ->
              Service              ->
              (String, LinkType)   ->  -- for pullers 
              (String, LinkType)   ->  -- for pushers
              OnError_             ->
              (Controller -> IO a) -> IO a
  withPipe ctx srv (pus, pust)
                   (pul, pult) onErr =
    withStreams ctx srv (-1)
                [Poll "pusher" pus PipeT pust [] [],
                 Poll "puller" pul PullT pult [] []]
                (\_ -> return ()) onErr job
    where job s = passAll s ["pusher"]

