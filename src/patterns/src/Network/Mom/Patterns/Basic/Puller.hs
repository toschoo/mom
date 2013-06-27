module Network.Mom.Patterns.Basic.Puller
where

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams

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

