module Network.Mom.Patterns.Basic.Puller
where

  import           Control.Monad.Trans (liftIO)

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams

  withPuller :: Context              ->
                Service              -> 
                String               ->
                LinkType             ->
                StreamAction         ->
                OnError_             ->
                Sink                 ->
                (Controller -> IO a) -> IO a
  withPuller ctx srv add lt onTmo onErr snk =
    withStreams ctx srv (-1) 
                [Poll "pusher" add PullT lt [] []]
                onTmo
                onErr
                (\_ -> snk)

  withPipeline :: Context              ->
                  Service              ->
                  (String, LinkType)   ->  -- for pullers 
                  (String, LinkType)   ->  -- for pushers
                  OnError_             ->
                  (Controller -> IO a) -> IO a
  withPipeline ctx srv (pus, pust)
                       (pul, pult) onErr =
    withStreams ctx srv (-1)
                [Poll "pusher" pus PipeT pust [] [],
                 Poll "puller" pul PullT pult [] []]
                (\_ -> return ()) onErr job
    where job s = passAll s ["pusher"]

