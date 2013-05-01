module Network.Mom.Patterns.Basic.Server
where

  import           Control.Monad.Trans (liftIO)
  import           Data.Conduit (($$), (=$), (=$=))

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams

  withServer :: Context              ->
                Service              -> 
                String               ->
                LinkType             ->
                StreamAction         ->
                OnError_             ->
                StreamConduit        ->
                (Controller -> IO a) -> IO a
  withServer ctx srv add lt onTmo onErr serve =
    withStreams ctx srv (-1) 
                [Poll "client" add ServerT lt [] []]
                onTmo
                onErr
                job 
    where job s = serve s =$ passAll s outStream

  withQueue :: Context              ->
               Service              ->
               (String, LinkType)   ->
               (String, LinkType)   ->
               OnError_             ->
               (Controller -> IO a) -> IO a
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

  outStream :: [Identifier]
  outStream = ["client"]

