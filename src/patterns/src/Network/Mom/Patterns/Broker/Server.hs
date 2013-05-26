{-# Language RankNTypes #-}
module Network.Mom.Patterns.Broker.Server
where

  import           Control.Monad.Trans (liftIO)
  import           Control.Monad (unless, when)
  import           Control.Applicative ((<$>))
  import           Prelude hiding (catch)
  import           Control.Exception (catch, try, SomeException, throwIO,
                                      bracket, bracketOnError, finally)
  import           Data.Conduit (($$), (=$), (=$=))

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
  import           Network.Mom.Patterns.Broker.Common 

  withServer :: Context              ->
                Service              -> 
                String               ->
                LinkType             ->
                StreamAction         ->
                OnError_             ->
                StreamConduit        ->
                (Controller -> IO a) -> IO a
  withServer ctx srv add _ onTmo onErr serve act = 
    withStreams ctx srv (-1) 
                [Poll "client" add DealerT Connect [] []]
                onTmo
                onErr
                job $ \c ->
      finally (send c ["client"] (mdpWConnect srv) >> act c)
              (send c ["client"]  mdpWDisconnect)
    where job s | getSource s == "client" = mdpServe s =$ passAll s ["client"]
                | otherwise               = return ()
          mdpServe s = do
            f <- mdpWRcvReq
            case f of
              WRequest is -> serve s =$= mdpWSndRep is
              WBeat    _  -> mdpWBeat
              WDisc    _  -> liftIO (throwIO $ ProtocolExc 
                                           "Broker disconnects")
              _           -> liftIO (throwIO $ Ouch 
                                       "Unknown frame from Broker!")
                        
