module Network.Mom.Patterns.Broker.Server
where

  import           Control.Monad.Trans (liftIO)
  import           Control.Monad (unless, when)
  import           Control.Applicative ((<$>))
  import           Prelude hiding (catch)
  import           Control.Exception (catch, try, SomeException, throwIO,
                                      bracket, bracketOnError, finally)
  import           Control.Concurrent
  import           Data.Conduit (($$), (=$), (=$=))
  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
  import           Data.Char (toLower)
  import           Data.Map (Map)
  import qualified Data.Map as Map
  import qualified System.ZMQ            as Z

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
  import qualified Network.Mom.Patterns.Basic.Server as S
  import           Network.Mom.Patterns.Broker.Common 

  withServer :: Z.Context            ->
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
              (putStrLn "disconnect") -- send c S.outStream mdpWDisconnect)
    where job s = mdpServe s =$ passAll s ["client"]
          mdpServe s = do
            f <- mdpWRcvReq
            case f of
              WRequest is -> serve s =$= mdpWSndRep is
              WBeat    i  -> mdpWBeat
              WDisc    i  -> liftIO (throwIO $ ProtocolExc 
                                           "Broker disconnects")
              x           -> liftIO (throwIO $ Ouch 
                                       "Unknown frame from Broker!")
                        
