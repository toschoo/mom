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
      finally (send c ["client"] connect >> act c)
              (putStrLn "disconnect") -- send c S.outStream disconnect)
    where job s = mdpServe s =$ passAll s ["client"]
          connect    = streamList [B.empty, -- identity delimiter
                                   B.empty,
                                   mdpW01, 
                                   xReady, 
                                   B.pack srv]
          disconnect = streamList [B.empty, -- identity delimiter
                                   B.empty, 
                                   mdpW01,
                                   xDisc]
          mdpServe s = do
            f <- getMDP
            case f of
              RequestFrame is -> serve s =$= mdpOut is
              BeatFrame       -> propagateList beat
              DiscFrame       -> liftIO (throwIO $ ProtocolExc 
                                           "Broker disconnects")
          getMDP = do
            empty -- identity delimiter
            empty
            protocol
            t <- frameType
            case t of
              HeartBeatT  -> return BeatFrame
              DisconnectT -> return DiscFrame
              RequestT    -> RequestFrame <$> identities
              x           -> liftIO (throwIO $ ProtocolExc $ 
                                       "Unexpected Frame: " ++ show x)
          protocol   = chunk mdpW01 "Unknwon Protocol"
          mdpOut is  = propagateList (mdpHead is) >> passThrough
          beat       = [B.empty,
                        B.empty,
                        mdpW01,
                        xHeartBeat]
          mdpHead is = [B.empty, -- identity delimiter
                        B.empty,
                        mdpW01,
                        xReply] ++ toIs is ++ [B.empty]
                        
  ------------------------------------------------------------------------
  -- Broker Messages 
  ------------------------------------------------------------------------
  data Frame = RequestFrame [Identity] 
             | BeatFrame
             | DiscFrame
    deriving (Eq, Show)

