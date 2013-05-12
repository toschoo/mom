module Network.Mom.Patterns.Broker.Bridge
where

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
  import           Network.Mom.Patterns.Broker.Common
  import           Network.Mom.Patterns.Broker.Client
  import           Network.Mom.Patterns.Broker.Server

  import qualified Data.ByteString        as B
  import qualified Data.ByteString.Char8  as BC
  import qualified Data.Conduit           as C
  import           Data.Conduit (($$))

  import           Control.Monad.Trans (liftIO)
  import           Prelude hiding (catch)
  import           Control.Exception (SomeException, throwIO,
                                      catch, try, finally)

  withBridge :: Context  -> Service -> String -> String -> 
                OnError_ -> (Controller -> IO a)        -> IO a
  withBridge ctx srv aServer aClient onerr act = 
    withStreams ctx srv (-1)
                [Poll "source" aServer DealerT Connect [] [],
                 Poll "target" aClient DealerT Connect [] []]
                onTmo onerr bridge $ \c ->
      finally (send c ["source"] (mdpWConnect srv) >> act)
              (send c ["source"] mdpWDisconnect)
    where onTmo _    = return ()
          bridge s | getSource s == "source" = handleForth s
                   | otherwise               = handleBack  s 
          handleForth s = do
            f <- mdpWRcvReq
            case f of
              WRequest is -> mdpCSndReq srv -- now we forget the client!
              WBeat    i  -> mdpWBeat
              WDisc    i  -> liftIO (throwIO $ ProtocolExc 
                                           "Broker disconnects")
              x           -> liftIO (throwIO $ Ouch 
                                       "Unknown frame from Broker!")
            
