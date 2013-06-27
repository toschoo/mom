module Network.Mom.Patterns.Basic.Client
where

  import qualified System.ZMQ            as Z

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams

  data Client = Client {clSock    :: Z.Socket Z.Req,
                        clService :: Service}

  withClient :: Context          ->
                Service          -> 
                String           ->
                LinkType         ->
                (Client -> IO a) -> IO a
  withClient ctx srv add lt act =
    Z.withSocket ctx Z.Req $ \s -> do
      link lt s add []
      act $ Client s srv

  request :: Client -> Timeout -> Source -> SinkR (Maybe a) -> IO (Maybe a)
  request c tmo src snk = do
    runSender   (clSock c) src
    if tmo /= 0 then runReceiver (clSock c) tmo snk
                else return Nothing

