module Network.Mom.Patterns.Broker.Client (
                   Client, withClient,
                   request, checkReceive)
where

  import qualified Data.ByteString        as B
  import qualified Data.ByteString.Char8  as BC

  import           Control.Monad.Trans (liftIO)
  import           Control.Applicative ((<$>))
  import           Data.Conduit (($=), (=$))
  import qualified Data.Conduit          as C
  import qualified System.ZMQ as Z

  import qualified Network.Mom.Patterns.Basic.Client as Basic
  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
  import           Network.Mom.Patterns.Broker.Common 

  data Client = Client {clSock    :: Z.Socket Z.XReq,
                        clService :: Service}

  withClient :: Z.Context        ->
                Service          -> 
                String           ->
                LinkType         ->
                (Client -> IO a) -> IO a
  withClient ctx srv add _ act =
    Z.withSocket ctx Z.XReq $ \s -> do
      link Connect s add []
      act $ Client s srv

  request :: Client -> Timeout -> Source -> SinkR (Maybe a) -> IO (Maybe a)
  request c tmo src snk = do
    runSender   (clSock c)     $ mdpSrc (clService c) src
    runReceiver (clSock c) tmo $ mdpSnk (clService c) snk

  checkReceive :: Client -> Timeout -> SinkR (Maybe a) -> IO (Maybe a)
  checkReceive c tmo snk = 
    runReceiver (clSock c) tmo $ mdpSnk (clService c) snk
    
  mdpSrc :: Service -> Source -> Source
  mdpSrc sn src = src $= mdpCSndReq sn 

  mdpSnk :: Service -> SinkR (Maybe a) -> SinkR (Maybe a)
  mdpSnk sn snk = mdpCRcvRep sn =$ snk
