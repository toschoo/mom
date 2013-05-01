module Network.Mom.Patterns.Basic.Publisher
where

  import           Control.Monad.Trans (liftIO)
  import           Control.Monad (unless, when)
  import           Control.Applicative ((<$>))
  import           Prelude hiding (catch)
  import           Control.Exception (catch, try, SomeException, throwIO,
                                      bracket, bracketOnError, finally)
  import           Control.Concurrent
  import           Data.Conduit (($$), ($=), (=$=))
  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
  import           Data.List (intercalate)
  import           Data.Map (Map)
  import qualified Data.Map as Map
  import qualified System.ZMQ            as Z

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams

  newtype Pub = Pub {pubSock :: Z.Socket Z.Pub}

  withPub :: Context       ->
             String        -> 
             LinkType      ->
             (Pub -> IO a) -> IO a
  withPub ctx add lt act = 
    Z.withSocket ctx Z.Pub $ \s -> 
      link lt s add [] >> (act $ Pub s)

  issue :: Pub -> [Service] -> Source -> IO ()
  issue p topics src = runSender (pubSock p) pubSrc
    where pubSrc = let ts = B.pack $ intercalate "," topics
                    in streamList [ts] >> src

  withForwarder :: Context              ->
                   Service              ->
                   [Service]            ->
                   (String, LinkType)   ->  -- subscribers
                   (String, LinkType)   ->  -- publishers
                   OnError_             ->
                   (Controller -> IO a) -> IO a
  withForwarder ctx srv topics (pub, pubt)
                               (sub, subt) onErr =
    withStreams ctx srv (-1)
                [Poll "sub" sub SubT subt topics [],
                 Poll "pub" pub PubT pubt []     []]
                (\_ -> return ()) onErr job
    where job s = passAll s ["pub"]
