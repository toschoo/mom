module Network.Mom.Patterns.Basic.Pusher
where

  import           Control.Monad.Trans (liftIO)
  import qualified System.ZMQ            as Z

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams

  newtype Pipe = Pipe {pipSock :: Z.Socket Z.Push}

  withPipe :: Context        ->
              String         -> 
              LinkType       ->
              (Pipe -> IO a) -> IO a
  withPipe ctx add lt act = 
    Z.withSocket ctx Z.Push $ \s -> link lt s add [] >> (act $ Pipe s)

  push :: Pipe -> Source -> IO ()
  push p src = runSender (pipSock p) src


