module Network.Mom.Patterns.Basic.Pusher
where

  import qualified System.ZMQ            as Z

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams

  newtype Pusher = Pusher {pipSock :: Z.Socket Z.Push}

  withPusher :: Context          ->
                String           -> 
                LinkType         ->
                (Pusher -> IO a) -> IO a
  withPusher ctx add lt act = 
    Z.withSocket ctx Z.Push $ \s -> link lt s add [] >> act (Pusher s)

  push :: Pusher -> Source -> IO ()
  push p = runSender (pipSock p) 


