-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Basic/Subscriber.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Subscriber side of \'Publish Subscribe\'
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Basic.Subscriber (
         Sub, withSub, subscribe, checkSub)
         
where

  import qualified Data.Conduit          as C
  import qualified System.ZMQ            as Z
  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams

  ------------------------------------------------------------------------
  -- | Subscription data type
  ------------------------------------------------------------------------
  newtype Sub = Sub {subSock :: Z.Socket Z.Sub}

  ------------------------------------------------------------------------
  -- | Create a subscription and start the action, in which it lives
  --
  --   * 'Context'       - The zeromq context
  --
  --   * 'String'        - The address 
  --
  --   * 'LinkType'      - The link type, usually Connect
  --
  --   * ('Sub' -> IO a) - The action, in which the subscription lives
  ------------------------------------------------------------------------
  withSub :: Context       ->
             String        -> 
             LinkType      ->
             (Sub -> IO a) -> IO a
  withSub ctx add lt act = 
    Z.withSocket ctx Z.Sub $ \s -> 
      link lt s add [] >> act (Sub s)

  ------------------------------------------------------------------------
  -- | Subscribe to a list of topics;
  --   Note that a subscriber has to subscribe to at least one topic
  --   to receive any data.
  --
  --   * 'Sub'       - The subscriber
  --
  --   * ['Service'] - The list of topics to subscribe to
  ------------------------------------------------------------------------
  subscribe :: Sub -> [Service] -> IO ()
  subscribe s = mapM_ (Z.subscribe $ subSock s) 

  ------------------------------------------------------------------------
  -- | Check for new data:
  --
  --   * 'Sub'     - The subscriber
  --
  --   * 'Timeout' - When timeout expires,
  --                 the function returns 'Nothing'.
  --                 Timeout may be 
  --                 -1  - listen eternally,
  --                 0   - return immediately,
  --                 \> 0 - timeout in microseconds
  --
  --   * 'SinkR'   - Sink the result stream.
  --                 Note that the subscription header,
  --                 /i.e./ a message segment containing
  --                        a comma-separated list 
  --                        of the topics, to which
  --                        the data belong,
  --                 is dropped.
  ------------------------------------------------------------------------
  checkSub :: Sub -> Timeout -> SinkR (Maybe a) -> IO (Maybe a)
  checkSub s tmo snk = runReceiver (subSock s) tmo subSnk
    where subSnk = do
            mb <- C.await -- subscription header is filtered out!
            case mb of
              Nothing -> return Nothing
              Just _  -> snk

