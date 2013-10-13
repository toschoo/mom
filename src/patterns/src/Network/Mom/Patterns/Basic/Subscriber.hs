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
         Sub, withSub, subscribe, checkSub, withSubVarR, withSubVar_)
         
where

  import qualified Data.Conduit          as C
  import qualified System.ZMQ            as Z
  import           Control.Concurrent (MVar, modifyMVar_, forkIO, killThread)
  import           Control.Monad      (forever)
  import           Control.Exception  (bracket)
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
  checkSub s tmo snk = runReceiver (subSock s) tmo (subSnk snk)

  subSnk :: SinkR (Maybe a) -> SinkR (Maybe a)
  subSnk snk = do
    mb <- C.await -- subscription header is filtered out!
    case mb of
      Nothing -> return Nothing
      Just _  -> snk

  withSubVar_ :: Context -> String -> LinkType -> [Service] ->
                 MVar a  -> (MVar a -> SinkR (Maybe ()))    -> IO () -> IO ()
  withSubVar_ ctx a l ts m snk act = bracket (forkIO subvar)
                                             killThread
                                             (\_ -> act)
    where subvar = withSub ctx a l $ \s -> 
                     subscribe s ts >> forever (do
                       _ <- runReceiver (subSock s) (-1) (subSnk (snk m))
                       return ())

  withSubVarR :: Context -> String -> LinkType -> [Service] ->
                 MVar a  -> SinkR (Maybe a)    -> IO ()     -> IO ()
  withSubVarR ctx a l ts m snk act = bracket (forkIO subvar)
                                             killThread
                                             (\_ -> act)
    where subvar = withSub ctx a l $ \s -> 
                     subscribe s ts >> forever (do
                       mbR <- runReceiver (subSock s) (-1) (subSnk snk)
                       case mbR of
                         Nothing -> return ()
                         Just r  -> modifyMVar_ m $ \_ -> return r)

