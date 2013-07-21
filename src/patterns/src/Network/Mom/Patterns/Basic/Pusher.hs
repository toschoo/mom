-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Basic/Pusher.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Pusher side of \'Pipeline\'
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Basic.Pusher (
                     Pusher, withPusher, push)
where

  import qualified System.ZMQ            as Z
  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams

  ------------------------------------------------------------------------
  -- | The pusher data type
  ------------------------------------------------------------------------
  newtype Pusher = Pusher {pipSock :: Z.Socket Z.Push}

  ------------------------------------------------------------------------
  -- | The function in whose scope the pusher lives:
  --
  --   * 'Context'          - The zeromq Context
  --
  --   * 'String'           - The address
  --
  --   * 'LinkType'         - Link type; usually, you want to bind
  --                          a pusher to its address
  --
  --   * ('Pusher' -> IO a) - Action in whose scope the pusher lives
  ------------------------------------------------------------------------
  withPusher :: Context          ->
                String           -> 
                LinkType         ->
                (Pusher -> IO a) -> IO a
  withPusher ctx add lt act = 
    Z.withSocket ctx Z.Push $ \s -> link lt s add [] >> act (Pusher s)

  ------------------------------------------------------------------------
  -- | Push a job down the pipeline;
  --   the 'Source' creates the outgoing stream.
  ------------------------------------------------------------------------
  push :: Pusher -> Source -> IO ()
  push p = runSender (pipSock p) 


