-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Basic/Publisher.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Publish side of 'Publish/Subscribe'
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Basic.Publisher (
                 
                     -- * Publisher

                     Pub, withPub, issue, 

                     -- * Forwarder

                     withForwarder
                   )
where

  import qualified Data.ByteString.Char8 as B
  import           Data.List (intercalate)
  import qualified System.ZMQ            as Z

  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams

  ------------------------------------------------------------------------
  -- | Publisher data type
  ------------------------------------------------------------------------
  newtype Pub = Pub {pubSock :: Z.Socket Z.Pub}

  ------------------------------------------------------------------------
  -- | Create and link a publisher:
  --
  --   * 'Context'     -  The zeromq context
  --
  --   * 'String'      -  The service address
  --
  --   * 'LinkType'    -  How to link (bind or connect)
  --
  --   * (Pub -> IO a) -  The action, in whose scope the publisher lives
  ------------------------------------------------------------------------
  withPub :: Context       ->
             String        -> 
             LinkType      ->
             (Pub -> IO a) -> IO a
  withPub ctx add lt act = 
    Z.withSocket ctx Z.Pub $ \s -> 
      link lt s add [] >> act (Pub s)

  ------------------------------------------------------------------------
  -- | Publish data:
  --
  --   * 'Pub'       - The publisher
  --
  --   * ['Service'] - List of topics, to which these data should be
  --                   published
  --
  --   * 'Source'    - Create the stream to publish.
  --                   The first message segment
  --                   contains the subscription header,
  --                   /i.e./ the comma-separated list of topics
  ------------------------------------------------------------------------
  issue :: Pub -> [Service] -> Source -> IO ()
  issue p topics src = runSender (pubSock p) pubSrc
    where pubSrc = let ts = B.pack $ intercalate "," topics
                    in streamList [ts] >> src

  ------------------------------------------------------------------------
  -- | A simple forwarder,
  --   /i.e./ a device that connects to a publisher
  --   and provides an additional endpoint 
  --   for more subscribers to connect to.
  --   A forwarder, hence, is a means to extend 
  --   the capacity of a publisher.
  --
  --   * 'Context'            - The zeromq context
  --
  --   * 'Service'            - The name of the forwarder
  --
  --   * (String, 'LinkType') - access point for subscribers;
  --                            usually, you want to bind
  --                            the address, such that subscribers
  --                            connect to it.
  --
  --   * (String, 'LinkType') - access point for the publisher;
  --                            usually, you want to connect 
  --                            to the publisher.
  --
  --   * 'OnError_'           - Error handler
  --
  --   * 'Control' a          - Control loop
  ------------------------------------------------------------------------
  withForwarder :: Context            ->
                   Service            ->
                   [Service]          ->
                   (String, LinkType) ->  -- subscribers
                   (String, LinkType) ->  -- publishers
                   OnError_           ->
                   Control a          -> IO a
  withForwarder ctx srv topics (pub, pubt)
                               (sub, subt) onErr =
    withStreams ctx srv (-1)
                [Poll "sub" sub SubT subt topics [],
                 Poll "pub" pub PubT pubt []     []]
                (\_ -> return ()) onErr job
    where job s = passAll s ["pub"]
