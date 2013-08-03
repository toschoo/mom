-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Basic/Client.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
-- 
-- Client side of Client/Server
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Basic.Client (
         Client, clService, withClient, request)
where

  import qualified System.ZMQ            as Z
  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams

  ------------------------------------------------------------------------
  -- | Client data type
  ------------------------------------------------------------------------
  data Client = Client {clSock    :: Z.Socket Z.Req,
                        clService :: Service} -- currently unused

  ------------------------------------------------------------------------
  -- | Create a client
  --   with name 'Service',
  --   linking to address 'String',
  --   connecting or binding the address according to 'LinkType'
  --   and finally entering the action, in whose scope
  --   the client lives.
  ------------------------------------------------------------------------
  withClient :: Context          ->
                Service          -> 
                String           ->
                LinkType         ->
                (Client -> IO a) -> IO a
  withClient ctx srv add lt act =
    Z.withSocket ctx Z.Req $ \s -> do
      link lt s add []
      act $ Client s srv

  ------------------------------------------------------------------------
  -- | Request a service:
  --
  --   * 'Client' - The client, through which the service is requested
  --
  --   * 'Timeout' - Timeout in microseconds, -1 to wait eternally.
  --                 With timeout = 0, the function returns immediately
  --                 with 'Nothing'.
  --                 When the timeout expires, request is abandoned. 
  --                 In this case, the result of the request
  --                 is Nothing.
  --               
  --   * 'Source'  - The source of the request stream;
  --                 the format of the request will probably comply
  --                 with some communication protocol,
  --                 as, for instance, in the majordomo pattern.
  --
  --   * 'SinkR'   - The sink receiving the reply. The result of the sink
  --                 is returned as the request's overall result.
  --                 Note that the sink may perform different 
  --                 actions on the segments of the resulting stream,
  --                 /e.g./ storing data in a database,
  --                 and return the number of records received.
  --
  -- A \'hello world\' Example:
  --
  -- >  import qualified Data.Conduit          as C
  -- >  import qualified Data.ByteString.Char8 as B
  -- >  import           Network.Mom.Patterns.Basic.Client
  -- >  import           Network.Mom.Patterns.Types
  --
  -- >  main :: IO ()
  -- >  main = withContext 1 $ \ctx -> 
  -- >           withClient ctx "test" 
  -- >               "tcp://localhost:5555" Connect $ \c -> do
  -- >             mbX <- request c (-1) src snk
  -- >             case mbX of
  -- >               Nothing -> putStrLn "No Result"
  -- >               Just x  -> putStrLn $ "Result: " ++ x
  -- >    where src = C.yield (B.pack "hello world")
  -- >          snk = do mbX <- C.await 
  -- >                   case mbX of
  -- >                     Nothing -> return Nothing
  -- >                     Just x  -> return $ Just $ B.unpack x
  ------------------------------------------------------------------------
  request :: Client -> Timeout -> Source -> SinkR (Maybe a) -> IO (Maybe a)
  request c tmo src snk = do
    runSender   (clSock c) src
    if tmo /= 0 then runReceiver (clSock c) tmo snk
                else return Nothing
