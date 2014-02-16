-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Stompl/Client/Patterns.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- The Stomp specification defines only one queuing pattern:
-- /publish and subscribe/.
-- In some situations, other patterns may be more appropriate
-- such as /peer-to-peer/ or /client server/.
-- Whereas patterns like peer-to-peer 
-- are easy to simulate with the means provided by Stomp,
-- client/server needs some more coordination
-- between the involved parties, the clients and the server.
--
-- This module provides abstractions that implement
-- a simple client/server protocol on top of Stomp.
-- A server is an application that provides a /service/
-- to others.
-- The service must be explicitly requested by a client
-- and only the requesting client must see the response
-- produced by the server.
--
-- The module, basically, provides two data types ('ClientA' and 'ServerA')
-- and two functions working on these data types, namely
-- 'request' and 'reply'.
-- With the request function, the client requests a service
-- and waits for a response.
-- With the reply function, a server waits for a request,
-- produces a response and sends it back through a channel
-- indicated by the client.
--
-- Internally, 'request' and 'reply' use a message header
-- called \"__client__\" to agree on the reply queue
-- 
-------------------------------------------------------------------------------
module Network.Mom.Stompl.Client.Patterns (
                          -- * Client
                          ClientA, withClient, request, checkRequest,
                          -- * Server
                          ServerA, withServer, reply)
where

  import           Network.Mom.Stompl.Client.Queue 
  import qualified Network.Mom.Stompl.Frame as F
  import           System.Timeout
  import qualified Codec.MIME.Type as M
  import           Control.Exception (throwIO)

  ------------------------------------------------------------------------
  -- | The client data type
  ------------------------------------------------------------------------
  data ClientA i o = Cl {
                      clChn :: String,
                      clIn  :: Reader i,
                      clOut :: Writer o}
  
  ------------------------------------------------------------------------
  -- | The function creates a client that lives within its scope.
  --
  --   Parameters:
  --
  --   * 'Con': Connection to a Stomp broker
  --
  --   * 'String': Name of the Client, used for debugging.
  --
  --   * 'ReaderDesc' i: Description of a reader queue;
  --                     this is the queue through which the server
  --                     will send its response.
  --
  --   * 'WriterDesc' o: Description of a writer queue;
  --                     this is the queue through which the server
  --                     is expecting requests.
  --
  --   * 'ClientA' i o -> IO r: An application-defined action
  --                            whose scope defines the client's lifetime
  ------------------------------------------------------------------------
  withClient :: Con -> String ->
                       ReaderDesc i ->
                       WriterDesc o ->
                       (ClientA i o  -> IO r) -> IO r
  withClient c n rd@(rn, _, _, _) wd act =
    withPair c n rd wd $ \(r,w) -> act $ Cl rn r w

  -- the reply queue header ----------------------------------------------
  channel :: String
  channel = "__client__"

  ------------------------------------------------------------------------
  -- | The client will send the request of type /o/
  --   and wait for the reply until the timeout exprires.
  --   The reply is of type /i/ and is returned as 'Message' /i/.
  --   If the timeout expires before the reply has been received,
  --   the function returns 'Nothing'.
  --
  --   Parameters:
  --
  --   * 'ClientA' i o: The client; note that i is the type of the reply,
  --                                          o is the type of the request.
  --
  --   * 'Int': The timeout in microseconds.
  --
  --   * ['F.Header']: List of additional headers 
  --                   to be sent with the request;
  --                   note that the function, internally,
  --                   uses a header named \"__client__\".
  --                   This header name, hence, is reserved
  --                   and must not be used by the application.
  --
  --  * /o/: The request 
  ------------------------------------------------------------------------
  request :: ClientA i o -> 
             Int -> [F.Header] -> o -> IO (Maybe (Message i))
  request c tmo hs r = 
    writeQ (clOut c) ((channel, clChn c) : hs) r >> 
      timeout tmo (readQ (clIn c))

  ------------------------------------------------------------------------
  -- | This function serves as a \"delayed\" receiver for the case
  --   that the timeout of a request has expired.
  --   When using this function, it is assumed
  --   that a request has been made, but no response has been received.
  --   It can be used in time-critical applications,
  --   where the client may use the time between request and reply
  --   productively, instead of blocking on the reply queue.
  --
  --   Use this function with care! It can be easily abused
  --   to break the client/server pattern, when it is called
  --   without a request having been made before.
  --   If, in this case, /timout/ is /-1/,
  --   the application will block for ever.
  --
  --   For parameters, please refer to 'request'.
  ------------------------------------------------------------------------
  checkRequest :: ClientA i o -> Int -> IO (Maybe (Message i))
  checkRequest c tmo = timeout tmo $ readQ (clIn c)

  ------------------------------------------------------------------------
  -- | The server data type
  ------------------------------------------------------------------------
  data ServerA i o = Srv {
                      srvIn  :: Reader i,
                      srvOut :: Writer o}
  
  ------------------------------------------------------------------------
  -- | The function creates a server
  --   that lives within its scope.
  --
  --   Parameters:
  --
  --   * 'Con': Connection to a Stomp broker
  --
  --   * 'String': Name of the Server, used for debugging.
  --
  --   * 'ReaderDesc' i: Description of a reader queue;
  --                     this is the queue through which clients
  --                     are expected to send requests.
  --
  --   * 'WriterDesc' o: Description of a writer queue;
  --                     this is the queue through which
  --                     a specific client will expect the reply.
  --                     Note that the server will overwrite
  --                     the destination of this queue
  --                     using 'writeAdHoc'; 
  --                     the destination of this queue, hence,
  --                     is irrelevant.
  --
  --   * 'ServerA' i o -> IO r: An application-defined action
  --                            whose scope defines the server's lifetime
  ------------------------------------------------------------------------
  withServer :: Con -> String ->
                       ReaderDesc i ->
                       WriterDesc o ->
                       (ServerA i o  -> IO r) -> IO r
  withServer c n rd wd act =
    withPair c n rd wd $ \(r,w) -> act $ Srv r w

  ------------------------------------------------------------------------
  -- | Waits for a client request, 
  --   calls the application-defined transformer to generate a reply
  --   and sends this reply through the queue
  --   whose name is indicated by the value of the \"__client__\" header.
  --   The time a server waits for a request may be restricted
  --   by the timeout. Typically, you would call reply with 
  --   timeout set to /-1/ (for /wait eternally/).
  --   There may be situations, however, where it actually
  --   makes sense to restrict the waiting time,
  --   /i.e./ to perform some housekeeping in between.
  --
  --   Typically, you call reply in a loop like
  --
  --   > forever $ reply srv (-1) nullType [] f
  --
  --   where /f/ is a function of type 
  --
  --   > 'Message' i -> 'IO' o.
  --
  --   Parameters:
  --
  --   * 'ServerA' i o: The server; note that i is the request queue
  --                                     and  o the reply queue.
  --
  --   * 'Int': The timeout in microseconds.
  --
  --   * ['F.Header']: Additional headers to be sent with the reply.
  --
  --   * 'Message' i -> IO o: Transforms the request into a reply -
  --                          this defines the service provided by this
  --                          application.
  ------------------------------------------------------------------------
  reply :: ServerA i o -> Int -> [F.Header] -> 
           (Message i -> IO o) -> IO ()
  reply s tmo hs transform = do
    mbM <- timeout tmo $ readQ (srvIn s)
    case mbM of
      Nothing -> return ()
      Just m  -> 
        case lookup channel $ msgHdrs m of
          Nothing -> throwIO $ 
                       ProtocolException "No reply channel defined!"
          Just c  -> do x <- transform m
                        writeAdHoc (srvOut s) c hs x

