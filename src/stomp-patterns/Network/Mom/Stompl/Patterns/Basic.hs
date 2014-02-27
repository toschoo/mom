-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Stompl/Patterns/Basic.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- This module provides the /basic/ patterns
-- client\/server, publish and subscribe and pusher\/worker
-- (a.k.a. pipeline) as well as a /registry/,
-- a means to support patterns
-- where one application uses or serves a set of other applications. 
-- 
-- Basic patterns can be distinguished by the data exchange
-- defined by their protocol:
--
--  * client\/server: The client, first, sends data to the server
--                    (the request) and, then, 
--                    the server sends data to this client
--                    (the reply);
--
--  * publish and subscribe: The publisher sends data to all subscribers
--                           (the topic);
--
--  * pusher\/worker: The pusher sends data to the worker (the request)
--                   without receiving a reply.
--
--  We call the processing performed by an application on behalf of 
--  another application a /job/.
--  There are three different job types:
--
--  * service: must be requested explicitly and includes a message
--             sent from the application that perfoms the service (server)
--             to the application that requests the service (client).
--
--  * task: must be requested explicitly, but does not include a reply.
--
--  * topic: is sent to registered subscribers without being requested
--           explicitly.
--
--  Applications providing a job are generically called providers, 
--  applications requesting a job are called consumers.
--  Providers, hence, are servers, workers and publishers, and
--  consumers are clients, pushers and subscribers.
--  Note that this is somewhat different from the 
--  data-centric terminology \"producer\" and \"consumer\".
--  It is not very useful
--  to distinugish servers from clients or
--  pushers from workers by referring to the distinction
--  of producing or not producing data.
--  It is in fact the pusher that produces data,
--  not the worker. The pusher, however, is the one
--  that requests something from the worker. The task, in this case,
--  is the \"good\" that is provided by one side and consumed
--  by the other.
--
--  This distinction is relevant when we start to think
--  about /reliability/.
--  Reliability is a relation between a provider and a consumer:
--  The consumer relies on the producer,
--  not the other way round, /e.g./
--  a pusher relies on a woker
--  and a client on a server to get the job done.
--
--  The interfaces in this library 
--  give some guarantees related to reliability, 
--  but there are also some pitfalls:
--  
--  * A client using timeouts can be sure that 
--      the requested service has been performed
--      when the reply arrives before the timeout expires.
--      If no reply arrives before timeout expiration,
--      no such claim can be made (in particular not
--      that the service has not been performed).
--      The service may have been performed, but 
--      it may have taken more time than expected or
--      it may have been performed, 
--      but the reply message has failed to arrive.
--      If the service is /idempotent/ -
--      /i.e./ calling the service twice has 
--      the same effect as calling it once -
--      the client, when the timeout has expired,
--      can just send the request once again;
--      otherwise, it has to use other means to recover 
--      from this situation.
--
--  * A pusher will never know if the task has been performed
--      correctly, since there is no response from the worker.
--      This is one of the reasons that the pipeline pattern 
--      should usually not be used alone, but in the context of
--      a balancer. (You usually want to push 
--      a request to a worker through a balancer --
--      one of the ideas behind pusher/woker is work balancing.)
--      A balancer may request providers to send /heartbeats/
--      and, this way, minimise the risk of failure.
--      The worker still may fail between a heartbeat
--      and a request and even the fact that it does send hearbeats
--      does not necessarily mean that it is operating correctly.
--      If it is essential for the client to know
--      that all tasks have been performed correctly,
--      other verification means are required.
--      
--  * Finally, a subscriber will never know
--             whether a publisher is still working correctly
--             or not, if the publisher does not send data
--             periodically. A reliable design
--             would use periodic publishers, /i.e./
--             publishers that send data at a constant rate,
--             even if no new data are available.
--             The data update, in this case,
--             would have the effect of a heartbeat.
--
--  The library uses a set of headers that must not be used by applications.
--  All internal header keys start and end with two underscores.
--  By avoiding this naming of header keys, application code easily avoids
--  naming conflicts. The headers used in basic patterns are:
--
--  * __channel__: Reply queue (client\/server)
--
--  * __job__: The requested /job/ (client\/server, pusher\/worker and registry)
--
--  * __type__: Request type (registry)
--
--  * __job-type__: Type of job (registry)
--
--  * __queue__: Queue to register (registry)
--
--  * __hb__: Heartbeat specification (registry)
--
--  * __sc__: Status Code (registry) 
-------------------------------------------------------------------------------
module Network.Mom.Stompl.Patterns.Basic (
                          -- * Client
                          ClientA, clName, withClient, request, checkRequest,

                          -- * Server
                          ServerA, srvName, withServer, reply,

                          -- * Registry
                          -- $registry_intro

                          register, unRegister, 
                          heartbeat, HB, mkHB,

                          -- $registry_howto

                          -- * withServerThread
                          withServerThread, RegistryDesc,

                          -- * Pusher
                          PusherA, pushName, withPusher, push,

                          -- * Worker
                          withTaskThread,

                          -- * More about Registries
                          -- $registry_core

                          Registry, withRegistry, 
                          mapR, getProvider, showRegistry,
                          Provider, prvQ, JobType(..),

                          -- $registry_usage

                          -- * Publisher
                          PubA, pubName, withPub, publish, withPubThread,

                          -- * Subscriber
                          SubA, subName, withSub, checkIssue,
                          withSubThread, withSubMVar,

                          -- * Heartbeats for Pub
                          withPubProxy,

                          -- * Exceptions and Error Handling
                          PatternsException(..), OnError,
                          StatusCode(..), readStatusCode,

                          -- * Useful Types and Helpers
                          JobName, QName,
                          nobody, ignorebody,
                          bytesOut, bytesIn,
                          getJobName, getJobType, getQueue, getChannel, 
                          getHB,
                          getSC,
                          getHeader)
where

  import           Registry
  import           Types

  import           Network.Mom.Stompl.Client.Queue 
  import qualified Network.Mom.Stompl.Frame as F
  import           System.Timeout
  import           Codec.MIME.Type (Type)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, catches, finally)
  import           Control.Concurrent 
  import           Control.Monad (forever, unless, when, void)
  import           Data.Time
  import qualified Data.ByteString.Char8 as B

  {- $registry_intro
      Before we continue the survey of basic patterns,
      we have to introduce registries.
      Registries are used by some patterns, advanced patterns,
      but also publishers,
      to use a set of providers that register beforehand
      or, in the case of publishers, 
      to serve a set of consumers that have registered to
      the publisher.
      A typical example is balancers (/majordomo pattern/):
      Servers or tasks register to a balancer, which 
      on receiving a request from a client for a certain job,
      forwards the request to one of the registered providers
      of this job. Internally, the register balances the request,
      such that, with more than one provider currently registered,
      two consecutive requests will not be served
      by the same provider.
      Note that registers provide different modes of using providers:
      a load balancer will send a request to only one of its providers,
      whereas publishers will send the data they produce to all
      currently registered consumers.
      The difference is defined by the 'JobType' of a given 'Job'.
      Of course, only providers of the same type may register
      for the same job. 

      Registers provide a queue through which services can register;
      patterns using registers would provide 
      another queue through which they receive requests. 
      Registers allow for some level of reliability,
      /i.e./ registers can ensure with certain probability 
             that providers are available at the time,
             when a request is made.
     Therefore, registries may request heartbeats from providers.
     Heartbeats are negotiated on registration.
     Note that registries do not send heartbeats back to providers.
     Providers have to use other strategies to make sure
     that the registry to which they have registered is actually
     available. 
  -}

  {- $registry_howto
     The following example shows
     how to use the registration functions and heartbeats
     together with a server:

     > -- The definition of the variables
     > -- reg, jn, rn, tmo
     > -- is out of the scope of this listing;
     > -- their data type and meaning 
     > -- can be inferred from the context.
     >
     >  withConnection "127.0.0.1" 61613 [] [] $ \c -> 
     >    withServer c "Test" (q,            [], [], iconv)
     >                        ("unknown",    [], [], oconv) $ \s -> do
     >      (sc,me) <- if null reg -- if parameter reg is null
     >                   then return (OK, 0)
     >                   else register c jn Service reg rn tmo 500
     >      case sc of
     >        -- ok ------------------------------
     >        OK -> 
     >          if me < 0 || me > 5000 -- accept heartbeat  from 
     >                                 -- 0 (no heartbeats) to
     >                                 -- 5 seconds
     >            then do void $ unRegister c jn wn rn tmo
     >                    throwIO $ UnacceptableHbX me
     >            else do hb <- mkHB me
     >                    m  <- newMVar hb 
     >                    let p = if me <= 0 then (-1) else 1000 * me 
     >                    withWriter c "HB" reg [] [] nobody $ \w -> 
     >                      finally (forever $
     >                        reply s p t hs transform >> heartbeat m w jn rn) (do
     >                        -- "don't forget to unregister!" (Frank Zappa)
     >                        sc <- unRegister c jn wn rn tmo
     >                              unless (sc == OK) $ 
     >                                throwIO $ NotOKX sc "on unregister")
     >        -- not ok ---------------------------
     >        e -> throwIO $ NotOKX e "on register"

     There is, however, a function that does all of this internally: 
     'withServerThread'.
  -}

  {- $registry_core
     Until now, we have only looked at how to connect to a registry,
     not at how to use it and what a registry actually is in terms of
     data types.
     Well, answering the second question is simple:
     a registry, from the perspective of the user application,
     is an opaque data type with a set of functions:
  -}

  {- $registry_usage
     A typical example of how to use a registry in practice 
     is the balancer pattern, which is shown (without error handling)
     below:

     > -- The definition of the variables
     > -- c, n qn, mn, mx, onErr, rq
     > -- is out of the scope of this listing;
     > -- their data type and meaning 
     > -- can be inferred from the context.
     >
     > withRegistry c n qn (mn, mx) onErr $ \reg ->
     >   withPair c n (rq,        [], [], bytesIn) 
     >                ("unknown", [], [], bytesOut) $ \(r,w) -> 
     >     forever $ do
     >       m  <- readQ r        -- receive a request
     >       jn <- getJobName m   -- get the job name from the request 
     >       t  <- mapR reg jn (send2Prov w m)   -- apply job
     >       unless t $ throwIO $ NoProviderX jn -- throw exception
     >                                           -- when job is not provided
     > where send2Prov w m p = writeAdHoc w (prvQ p) nullType 
     >                                      (msgHdrs m) $ msgContent m

     User applications, usually, do not need to use registries directly.
     Registries are used in patterns, namely in Desks, Balancers
     and in /Pub/s.
  -}

  ------------------------------------------------------------------------
  -- | The client data type, which implements the client side
  --   of the client\/server protocol.
  ------------------------------------------------------------------------
  data ClientA i o = Cl {
                      -- | Access to the client name
                      clName :: String,
                      clChn  :: QName,
                      clJob  :: JobName,
                      clIn   :: Reader i,
                      clOut  :: Writer o}
  
  ------------------------------------------------------------------------
  -- | The function creates a client that lives within its scope.
  --
  --   Parameters:
  --
  --   * 'Con': Connection to a Stomp broker
  --
  --   * 'String': Name of the Client, which can be used for error reporting.
  --
  --   * 'JobName': Name of the 'Service' the client will request
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
  withClient :: Con -> String  ->
                       JobName ->
                       ReaderDesc i ->
                       WriterDesc o ->
                       (ClientA i o  -> IO r) -> IO r
  withClient c n jn rd@(rn, _, _, _) wd act =
    withPair c n rd wd $ \(r,w) -> act $ Cl n rn jn r w

  ------------------------------------------------------------------------
  -- | The client will send the request of type /o/
  --   and wait for the reply until the timeout exprires.
  --   The reply is of type /i/ and is returned as 'Message' /i/.
  --   If the timeout expires before the reply has been received,
  --   the function returns 'Nothing'.
  --
  --   Since servers do not know the clients they are serving,
  --   'request' sends the name of its reader queue (the /reply queue/)
  --   as message header to the server.
  --
  --   Parameters:
  --
  --   * 'ClientA' i o: The client; note that i is the type of the reply,
  --                                          o is the type of the request.
  --
  --   * 'Int': The timeout in microseconds.
  --
  --   * 'Type': The /MIME/ type of the request.
  --
  --   * ['F.Header']: List of additional headers 
  --                   to be sent with the request.
  --
  --  * /o/: The request 
  ------------------------------------------------------------------------
  request :: ClientA i o -> 
             Int -> Type -> [F.Header] -> o -> IO (Maybe (Message i))
  request c tmo t hs r = 
    let hs' = [("__channel__", clChn c),
               ("__job__", clJob c)] ++ hs
     in writeQ (clOut c) t hs' r >> timeout tmo (readQ (clIn c))

  ------------------------------------------------------------------------
  -- | This function serves as a \"delayed\" receiver for the case
  --   that the timeout of a request has expired.
  --   When using this function, it is assumed
  --   that a request has been made, but no response has been received.
  --   It can be used in time-critical applications,
  --   where the client may use the time between request and reply
  --   productively, instead of passively blocking on the reply queue.
  --
  --   Use this function with care! It can be easily abused
  --   to break the client\/server pattern, when it is called
  --   without a request having been made before.
  --   If, in this case, /timout/ is /-1/,
  --   the application will block forever.
  --
  --   The function receives those parameters from 'request'
  --   that are related to receiving the reply, /i.e./
  --   'Type', ['F.Header'] and /o/ are not passed to /checkRequest/.
  ------------------------------------------------------------------------
  checkRequest :: ClientA i o -> Int -> IO (Maybe (Message i))
  checkRequest c tmo = timeout tmo $ readQ (clIn c)

  ------------------------------------------------------------------------
  -- | The server data type, which implements the server side
  --   of the client\/server protocol.
  ------------------------------------------------------------------------
  data ServerA i o = Srv {
                      -- | Access to the server name
                      srvName :: String,
                      srvIn   :: Reader i,
                      srvOut  :: Writer o}
  
  ------------------------------------------------------------------------
  -- | The function creates a server
  --   that lives within the scope of the application-defined action
  --   passed into it.
  --
  --   Parameters:
  --
  --   * 'Con': Connection to a Stomp broker
  --
  --   * 'String': Name of the Server, which can be used for error reporting.
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
  withServer :: Con -> String        ->
                       ReaderDesc i  ->
                       WriterDesc o  ->
                       (ServerA i o  -> IO r) -> IO r
  withServer c n rd wd act =
    withPair c n rd wd $ \(r,w) -> act $ Srv n r w

  ------------------------------------------------------------------------
  -- | Waits for a client request, 
  --   calls the application-defined transformer to generate a reply
  --   and sends this reply through the reply queue
  --   whose name is indicated by a header in the request.
  --   The time a server waits for a request may be restricted
  --   by the timeout. Typically, you would call reply with 
  --   timeout set to /-1/ (/wait eternally/).
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
  --   > Message i -> IO o.
  --
  --   Parameters:
  --
  --   * 'ServerA' i o: The server; note that i is the request queue
  --                                     and  o the reply queue.
  --
  --   * 'Int': The timeout in microseconds.
  --
  --   * 'Type': The /MIME/ type of the reply.
  --
  --   * ['F.Header']: Additional headers to be sent with the reply.
  --
  --   * 'Message' i -> IO o: Transforms the request into a reply -
  --                          this defines the service provided by this
  --                          application.
  ------------------------------------------------------------------------
  reply :: ServerA i o -> Int -> Type -> [F.Header] -> 
           (Message i -> IO o) -> IO ()
  reply s tmo t hs transform = do
    mbM <- timeout tmo $ readQ (srvIn s)
    case mbM of
      Nothing -> return ()
      Just m  -> do
        c <- getChannel m
        o <- transform m
        writeAdHoc (srvOut s) c t hs o

  ------------------------------------------------------------------------
  -- | Create a server that works in a background thread:
  --   The background thread (and with it the server)
  --   is running until the action passed in to the function (IO r)
  --   terminates; when it terminates, the background thread is
  --   terminated as well.
  --   /withServerThread/ may connect to a registry
  --   (to serve as a provider of a balancer for instance),
  --   which is automatically handled internally
  --   when a RegistryDesc is passed in with a 'QName'
  --   that is not null. 
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * 'String': The name of the server, used for error reporting;
  --
  --   * 'JobName': The job provided by this server
  --
  --   * 'Type': The MIME Type (passed to 'reply')
  --
  --   * ['F.Header']: Additional headers (passed to 'reply')
  --
  --   * 'Message' i -> IO o: The core of the reply function:
  --                          transforming a request of type /i/
  --                          into a reply of type /o/
  --
  --   * 'ReaderDesc' i: The reader through which requests are expected;
  -- 
  --   * 'WriterDesc' o: The writer through which replies are sent;
  --
  --   * 'RegistryDesc': Describes whether and how to connect to a registry:
  --                     if the queue name of the registry description 
  --                     is null,
  --                     the function will not connect to a registry;
  --                     otherwise it will connect to the registry
  --                     proposing the best value of the 'RegistryDesc'
  --                     as its preferred heartbeat rate;
  --                     should the heartbeat rate returned by the registry
  --                     be outside the scope of min and max,
  --                     /withServerThread/ will terminate 
  --                     with 'UnacceptableHbX'.
  --
  --   * 'OnError': Error handler
  --
  --   * IO r: The function starts a new thread on which the 
  --           the server is working; 
  --           the thread from which the function was called
  --           continues in this action.
  --           Its return value is also the result of /withServerThread/.
  --           When the action terminates,
  --           the new thread is terminated internally.
  ------------------------------------------------------------------------
  withServerThread :: Con  -> String     -> JobName ->
                      Type -> [F.Header] -> (Message i -> IO o) ->
                      ReaderDesc i -> 
                      WriterDesc o ->
                      RegistryDesc -> 
                      OnError      -> IO r -> IO r
  withServerThread c n jn t hs transform
                     rd@(rn, _, _, _)
                     wd
                     (reg, tmo, (best, mn, mx))
                     onErr action =
    withServer c n rd wd $ \s -> do
      (sc,me) <- if null reg 
                   then return (OK, 0)
                   else register c jn Service reg rn tmo best
      case sc of
        OK -> 
          if me < mn || me > mx
            then do finalise c jn reg rn tmo
                    throwIO $ UnacceptableHbX me

            else do hb <- mkHB me
                    m  <- newMVar hb 
                    let p = if me <= 0 then (-1) else 1000 * me 
                    withThread (finally (srv m p s) 
                                        (finalise c jn reg rn tmo)) action
        e -> throwIO $ NotOKX e "on register"
      where srv m p s = withWriter c "HB" reg [] [] nobody $ \w -> 
                          forever $ catches (
                            reply s p t hs transform >> heartbeat m w jn rn) (
                           ignoreHandler (srvName s) onErr)

  ------------------------------------------------------------------------
  -- Finaliser for the registry
  ------------------------------------------------------------------------
  finalise :: Con -> JobName -> QName -> QName -> Int -> IO ()
  finalise c jn wn rn tmo | null wn   = return ()
                          | otherwise = do 
                               sc <- unRegister c jn wn rn tmo
                               unless (sc == OK) $ 
                                     throwIO $ NotOKX sc "on unregister"

  ------------------------------------------------------------------------
  -- | The publisher data type
  ------------------------------------------------------------------------
  data PubA o = Pub {
                  -- | Access to the name of the publisher
                  pubName :: String,
                  pubJob  :: JobName,
                  pubReg  :: Registry,
                  pubConv :: OutBound o,
                  pubOut  :: Writer B.ByteString}

  ------------------------------------------------------------------------
  -- | Create a publisher with the lifetime of the scope
  --   of the user action passed in.
  --   The publisher, internally, creates a registry
  --   to which subscribers will connect to obtain the topic data.
  --   The registry will not expect heartbeats from subscribers,
  --   since the dependability relation is the other way round:
  --   the publisher does not depend on subscribers,
  --   but subscribers depend on a publisher.
  --   The publisher, usually, does not send heartbeats either.
  --   For exceptions to this rule, see 'withPubProxy'.
  --
  --   * 'Con': Connect to a Stomp broker;
  --
  --   * String: Name of the publisher used for error reporting;
  --
  --   * 'JobName': The name of the topic;
  --
  --   * 'QName': Name of the registration queue (see 'withRegistry');
  --
  --   * 'OnError': Error Handler passed to the registry;
  --
  --   * 'WriterDesc': Queue through which data are published;
  --                   note that the queue name is irrelevant.
  --                   The publisher will send data to the queues
  --                   of registered subscribers (see 'mapR');
  --
  --   * 'PubA' -> IO r: Action that defines the lifetime
  --                     of the publisher; the result (/r/)
  --                     is also the result of /withPub/.
  ------------------------------------------------------------------------
  withPub :: Con -> String -> JobName -> QName -> OnError -> 
             WriterDesc o  -> (PubA o -> IO r) -> IO r
  withPub c n jn rn onErr (_, wos, wh, oconv) act = 
    withRegistry c  n rn (0,0) onErr $ \r ->
      withWriter c jn "unknown" wos wh bytesOut $ \w -> 
        act $ Pub n jn r oconv w

  ------------------------------------------------------------------------
  -- | Publish data of type /o/:
  --
  --   * 'PubA' o: Publisher to use;
  --
  --   * 'Type': MIME Type of the message to be sent;
  --
  --   * ['F.Header']: Additional headers to be sent with the message;
  --
  --   * /o/: The message content.
  ------------------------------------------------------------------------
  publish :: PubA o -> Type -> [F.Header] -> o -> IO ()
  publish p t hs x = let oc = pubConv p
                      in oc x >>= \m ->
                         void $ mapR  (pubReg p) (pubJob p) $ \prv -> 
                           writeAdHoc (pubOut p) (prvQ prv) t hs m

  ------------------------------------------------------------------------
  -- | Create a publisher that works in a background thread
  --   publishing periodically at a monotonic rate,
  --   /i.e./ it creates data and publishes them,
  --          computes the difference 
  --             of the publication rate minus the time needed
  --                to create and publish the data 
  --          and will then suspend the thread for this period.
  --          For a publication rate of /p/ microseconds,
  --              the thread will be delayed for /p - x/ microseconds,
  --              if /x/ corresponds to the time that was spent
  --                 on creating and publishing the data.
  --
  --  The precision depends of course on your system and
  --  its current workload.
  --  For most cases, this will be equal to just suspending the thread
  --  for the publication rate.
  --
  --  Parameters:
  --
  --  * 'Con': Connection to a Stomp broker;
  --
  --  * String: Name of the publisher used for error reporting;
  --
  --  * 'JobName': Name of the topic;
  --
  --  * 'QName': Registration queue;
  --
  --  * Type: MIME Type of the published message;
  --
  --  * ['F.Header']: Additional headers to be sent
  --                  with the message;
  --
  --  * IO o: Action to create the message content;
  --
  --  * 'WriterDesc' o: Queue through which the message
  --                    will be published (remember, however,
  --                    that the queue name is irrelevant);
  --
  --  * Int: Publication rate in microseconds;
  --
  --  * 'OnError': Error handler for the registry
  --               and the publisher;
  --
  --  * IO r: Action that defines the lifetime of the publisher;
  --          The result /r/ is also the result of /withPubThread/.   
  ------------------------------------------------------------------------
  withPubThread :: Con -> String -> JobName -> QName ->
                   Type -> [F.Header] -> IO o ->
                   WriterDesc o       -> Int  -> 
                   OnError -> IO r    -> IO r
  withPubThread c n jn rn t hs create wd period onErr action = 
    withPub c n jn rn onErr wd $ \p -> withThread (doPub p) action
    where doPub p = forever $ catches (do
                      n1 <- getCurrentTime
                      create >>= publish p t hs
                      n2 <- getCurrentTime
                      let d = nominal2us (n2 `diffUTCTime` n1)
                      when (d < period) $ threadDelay (period - d)) (
                    ignoreHandler (pubName p) onErr)

  ------------------------------------------------------------------------
  -- | Subscriber data type
  ------------------------------------------------------------------------
  data SubA i = Sub {
                 -- | Access to the subscriber name
                 subName :: String,
                 subIn   :: Reader i
                }

  ------------------------------------------------------------------------
  -- | Create a subscriber with the lifetime 
  --   of the user action passed in.
  --   The subscriber will internally connect to a publisher's
  --   registry and receive data as long as it stays connected.
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * String: Subscriber name useful for error reporting;
  --
  --   * 'JobName': Subscribed topic;
  --
  --   * 'QName': Queue of a registry to connect to
  --              (the 'Pub's registration queue!)
  --
  --   * Int: Registration timeout in microseconds;
  --
  --   * 'ReaderDesc': This is the queue through which
  --                   the subscriber will receive data.
  --
  --   * 'SubA' i -> IO r: Action that defines the lifetime
  --                       of the subscriber. Its result /r/
  --                       is also the result of /withSub/.
  ------------------------------------------------------------------------
  withSub :: Con -> String -> JobName -> QName -> Int ->
             ReaderDesc i  -> (SubA i -> IO r) -> IO r
  withSub c n jn wn tmo (rn, ros, rh, iconv) act = 
    withReader c n rn ros rh iconv $ \r -> do
      mbR <- if null wn 
               then return $ Just (OK,0)
               else timeout tmo $ register c jn Topic wn rn tmo 0
      case mbR of
        Nothing     -> throwIO $ TimeoutX "on register"
        Just (OK,_) -> finally (act $ Sub n r) 
                               (finalise c jn wn rn tmo)
        Just (sc,_) -> throwIO $ NotOKX sc "on register "

  ------------------------------------------------------------------------
  -- | Check if data have been arrived for this subscriber;
  --   if data are available before the timeout expires,
  --   the function results in 'Just' ('Message' i);
  --   if the timeout expires first, the result is 'Nothing'.
  --
  --   * 'SubA' i: The subscriber to check 
  --
  --   * Int: Timeout in microseconds
  ------------------------------------------------------------------------
  checkIssue :: SubA i -> Int -> IO (Maybe (Message i))
  checkIssue s tmo = timeout tmo $ readQ (subIn s)

  ------------------------------------------------------------------------
  -- | Create a subscriber that works in a background thread;
  --   Whenever data are available, an application callback passed in
  --   to the function is called with the message that has arrived.
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * String: Subscriber name used for error reporting;
  --
  --   * 'JobName': Subscribed topic;
  --
  --   * 'QName': The publisher's registration queue;
  --
  --   * Int: Registration timeout in microseconds;
  --
  --   * 'ReaderDesc' i: Queue through which the subscriber
  --                     shall receive data;
  --
  --   * 'Message' i -> IO (): Application callback;
  --
  --   * 'OnError': Error handler; 
  --
  --   * IO r: Action that defines the lifetime of the subscriber;
  --           the result /r/ is also the result of /withSubThread/.
  ------------------------------------------------------------------------
  withSubThread :: Con -> String -> JobName    -> QName  -> Int     ->
                   ReaderDesc i  -> (Message i -> IO ()) -> OnError -> 
                   IO r -> IO r
  withSubThread c n jn wn tmo rd job onErr action = 
     withSub c n jn wn tmo rd $ \s -> withThread (go s) action
    where go s = forever $ catches (chk s >>= job)
                                   (ignoreHandler (subName s) onErr)
          chk s = checkIssue s (-1) >>= \mbX ->
                  case mbX of
                    Nothing -> chk s
                    Just m  -> return m

  ------------------------------------------------------------------------
  -- | Create a subscriber that works in a background thread 
  --   and updates an MVar, whenever new data are available;
  --   the function is in fact a special case of 'withSubThread',
  --   where the application callback updates an MVar.
  --   Note that the MVar must not be empty when the function
  --   is called, otherwise, it will block on modifying the MVar.
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * String: Subscriber name used for error reporting;
  --
  --   * 'JobName': Subscribed topic;
  --
  --   * 'QName': The publisher's registration queue;
  --
  --   * Int: Registration timeout in microseconds;
  --
  --   * 'ReaderDesc' i: Queue through which the subscriber
  --                     shall receive data;
  --
  --   * 'MVar' i: MVar to update;
  --
  --   * 'OnError': Error handler; 
  --
  --   * IO r: Action that defines the lifetime of the subscriber;
  --           the result /r/ is also the result of /withSubMVar/.
  ------------------------------------------------------------------------
  withSubMVar :: Con -> String -> JobName -> QName   -> Int ->
                 ReaderDesc i  -> MVar i  -> OnError -> 
                 IO r -> IO r
  withSubMVar c n jn wn tmo rd v = 
    withSubThread c n jn wn tmo rd job 
    where job m = modifyMVar_ v $ \_ -> return $ msgContent m 

  ------------------------------------------------------------------------
  -- | The Pusher data type, which implements
  --   the consumer side of the pipeline protocol.
  --   Note that, when we say "consumer" here,
  --   the pusher is actually a data producer,
  --   but consumes the effect of having a task done.
  --   The pusher can be seen as a client
  --   that does not expect a reply.
  ------------------------------------------------------------------------
  data PusherA o = Pusher {
                    -- | Access to the pusher's name
                    pushName :: String,
                    pushJob  :: JobName,
                    pushQ    :: Writer o
                  }

  ------------------------------------------------------------------------
  -- | Create a 'Pusher' with the lifetime of the action passed in:
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * String: Name of the pusher, which may be used for error reporting;
  --
  --   * 'JobName': Name of the job requested by this pusher;
  --
  --   * 'WriterDesc' o: 'Writer' queue through which 
  --                              the job request is pushed;
  --
  --   * ('PusherA' o -> IO r): Action that defines the lifetime of
  --                            the pusher; the result /r/ is also
  --                            the result of 'withPusher'.
  ------------------------------------------------------------------------
  withPusher :: Con -> String -> JobName -> WriterDesc o -> 
                (PusherA o -> IO r) -> IO r
  withPusher c n jn (wq, wos, wh, oconv) action = 
    withWriter c n wq wos wh oconv $ \w -> action $ Pusher n jn w

  ------------------------------------------------------------------------
  -- | Push a 'Job':
  --
  --     * 'PusherA' o: The pusher to be used;
  --
  --     * 'Type': The MIME Type of the message to be sent;
  --
  --     * ['F.Header']: The headers to be sent with the message;
  --
  --     * /o/: The message contents.
  ------------------------------------------------------------------------
  push :: PusherA o -> Type -> [F.Header] -> o -> IO ()
  push p t hs m = let hs' = ("__job__", pushJob p) : hs
                   in writeQ (pushQ p) t hs' m

  ------------------------------------------------------------------------
  -- | On the other side of the pipeline,
  --   there sits a worker waiting for requests.
  --   Note that no /Worker/ data type is defined.
  --   Instead, there is only a /withTaskThread/ function
  --   that, internally, creates a worker acting in a background thread.
  --   The rationale is that it does not make too much sense
  --   to have a pipeline with only one worker. 
  --   It is in fact part of the idea of the pipeline pattern 
  --   that several workers are used through a balancer.
  --   /withTaskThread/ implements the interaction with the registry
  --   internally and frees the programmer from concerns related
  --   to registration. If you really need a single worker,
  --   you can call the function with an empty RegistryDesc, 
  --   /i.e./ with an empty queue name.
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * String: Name of the worker used for error reporting;
  --
  --   * 'JobName': Name of the job, the worker provides;
  --
  --   * ('Message' i  -> IO ()): The job provided by the worker.
  --                              Note that the function does not
  --                              return a value: Since workers do
  --                              not produce a reply, no result
  --                              is necessary;
  --
  --   * 'ReaderDesc' i: Queue through which the worker receives
  --                     requests;
  --
  --   * 'RegistryDesc': The registry to which the worker connects;
  --
  --   * OnError: Error handler;
  --
  --   * IO r: Action that defines the worker's lifetime.
  --
  ------------------------------------------------------------------------
  withTaskThread :: Con -> String -> JobName     ->
                    (Message i -> IO ())         -> 
                    ReaderDesc i -> RegistryDesc -> 
                    OnError      -> IO r         -> IO r
  withTaskThread c n jn task
                     (rn, ros, rh, iconv)
                     (reg, tmo, (best, mn, mx))
                     onErr action = do
      (sc,me) <- if null reg
                   then return (OK,0)
                   else register c jn Task reg rn tmo best
      case sc of
        OK -> 
          if me < mn || me > mx
            then do finalise c jn reg rn tmo
                    throwIO $ UnacceptableHbX me

            else do hb  <- mkHB me
                    m   <- newMVar hb 
                    let p = if me <= 0 then (-1) else 1000 * me 
                    withReader c n rn ros rh iconv $ \r -> 
                      withThread (finally (tsk m r p) 
                                          (finalise c jn reg rn tmo)) action
        e -> throwIO $ NotOKX e "on register"
      where tsk m r p = withWriter   c "HB" reg [] [] nobody $ \w -> 
                          forever $ catches (do
                            mbM <- timeout p $ readQ r 
                            case mbM of
                              Nothing -> return ()
                              Just x  -> task x 
                            heartbeat m w jn rn)
                           (ignoreHandler n onErr)

  ------------------------------------------------------------------------
  -- | Unlike servers and workers,
  --   publishers have no interface to connect 
  --   internally to a registry.
  --   The rationale for this is that
  --   publishers do not need load balancers or similar means
  --   that would require registration.
  --   As a consequence, there is no means to send heartbeats internally.
  --   Sometimes, however, the need to connect to a registry may arise.
  --   The Desk pattern is an example where it makes sense 
  --   to register a publisher.
  --   But then, there is no means to internally send heartbeats
  --   proving that the publisher is still alive.
  --   For this case, a simple solution 
  --   for periodic publishers is available:
  --   a heartbeat proxy that is implemented as a subscriber
  --   receiving data from the publisher and 
  --   sending a heartbeat on every dataset that arrives.
  --   
  --   This function provides a proxy that internally
  --   connects to a registry on behalf of a publisher
  --   and sends heartbeats.
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * String: Name of the proxy used for error reporting;
  --
  --   * 'JobName': Name of the topic, the publisher provides;
  --
  --   * 'QName': Registration queue of the publisher -
  --              this is the queue 
  --              to which the internal subscriber connects;
  --
  --   * 'ReaderDesc' i: The queue through which the internal
  --                     subscriber receives data;
  --
  --   * 'RegistryDesc': The other registry - 
  --                     it is this registry to which the 
  --                     proxy will send heartbeats;
  --
  --   * 'OnError': Error Handler;
  --
  --   * IO r: Action that definex the proxy's lifetime;
  --           its result /r/ is also the result of /withPubProxy/.
  ------------------------------------------------------------------------
  withPubProxy :: Con -> String -> JobName      -> QName   ->
                  ReaderDesc i  -> RegistryDesc -> OnError -> IO r -> IO r
  withPubProxy c n jn pq rd (reg, tmo, (best, mn, mx)) onErr action =
    withSub c n jn pq tmo rd $ \s -> 
      withWriter c "HB" reg [] [] nobody $ \w -> do
        (sc, h) <- register c jn Topic reg pq tmo best
        if sc /= OK
          then throwIO $ NotOKX sc "on register proxy"
          else if h < mn || h > mx
                 then throwIO $ UnacceptableHbX h
                 else do hb <- mkHB h >>= newMVar
                         withThread (finally (beat hb (h * 1000) s w 0)
                                             (finalise c jn reg pq tmo)) 
                                    action
    where beat :: MVar HB -> Int -> SubA i -> Writer () -> Int -> IO ()
          beat hb h s w i = forever $ do -- forever continues 
                                         -- in case of exception
            mbM  <- checkIssue s h
            case mbM of
              Nothing -> if i == 10 
                           then throwIO $ MissingHbX "No input from pub"
                           else beat hb h s w (i+1)
              Just _  -> heartbeat hb w jn pq >> beat hb h s w 0 
            `catches` (ignoreHandler n onErr)

