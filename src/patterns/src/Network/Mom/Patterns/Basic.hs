-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Basic.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
-- 
-- Basic communication patterns
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Basic (
          -- * Server/Client
          withServer,
          Client, clientContext, setClientOptions, withClient,
          request, askFor, checkFor,
          -- * Publish/Subscribe
          Pub, pubContext, setPubOptions, withPub, issue,
          withPeriodicPub,
          withSub,
          Sub, subContext, setSubOptions,
          withSporadicSub, checkSub, waitSub, unsubscribe, resubscribe,
          -- * Pipeline
          Pipe, pipeContext, setPipeOptions, withPipe, push, 
          withPuller,
          -- * Exclusive Pair
          Peer, peerContext, setPeerOptions, withPeer, send, receive,
          -- * Service Access Point
          AccessPoint(..), LinkType(..), parseLink,
          -- * Converters
          InBound, OutBound,
          idIn, idOut, inString, outString, inUTF8, outUTF8,
          -- * Errors and Error Handlers
          Criticality(..),
          OnError, OnError_,
          chainIO, chainIOe, tryIO, tryIOe,
          -- * Generic Serivce
          Service, srvName, srvContext, pause, resume, 
          changeParam, changeOption,
          -- * ZMQ Context
          Z.Context, Z.withContext,
          Z.SocketOption(..),
          -- * Helpers
          Topic, alltopics, notopic,
          Timeout, Parameter, noparam)
where

  import           Types
  import           Service
  import           Factory

  import           Network.Mom.Patterns.Device

  import qualified Data.ByteString.Char8  as B
  import qualified Data.Enumerator        as E
  import           Data.Enumerator (($$))

  import           Control.Concurrent 
  import           Control.Applicative ((<$>))
  import           Control.Monad
  import           Prelude hiding (catch)
  import           Control.Exception (SomeException,
                                      catch, try, finally)

  import qualified System.ZMQ as Z

  ------------------------------------------------------------------------
  -- | Starts one or more server threads
  --   and executes an action that
  --   receives a 'Service' to control the server.
  --   The 'Service' is a thread local resource.
  --   It must not be passed to threads forked 
  --   from the thread that has started the service.
  --   The 'Service' is valid only in the scope of the action.
  --   When the action terminates, the server is automatically stopped.
  --   During the action, the server can be paused and restarted.
  --   Also, the 'SocketOption' of the underlying ZMQ 'Z.Socket' 
  --   can be changed.
  --   Please refer to 'pause', 'resume' and 'changeOption' for more details.
  --
  --   The application may implement control parameters.
  --   Control parameters are mere strings that are passed
  --   to the application call-backs. 
  --   It is up to the application to enquire these strings
  --   and to implement different behaviour for the possible settings.
  --   Control parameter can be changed during run-time
  --   by means of 'changeParam'.
  --
  --   Parameters:
  --
  --   * 'Z.Context': The ZMQ context;
  --
  --   * 'String': The name of the server, useful for debugging;
  --
  --   * 'Parameter': The initial value of the control parameter 
  --                  passed to all application call-backs;
  --
  --   * 'Int': The number of worker threads;
  --            note that a server with only one thread
  --            handles client requests sequentially.
  --            The number of threads 
  --            (together with the number of hardware processing resources)
  --            defines how many client requests can be processed in parallel. 
  --
  --   * 'AccessPoint': The access point, 
  --                    through which this server can be reached;
  --
  --   * 'LinkType': The link type; 
  --                 standalone servers usually bind their access point,
  --                 whereas clients connect to it.
  --                 Instead, a server may also connect
  --                 to a load-balancing device,
  --                 to which other servers and clients connect
  --                 (see 'withDevice' and 'withQueue').
  --
  --   * 'InBound': The converter to convert the incoming
  --                data stream (of type 'B.ByteString') 
  --                into a client request component.
  --                Note that the converter converts
  --                single message segments to components of type /c/.
  --                The 'E.Iteratee', receiving this /c/-typed
  --                elements shall combine them 
  --                to a complete request of type /i/,
  --                which is then processed by an 'E.Enumerator'
  --                to create the server response.
  --
  --   * 'OutBound': The converter to convert the results of type /o/
  --                 to a 'B.ByteString', which then is sent
  --                 back to the client.
  --
  --   * 'OnError': The error handler
  --
  --   * 'String' -> 'E.Iteratee': The 'E.Iteratee' that processes
  --                             request components of type /c/
  --                             and yields a request of type /i/.
  --                             The 'String' argument is
  --                             the control parameter,
  --                             whose logic is implemented 
  --                             by the application.
  --
  --   * 'Fetch': The 'E.Enumerator' that processes
  --              the request of type /i/ to produce
  --              results of type /o/.
  --
  --   * 'Service' -> IO (): The action to invoke, 
  --                         when the server has been started; 
  --                         the service is used to control the server.
  --   
  --   The following code fragment shows a 
  --   simple server to process data base queries 
  --   using standard converters and error handlers
  --   not further defined here:
  --
  --   @
  --   withContext 1 $ \\ctx -> do
  --      c <- connectODBC \"DSN=xyz\"  -- some ODBC connection
  --      s <- prepare c \"select ...\" -- some database query 
  --      withServer ctx 
  --          \"MyQuery\" -- name of the server is \"MyQuery\"
  --          noparam     -- no parameter
  --          5           -- five worker threads
  --          (Address \"tcp:\/\/*:5555\" []) Bind -- bind to this address
  --          iconv oconv -- some standard converters
  --          onErr       -- some standard error handler
  --          (\\_  -> one []) -- 'E.Iteratee' for single segment messages;
  --                           -- refer to 'Enumerator' for details
  --          (dbFetcher s) $ \\srv -> -- the 'E.Enumerator';
  --            untilInterrupt $ do -- install a signal handler for /SIGINT/
  --                                -- and repeat the following action
  --                                -- until /SIGINT/ is received;
  --              putStrLn $ \"server \" ++ srvName srv ++ 
  --                         \" up and running...\"
  --              threadDelay 1000000
  --   @
  --
  --   The untilInterrupt loop may be implemented as follows:
  --
  --   @
  --
  --     untilInterrupt :: IO () -> IO ()
  --     untilInterrupt run = do
  --       continue <- newMVar True
  --       _ <- installHandler sigINT (Catch $ handler continue) Nothing
  --       go continue 
  --      where handler m = modifyMVar_ m (\\_ -> return False)
  --            go      m = do run
  --                           continue <- readMVar m
  --                           when continue $ go m
  --   @
  -- 
  --   Finally, a simple dbFetcher:
  --
  --   @
  --     dbFetcher :: SQL.Statement -> Fetch [SQL.SqlValue] String
  --     dbFetcher s _ _ _ stp = tryIO (SQL.execute s []) >>= \\_ -> go stp
  --       where go step = 
  --               case step of
  --                 E.Continue k -> do
  --                   mbR <- tryIO $ SQL.fetchRow s
  --                   case mbR of
  --                     Nothing -> E.continue k
  --                                        -- convRow is not defined here
  --                     Just r  -> go $$ k (E.Chunks [convRow r]) 
  --                 _ -> E.returnI step
  --   @
  ------------------------------------------------------------------------
  withServer :: Z.Context   -> String          -> 
                Parameter   -> Int             ->
                AccessPoint                    -> 
                LinkType                       ->
                InBound c   -> OutBound o      -> 
                OnError                        ->
                (String  -> E.Iteratee c IO i) ->
                Fetch i o                      -> 
                (Service -> IO a)              -> IO a
  withServer ctx name param n ac t iconv oconv onerr build fetch =
    withService ctx name param service 
    where service = serve n ac t iconv oconv onerr 
                          build fetch

  ------------------------------------------------------------------------
  -- the server implementation
  ------------------------------------------------------------------------
  serve :: Int                            ->
           AccessPoint                    ->
           LinkType                       -> 
           InBound c                      ->
           OutBound o                     ->
           OnError                        ->
           (String  -> E.Iteratee c IO i) ->
           Fetch i o                      -> 
           Z.Context -> String -> String  -> String -> IO () -> IO ()
  serve n ac t iconv oconv onerr 
        build fetch ctx name sockname param ready
  ------------------------------------------------------------------------
  -- prepare service for single client
  ------------------------------------------------------------------------
    | n <= 1 = 
      Z.withSocket ctx Z.Rep $ \client -> do
        link t ac client
        Z.withSocket ctx Z.Sub $ \cmd -> do
          conCmd cmd sockname ready
          poll False [Z.S cmd Z.In, Z.S client Z.In] (go client) param
      `catch` (\e -> onerr Fatal e name param >>= \_ -> return ())
  ------------------------------------------------------------------------
  -- prepare service for multiple clients 
  ------------------------------------------------------------------------
    | otherwise = (do
        add <- ("inproc://wrk_" ++) <$> show <$> mkUniqueId
        as  <- replicateM n newEmptyMVar 
        zs  <- replicateM n newEmptyMVar
        withQueue ctx ("Queue " ++ name)
                      (ac, t) (Address add [], Bind) onQErr $ \_ -> do
          _ <- mapM (uncurry $ start add) (zip as zs)
          mapM_ takeMVar as  -- wait for workers to start
          ready              -- report state to service
          mapM_ takeMVar zs) -- wait for workers to terminate
        `catch` (\e -> onerr Fatal e name param >>= \_ -> return ())
  ------------------------------------------------------------------------
  -- start thread
  ------------------------------------------------------------------------
    where start add a z = forkIO (startWork add a `finally` putMVar z ())
  ------------------------------------------------------------------------
  -- start worker for multiple clients 
  ------------------------------------------------------------------------
          startWork add starter = Z.withSocket ctx Z.Rep $ \worker -> (do
            trycon worker add retries
            Z.withSocket ctx Z.Sub $ \cmd -> do
              trycon cmd sockname retries
              Z.subscribe cmd noparam
              putMVar starter ()
              poll False [Z.S cmd Z.In, Z.S worker Z.In] (go worker) param)
            `catch` (\e -> onerr Critical e name param >>= \_ -> return ())
  ------------------------------------------------------------------------
  -- receive requests and do the job
  ------------------------------------------------------------------------
          go worker p = do
              ei <- E.run (rcvEnum worker iconv $$ build p)
              ifLeft ei (\e -> handle worker e p) $ \i ->
                        catch (body worker p i)
                              (\e -> handle worker e p)
          body worker p i = do
               eiR <- E.run (fetch ctx p i $$ itSend worker oconv)
               ifLeft eiR
                 (\e -> handle worker e p)
                 (\_ -> return ())
  ------------------------------------------------------------------------
  -- generic error handler
  ------------------------------------------------------------------------
          handle sock e p = onerr Error e name p >>= \mbX ->
              case mbX of
                Nothing -> 
                  Z.send sock B.empty []
                Just x  -> do 
                  Z.send sock x [Z.SndMore]
                  Z.send sock B.empty []
          onQErr c e nm _ = onerr c e nm noparam >>= \_ -> return ()

  ------------------------------------------------------------------------
  -- | Client data type
  ------------------------------------------------------------------------
  data Client i o = Client {
                         cliCtx  :: Z.Context,
                         cliSock :: Z.Socket Z.Req,
                         cliAdd  :: AccessPoint,
                         cliOut  :: OutBound o,
                         cliIn   :: InBound  i}

  ------------------------------------------------------------------------
  -- | Obtaining the 'Z.Context' from 'Client'
  ------------------------------------------------------------------------
  clientContext :: Client i o -> Z.Context
  clientContext = cliCtx

  ------------------------------------------------------------------------
  -- | Setting 'Z.SocketOption' to the underlying ZMQ 'Z.Socket'
  ------------------------------------------------------------------------
  setClientOptions :: Client i o -> [Z.SocketOption] -> IO ()
  setClientOptions c = setSockOs (cliSock c)

  ------------------------------------------------------------------------
  -- | Creates a 'Client';
  --   a client is not a background process like a server,
  --   but a data type that provides functions
  --   to interoperate with a server.
  --   'withClient' creates a client and 
  --   invokes the application-defined action,
  --   which receives a 'Client' argument.
  --   The lifetime of the 'Client' is limited
  --   to the invoked action.
  --   When the action terminates, the 'Client' /dies/.
  --
  --   Parameters:
  --
  --   * 'Z.Context': The ZMQ Context;
  --
  --   * 'AccessPoint': The access point, to which the client connects;
  --
  --   * 'OutBound': Converter to convert a request from type /o/
  --                 to the wire format 'B.ByteString'.
  --                 Note that, as for servers, the request
  --                 may be composed of components that together
  --                 form the request. The type /o/
  --                 corresponds to one of these request components,
  --                 not necessarily to the request type as a whole,
  --                 which is determined when issuing a request.
  --
  --   * 'InBound': Converter to convert a reply ('B.ByteString')
  --                into type 'i'.
  --                Note again that the reply may consist of many
  --                message segments. The type /i/ relates to one
  --                reply component, 
  --                not necessarily to the reply type as a whole,
  --                which is determined when issuing a request.
  --
  --   * 'Client' -> IO a: The action to perform with this client.
  ------------------------------------------------------------------------
  withClient :: Z.Context  -> AccessPoint -> 
                OutBound o -> InBound i   -> 
                (Client i o -> IO a)      -> IO a
  withClient ctx ac oconv iconv act = Z.withSocket ctx Z.Req $ \s -> do 
    trycon s (acAdd ac) retries
    act Client {
        cliCtx  = ctx,
        cliSock = s,
        cliAdd  = ac,
        cliOut  = oconv,
        cliIn   = iconv}

  ------------------------------------------------------------------------
  -- | Synchronously requesting a service;
  --   the function blocks the current thread,
  --   until a reply is received.
  --   
  --   Parameters:
  --
  --   * 'Client': The client that performs the request
  --
  --   * 'E.Enumerator': Enumerator to create the request message stream
  --
  --   * 'E.Iteratee': Iteratee to process the reply message stream
  --
  --   A simple client that just writes the results to 'stdout':
  --
  --   @
  --     rcv :: String -> IO ()
  --     rcv req = withContext 1 $ \\ctx -> 
  --       withClient ctx 
  --         (Address \"tcp:\/\/localhost:5555\" []) -- connect to this address
  --         (return . B.pack) (return . B.unpack) $ -- string converters
  --         \\s -> do
  --           -- request with enum and outit
  --           ei <- request s (enum req) outit      
  --           case ei of
  --             Left e  -> putStrLn $ \"Error: \" ++ show (e::SomeException)
  --             Right _ -> return ()
  --   @
  --
  --   @
  --     -- Enumerator that returns just one string
  --     enum :: String -> E.Enumerator String IO ()
  --     enum = once (return . Just)
  --   @
  --
  --   @
  --     -- Iteratee that just writes to stdout
  --     outit :: E.Iteratee String IO ()
  --     outit = do
  --       mbi <- EL.head
  --       case mbi of
  --         Nothing -> return ()
  --         Just i  -> liftIO (putStrLn i) >> outit
  --   @
  --
  --   Note that this code just issues one request,
  --   which is not the most typical use case.
  --   It is more likely that the action will loop for ever
  --   and receive requests, for instance, from a user interface.
  ------------------------------------------------------------------------
  request :: Client i o           ->   
             E.Enumerator o IO () ->
             E.Iteratee i IO a    -> IO (Either SomeException a) 
  request c enum it = tryout ?> reicv
    where tryout    = try $ askFor    c enum
          reicv  _  =       rcvClient c it

  ------------------------------------------------------------------------
  -- | Asynchronously requesting a service;
  --   the function sends a request to the server 
  --   without waiting for a result.
  --   
  --   Parameters:
  --
  --   * 'Client': The client that performs the request
  --
  --   * 'E.Enumerator': Enumerator to create the request message stream
  ------------------------------------------------------------------------
  askFor :: Client i o -> E.Enumerator o IO () -> IO ()
  askFor c enum = E.run_ (enum $$ itSend (cliSock c) (cliOut c))

  ------------------------------------------------------------------------
  -- | Polling for a reply;
  --   the function polls for a server request.
  --   If nothing has been received, it returns 'Nothing';
  --   otherwise it returns 'Just' the result or an error.
  --   
  --   Parameters:
  --
  --   * 'Client': The client that performs the request
  --
  --   * 'E.Iteratee': Iteratee to process the reply message stream
  --
  --   The synchronous request (see 'request') 
  --   could be implemented asynchronously like:
  --
  --   @
  --     rcv :: String -> IO ()
  --     rcv req = withContext 1 $ \\ctx -> do
  --       let ap = address l \"tcp:\/\/localhost:5555\" []
  --       withClient ctx ap 
  --         (return . B.pack) (return . B.unpack) 
  --         $ \\s -> do
  --           ei <- try $ askFor s (enum req)
  --           case ei of
  --             Left  e -> putStrLn $ \"Error: \" ++ show (e::SomeException)
  --             Right _ -> wait s
  --       -- check for results periodically 
  --       where wait s = checkFor s outit >>= \\mbei ->
  --               case mbei of
  --                 Nothing        -> do putStrLn \"Waiting...\"
  --                                      threadDelay 10000 >> wait s
  --                 Just (Left e)  -> putStrLn $ \"Error: \" ++ show e
  --                 Just (Right _) -> putStrLn \"Ready!\"
  --   @
  ------------------------------------------------------------------------
  checkFor :: Client i o -> E.Iteratee i IO a -> 
              IO (Maybe (Either SomeException a))
  checkFor c it = Z.poll [Z.S (cliSock c) Z.In] 0 >>= \[s] ->
    case s of
      Z.S _ Z.In -> Just <$> rcvClient c it
      _          -> return Nothing

  ------------------------------------------------------------------------
  -- The real working horse behind the scene
  ------------------------------------------------------------------------
  rcvClient :: Client i o -> E.Iteratee i IO a -> IO (Either SomeException a)
  rcvClient c it = E.run (rcvEnum (cliSock c) (cliIn c) $$ it)

  ------------------------------------------------------------------------
  -- | Publisher
  ------------------------------------------------------------------------
  data Pub o = Pub {
                 pubCtx  :: Z.Context,
                 pubSock :: Z.Socket Z.Pub,
                 pubAdd  :: AccessPoint,
                 pubOut  :: OutBound o}

  ------------------------------------------------------------------------
  -- | Obtaining the 'Z.Context' from 'Pub'
  ------------------------------------------------------------------------
  pubContext :: Pub o -> Z.Context
  pubContext = pubCtx

  ------------------------------------------------------------------------
  -- | Setting 'Z.SocketOption' to the underlying ZMQ 'Z.Socket'
  ------------------------------------------------------------------------
  setPubOptions :: Pub o -> [Z.SocketOption] -> IO ()
  setPubOptions p = setSockOs (pubSock p)

  ------------------------------------------------------------------------
  -- | Creates a publisher;
  --   A publisher is a data type
  --   that provides an interface to publish data to subscribers.
  --   'withPub' creates a publisher and invokes
  --   an application-defined action,
  --   which receives a 'Pub' argument.
  --   The lifetime of the publisher is limited to the action.
  --   When the action terminates, the publisher /dies/.
  --
  --   Parameter:
  --
  --   * 'Z.Context': The ZMQ Context
  --
  --   * 'AccessPoint': The access point the publisher will bind
  --
  --   * 'OutBound': A converter to convert from type /o/ 
  --                 to the wire format 'B.ByteString'.
  --                 Note that a publisher may create
  --                 a data stream; the type /o/ is then
  --                 the type of one segment of this stream,
  --                 not of the stream as a whole.
  --
  --   * 'Pub' -> IO (): The action to invoke
  ------------------------------------------------------------------------
  withPub :: Z.Context -> AccessPoint -> OutBound o -> 
             (Pub o -> IO a) -> IO a
  withPub ctx ac oconv act = Z.withSocket ctx Z.Pub $ \s -> do
    Z.bind s (acAdd ac)
    act Pub {
          pubCtx  = ctx,
          pubSock = s,
          pubAdd  = ac,
          pubOut  = oconv}

  ------------------------------------------------------------------------
  -- | Publishes the data stream created by an enumerator;
  --
  --   Parameters:
  --
  --   * 'Pub': The publisher
  --
  --   * 'E.Enumerator': The enumerator to create an outgoing 
  --                     data stream.
  --
  --   A simple weather report publisher:
  --
  --   @
  --     withContext 1 $ \\ctx -> withPub ctx
  --         (Address \"tcp:\/\/*:5555\" [])
  --         (return . B.pack) $ \\pub -> untilInterrupt $ do
  --           issue pub (once weather noparam)
  --           threadDelay 10000 -- update every 10ms
  --   @
  --
  --   @
  --     -- fake weather report with some random values
  --     weather :: String -> IO (Maybe String)
  --     weather _ = do
  --         zipcode     <- randomRIO (10000, 99999) :: IO Int
  --         temperature <- randomRIO (-10, 30) :: IO Int
  --         humidity    <- randomRIO ( 10, 60) :: IO Int
  --         return $ Just (unwords [show zipcode, 
  --                                 show temperature, 
  --                                 show humidity])
  --   @
  ------------------------------------------------------------------------
  issue :: Pub o -> E.Enumerator o IO () -> IO ()
  issue p enum = E.run_ (enum $$ itSend (pubSock p) (pubOut p))
             
  ------------------------------------------------------------------------
  -- | Creates a background process that
  --   periodically publishes data;
  --
  --   Parameters:
  --
  --   * 'Z.Context': The ZMQ Context
  --
  --   * 'String': Name of this Publisher; 
  --               useful for debugging
  --
  --   * 'Parameter': The initial value of the control parameter
  --
  --   * 'Z.Timeout': The period of the publisher in microseconds;
  --                  the process will issue the publisher data 
  --                  every n microseconds.
  --
  --   * 'AccessPoint': Bind address 
  --
  --   * 'OutBound': A converter that converts one segment
  --                 of the data stream from type /o/
  --                 to the wire format 'B.ByteString'
  --
  --   * 'OnError_': Error Handler
  --
  --   * 'String' -> 'Fetch': 'E.Enumerator' to create
  --                          the outgoing data stream;
  --                          the string argument is the parameter.
  --
  --   * 'Service' -> IO (): The user action to perform
  --
  --   The weather report publisher introduced above (see 'withPub')
  --   can be implemented by means of 'withPeriodicPub' as:
  --
  --   @
  --     withPeriodicPub ctx \"Weather Report\" noparam 
  --       100000 -- publish every 100ms
  --       (Address \"tcp:\/\/*:5555\" []) 
  --       (return . B.pack) -- string converter
  --       onErr_            -- standard error handler
  --       (\\_ -> fetch1 fetch) -- creates one instance
  --                             -- of the return of \"fetch\";
  --                             -- see 'Enumerator' for details
  --       $ \\pub -> 
  --         untilInterrupt $ do -- until /SIGINT/, see 'withServer' for details
  --           threadDelay 100000
  --           putStrLn $ \"I am doing nothing \" ++ srvName pub
  --   @
  ------------------------------------------------------------------------
  withPeriodicPub :: Z.Context           -> 
                     String -> Parameter ->
                     Z.Timeout           ->
                     AccessPoint         -> 
                     OutBound o          ->
                     OnError_            ->
                     Fetch_ o            -> 
                     (Service -> IO a)   -> IO a
  withPeriodicPub ctx name param period ac oconv onerr fetch =
    withService ctx name param service 
    where service = publish period ac oconv onerr fetch

  ------------------------------------------------------------------------
  -- PeriodicPub implementation
  ------------------------------------------------------------------------
  publish :: Z.Timeout            ->
             AccessPoint          -> 
             OutBound o           ->
             OnError_             ->
             Fetch_  o            -> 
             Z.Context -> String  -> 
             String -> String     -> IO () -> IO ()
  publish period ac oconv onerr 
          fetch ctx name sockname param ready = 
    Z.withSocket ctx Z.Pub $ \sock -> do
      Z.bind sock (acAdd ac)
      Z.withSocket ctx Z.Sub $ \cmd -> do
        conCmd cmd sockname ready
        periodicSend False period cmd (go sock) param
    `catch` (\e -> onerr Fatal e name param)
  ------------------------------------------------------------------------
  -- do the job periodically
  ------------------------------------------------------------------------
    where go sock p   = catch (body sock p) 
                              (\e -> onerr Error e name p)
          body sock p = do
            eiR <- E.run (fetch ctx p () $$ itSend sock oconv)
            ifLeft eiR
              (\e -> onerr Error e name p)
              (\_ -> return ())

  ------------------------------------------------------------------------
  -- | A subscription is a background service
  --   that receives and processes data streams
  --   from a publisher.
  --   A typical use case is an application
  --   that operates on periodically updated data;
  --   the subscriber would receive these data and
  --   and make them accessible to other threads in the process
  --   through an 'MVar'.
  --   
  --   Parameters:
  --
  --   * 'Z.Context': The ZMQ Context
  --
  --   * 'String': The subscriber's name 
  --
  --   * 'Parameter': The initial value of the control parameter
  --
  --   * ['Topic']:  The topics to subscribe to;
  --                 in the example above ('withPub'),
  --                 the publisher publishes the weather report
  --                 per zip code; the zip code, in this example,
  --                 could be a meaningful topic for a subscriber.
  --                 It is good practice to send the topic
  --                 in an initial message segment,
  --                 the envelope, to avoid that the subscriber
  --                 matches on some arbitrary part of the message.
  --
  --   * 'InBound': A converter that converts one segment
  --                of the incoming data stream to type /o/
  --
  --   * 'OnError_': Error handler
  --
  --   * 'Dump': 'E.Iteratee' to process the incoming data stream.
  --
  --   * 'Service' -> IO (): Application-defined action to control
  --                         the service. Note that 'Service' is
  --                         a thread-local resource and must not
  --                         be passed to threads forked from the action.
  --
  --  Weather Report Subscriber:
  --  
  --  @
  --     withContext 1 $ \\ctx -> 
  --       withSub ctx \"Weather Report\" noparam 
  --               [\"10001\"] -- zipcode to subscribe to
  --               (Address \"tcp:\/\/localhost:5555\" []) 
  --               (return . B.unpack) 
  --               onErr_ output -- Iteratee that just writes to stdout
  --               $ \\s -> untilInterrupt $ do
  --                 putStrLn $ \"Doing nothing \" ++ srvName s
  --                 threadDelay 1000000
  --  @
  ------------------------------------------------------------------------
  withSub :: Z.Context              -> 
             String                 -> 
             Parameter              -> 
             [Topic]                -> 
             AccessPoint            -> 
             InBound i -> OnError_  ->
             Dump i                 -> 
             (Service -> IO a)      -> IO a
  withSub ctx name param sub ac iconv onErr dump =
    withService ctx name param service 
    where service = subscribe sub ac iconv onErr dump

  subscribe :: [Topic]     -> 
               AccessPoint -> 
               InBound i   -> 
               OnError_    -> 
               Dump i      -> 
               Z.Context   -> 
               String      -> 
               String -> Parameter -> IO () -> IO ()
  subscribe sub ac iconv onerr dump 
            ctx name sockname param ready =
    Z.withSocket ctx Z.Sub $ \sock -> do
      trycon      sock (acAdd ac) retries
      mapM_ (Z.subscribe sock) sub
      Z.withSocket ctx Z.Sub $ \cmd -> do
        conCmd cmd sockname ready
        poll False [Z.S cmd Z.In, Z.S sock Z.In] (go sock) param
    `catch` (\e -> onerr Fatal e name param)
    where go sock p = do
            eiR <- E.run (rcvEnum sock iconv $$ dump ctx p)
            ifLeft eiR
              (\e -> onerr Error e name p)
              (\_ -> return ())

  ------------------------------------------------------------------------
  -- | An alternative to the background subscriber (see 'withSub');
  ------------------------------------------------------------------------
  data Sub i = Sub {
               subCtx   :: Z.Context,
               subSock  :: Z.Socket Z.Sub,
               subAdd   :: AccessPoint,
               subIn    :: InBound i}

  ------------------------------------------------------------------------
  -- | Obtaining the 'Z.Context' from 'Sub'
  ------------------------------------------------------------------------
  subContext :: Sub i -> Z.Context
  subContext = subCtx

  ------------------------------------------------------------------------
  -- | Setting 'Z.SocketOption' to the underlying ZMQ 'Z.Socket'
  ------------------------------------------------------------------------
  setSubOptions :: Sub i -> [Z.SocketOption] -> IO ()
  setSubOptions s = setSockOs (subSock s)

  ------------------------------------------------------------------------
  -- | Similar to 'Pub', a 'Sub' is a data type
  --   that provides an interface to subscribe data.
  --   'withSporadicSub' creates a subscriber and invokes
  --   an application-defined action,
  --   which receives a 'Sub' argument.
  --   The lifetime of the subscriber is limited to the action.
  --   When the action terminates, the subscriber /dies/.
  ------------------------------------------------------------------------
  withSporadicSub :: Z.Context -> AccessPoint -> InBound i -> [Topic] ->
                     (Sub i -> IO a) -> IO a
  withSporadicSub ctx ac iconv topics act = Z.withSocket ctx Z.Sub $ \s -> do
    trycon      s (acAdd ac) retries
    mapM_ (Z.subscribe s) topics
    act Sub {
          subCtx   = ctx,
          subSock  = s,
          subAdd   = ac,
          subIn    = iconv}

  ------------------------------------------------------------------------
  -- | Polling for data;
  --   If nothing has been received, the function returns 'Nothing';
  --   otherwise it returns 'Just' the result or an error.
  --   
  --   Parameters:
  --
  --   * 'Sub': The subscriber
  --
  --   * 'E.Iteratee': Iteratee to process the data
  ------------------------------------------------------------------------
  checkSub :: Sub i -> E.Iteratee i IO a -> 
              IO (Maybe (Either SomeException a))
  checkSub s it = Z.poll [Z.S (subSock s) Z.In] 0 >>= \[p] ->
    case p of
      Z.S _ Z.In -> Just <$> rcvSub s it
      _          -> return Nothing

  ------------------------------------------------------------------------
  -- | Waiting for data;
  --   the function blocks the current thread,
  --   until data are being received from the publisher.
  --   It returns either 'SomeException' or the result.
  --   
  --   Parameters:
  --
  --   * 'Sub': The subscriber
  --
  --   * 'E.Iteratee': Iteratee to process the data stream
  ------------------------------------------------------------------------
  waitSub :: Sub i -> E.Iteratee i IO a -> IO (Either SomeException a)
  waitSub = rcvSub

  ------------------------------------------------------------------------
  -- | Unsubscribe a topic
  ------------------------------------------------------------------------
  unsubscribe :: Sub i -> Topic -> IO ()
  unsubscribe s = Z.unsubscribe (subSock s)

  ------------------------------------------------------------------------
  -- | Subscribe another topic
  ------------------------------------------------------------------------
  resubscribe :: Sub i -> Topic -> IO ()
  resubscribe s = Z.subscribe (subSock s)

  ------------------------------------------------------------------------
  -- The working horse behind the scene
  ------------------------------------------------------------------------
  rcvSub :: Sub i -> E.Iteratee i IO a -> IO (Either SomeException a)
  rcvSub s it = E.run (rcvEnum (subSock s) (subIn s) $$ it)

  ------------------------------------------------------------------------
  -- | A puller is a background service 
  --   that receives and processes data streams from a pipeline.
  --   
  --   Parameters:
  --
  --   * 'Z.Context': The ZMQ Context
  --
  --   * 'String': The service name
  --
  --   * 'Parameter': The initial value of the control parameter
  --
  --   * 'AccessPoint': The address to connect to
  --
  --   * 'InBound': A converter to convert 
  --                segments of the incoming data stream
  --                from the wire format 'B.ByteString'
  --                to the type /i/
  --
  --   * 'OnError_': Error Handler
  --
  --   * 'Dump': 'E.Iteratee' to process
  --              the incoming data stream
  --
  --   * 'Service' -> IO (): Application-defined action
  --
  --   A worker that just writes the incoming stream to /stdout/:
  --
  --   @
  --     withContext 1 $ \\ctx -> 
  --       withPuller ctx \"Worker\" noparam 
  --             (Address \"tcp:\/\/localhost:5555\" [])
  --             (return . B.unpack)
  --             onErr_ output
  --             $ \\s -> untilInterrupt $ do
  --               putStrLn \"Doing nothing \" ++ srvName s
  --               threadDelay 100000
  --   @
  ------------------------------------------------------------------------
  withPuller :: Z.Context           ->
                String -> Parameter ->
                AccessPoint         ->
                InBound i           ->  
                OnError_            ->
                Dump   i            -> 
                (Service -> IO a)   -> IO a
  withPuller ctx name param ac iconv onerr dump =
    withService ctx name param service 
    where service = pull ac iconv onerr dump 

  pull :: AccessPoint          ->
          InBound i            ->
          OnError_             ->
          Dump   i             ->
          Z.Context -> String  -> 
          String    -> String  -> IO () -> IO ()
  pull ac iconv onerr dump ctx name sockname param ready = 
    Z.withSocket ctx Z.Pull $ \sock -> do
      trycon sock (acAdd ac) retries
      Z.withSocket ctx Z.Sub $ \cmd -> do
        conCmd cmd sockname ready
        poll False [Z.S cmd Z.In, Z.S sock Z.In] (go sock) param
    `catch` (\e -> onerr Fatal e name param)
  ------------------------------------------------------------------------
  -- do the job 
  ------------------------------------------------------------------------
    where go sock p = (do putStrLn "Running Worker Job"
                          E.run_  (rcvEnum sock iconv $$ dump ctx p))
                      `catch` (\e -> onerr Error e name p)
 
  ------------------------------------------------------------------------
  -- | A pipeline consists of a \"pusher\" 
  --   and a set of workers (\"pullers\").
  --   The pusher sends jobs down the pipeline that will be
  --   assigned to one of the workers. 
  --   The pipeline pattern is, thus, a work-balancing scheme.
  ------------------------------------------------------------------------
  data Pipe o = Pipe {
                  pipCtx  :: Z.Context,
                  pipSock :: Z.Socket Z.Push,
                  pipAdd  :: AccessPoint,
                  pipOut  :: OutBound o
                }

  ------------------------------------------------------------------------
  -- | Obtaining the 'Z.Context' from 'Pipe'
  ------------------------------------------------------------------------
  pipeContext :: Pipe o -> Z.Context
  pipeContext = pipCtx

  ------------------------------------------------------------------------
  -- | Setting 'Z.SocketOption' to the underlying ZMQ 'Z.Socket'
  ------------------------------------------------------------------------
  setPipeOptions :: Pipe o -> [Z.SocketOption] -> IO ()
  setPipeOptions p = setSockOs (pipSock p)

  ------------------------------------------------------------------------
  -- | Creates a pipeline;
  --   a 'Pipe' is a data type 
  --   that provides an interface to /push/ a data stream
  --   to workers connected to the other side of the pipe.
  --   'withPipe' creates a pipeline and invokes an application-defined
  --   action which receives a 'Pipe' argument.
  --   The lifetime of the 'Pipe' is limited to the action.
  --   When the action terminates, the 'Pipe' /dies/.
  --
  --   Parameters:
  --
  --   * 'Z.Context': The ZMQ Context
  --
  --   * 'AccessPoint': The bind address
  --
  --   * 'OutBound': A converter to convert message segments
  --                 of type /o/ to the wire format 'B.ByteString'
  --
  --   * 'Pipe' -> IO (): The action to invoke
  ------------------------------------------------------------------------
  withPipe :: Z.Context  -> AccessPoint -> 
              OutBound o -> 
              (Pipe o -> IO a)     -> IO a
  withPipe ctx ac oconv act = Z.withSocket ctx Z.Push $ \s -> do 
    Z.bind s (acAdd ac)
    act Pipe {
        pipCtx  = ctx,
        pipSock = s,
        pipAdd  = ac,
        pipOut  = oconv}

  ------------------------------------------------------------------------
  -- | Sends a job down the pipeline;
  --   
  --   Parameters:
  --
  --   * 'Pipe': The pipeline
  --
  --   * 'E.Enumerator': enumerator to create the data stream
  --                     that constitutes the /job/
  --
  --  A simple pusher:
  --  
  --  @
  --    sendF :: FilePath -> IO ()
  --    sendF f = withContext 1 $ \\ctx -> do
  --     let ap = Address \"tcp:\/\/*:5555\" []
  --     withPipe ctx ap return $ \\p ->
  --       push pu (EB.enumFile f) -- file enumerator
  --                               -- see Data.Enumerator.Binary (EB)
  --  @
  ------------------------------------------------------------------------
  push :: Pipe o -> E.Enumerator o IO () -> IO () 
  push p enum = E.run_ (enum $$ itSend (pipSock p) (pipOut p))

  ------------------------------------------------------------------------
  -- | An Exclusive Pair is a general purpose pattern
  --   of two equal peers that communicate with each other
  --   by sending ('send') and receiving ('receive') data.
  --   One of the peers has to 'Z.bind' the 'AccessPoint'
  --   the other 'Z.connect's to it.
  ------------------------------------------------------------------------
  data Peer a = Peer {
                  peeCtx  :: Z.Context,
                  peeSock :: Z.Socket Z.Pair,
                  peeAdd  :: AccessPoint,
                  peeIn   :: InBound a,
                  peeOut  :: OutBound a
                }

  ------------------------------------------------------------------------
  -- | Obtains the 'Z.Context' from a 'Peer'
  ------------------------------------------------------------------------
  peerContext :: Peer a -> Z.Context
  peerContext = peeCtx

  ------------------------------------------------------------------------
  -- | Sets 'Z.SocketOption' 
  ------------------------------------------------------------------------
  setPeerOptions :: Peer a -> [Z.SocketOption] -> IO ()
  setPeerOptions p = setSockOs (peeSock p)

  ------------------------------------------------------------------------
  -- | Creates a 'Peer';
  --   a peer is a data type 
  --   that provides an interface to exchange data with another peer.
  --   'withPeer' creates the peer and invokes an application-defined
  --   action that receives a 'Peer' argument.
  --   The lifetime of the 'Peer' is limited to the action.
  --   When the action terminates, the 'Peer' /dies/.
  --
  --   Parameters:
  --
  --   * 'Z.Context': The ZMQ Context
  --
  --   * 'AccessPoint': The address, to which this peer either
  --                    binds or connects
  --
  --   * 'LinkType': One of the peers has to bind the address,
  --                 the other has to connect.
  --
  --   * 'InBound': A converter to convert message segments
  --                from the wire format 'B.ByteString' to type /i/
  --
  --   * 'OutBound': A converter to convert message segments
  --                 of type /o/ to the wire format 'B.ByteString'
  --
  --   * 'Peer' -> IO (): The action to invoke
  ------------------------------------------------------------------------
  withPeer :: Z.Context -> AccessPoint -> LinkType ->
              InBound a -> OutBound a  ->
              (Peer a -> IO b) -> IO b
  withPeer ctx ac t iconv oconv act = Z.withSocket ctx Z.Pair $ \s -> 
    link t ac s >> act Peer {
                         peeCtx  = ctx,
                         peeSock = s,
                         peeAdd  = ac,
                         peeIn   = iconv,
                         peeOut  = oconv}

  ------------------------------------------------------------------------
  -- | Sends a data stream to another peer;
  --
  --   Parameters:
  --  
  --   * 'Peer': The peer
  --
  --   * 'E.Enumerator': Enumerator to create the outoing data stream
  ------------------------------------------------------------------------
  send :: Peer o -> E.Enumerator o IO () -> IO ()
  send p enum = E.run_ (enum $$ itSend (peeSock p) (peeOut p))

  ------------------------------------------------------------------------
  -- | Receives a data stream from another peer;
  --
  --   Parameters:
  --  
  --   * 'Peer': The peer
  --
  --   * 'E.Iteratee': Iteratee to process the incoming data stream
  ------------------------------------------------------------------------
  receive :: Peer i -> E.Iteratee i IO a -> IO (Either SomeException a)
  receive p it = E.run (rcvEnum (peeSock p) (peeIn p) $$ it)

  ------------------------------------------------------------------------
  -- connect to command socket
  ------------------------------------------------------------------------
  conCmd :: Z.Socket Z.Sub -> String -> IO () -> IO ()
  conCmd cmd sockname ready = do
    trycon      cmd sockname retries
    Z.subscribe cmd ""
    ready
