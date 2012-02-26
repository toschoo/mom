module Network.Mom.Patterns.Device (
         -- * Device Services
         withDevice,
         withQueue, 
         withForwarder, 
         withPipeline, 
         -- * Polling
         PollEntry, pollEntry,
         -- * Access Types
         AccessType(..),
         -- * Device Service Commands
         addDevice, remDevice, changeTimeout,
         -- * Streamer 
         Streamer, getStreamSource, filterTargets,
         -- * Transformer
         Transformer,
         putThrough, ignoreStream, continueHere,

         -- * Transformer Combinators
         -- $recursive_helpers

         emit, emitPart, pass, passBy, end, absorb, merge,
         -- * Helpers
         Identifier, OnTimeout)
where

  import           Types
  import           Service

  import qualified Data.Enumerator        as E
  import           Data.Enumerator (($$))
  import qualified Data.Enumerator.List   as EL
  import           Data.Monoid 
  import qualified Data.Map               as Map
  import           Data.Map (Map)
  import qualified Data.Sequence          as S
  import           Data.Sequence ((|>), ViewR(..), ViewL(..))
  import           Prelude hiding (catch)
  import           Control.Exception (catch, finally, throwIO,
                                      bracketOnError)
  import           Control.Concurrent
  import qualified System.ZMQ as Z

  ------------------------------------------------------------------------
  -- | Starts a device and executes an action that receives a 'Service'
  --   to control the device
  --
  --   Parameters:
  --
  --   * 'Z.Context' - The /ZMQ/ context
  --
  --   * 'String' - The device name
  --
  --   * 'Parameter' - The initial value of the control parameter
  --
  --   * 'Timeout' - The polling timeout:
  --     /< 0/ - listens eternally,
  --     /0/ - returns immediately,
  --     /> 0/ - timeout in microseconds;
  --     when the timeout expires, the 'OnTimeout' action is invoked.
  --
  --   * 'PollEntry' - List of 'PollEntry';
  --                   the device will polll over 
  --                   all list members and direct
  --                   streams to a subset of this list
  --                   determined by the stream transformer.
  --
  --   * 'InBound' - in-bound converter;
  --                 the stream is presented to the transformer
  --                 as chunks of type /o/.
  -- 
  --   * 'OutBound' - out-bound converter
  -- 
  --   * 'OnError_' - Error handler
  -- 
  --   * 'Parameter' -> 'OnTimeout' - Action to perform on timeout
  -- 
  --   * 'Parameter' -> 'Transformer' - The stream transformer
  -- 
  --   * 'Service' -> IO () - The action to invoke,
  --                          when the device has been started;
  --                          The 'Service' is used to control the device.
  ------------------------------------------------------------------------
  withDevice :: Z.Context -> String -> Parameter ->
                Timeout                          -> 
                [PollEntry]                      -> 
                InBound o -> OutBound o          -> 
                OnError_                         ->
                (Parameter -> OnTimeout)         ->
                (Parameter -> Transformer o)     ->
                (Service -> IO a)                -> IO a
  withDevice ctx name param tmo acs iconv oconv onerr ontmo trans action =
    withService ctx name param service action
    where service = device_ tmo acs iconv oconv onerr ontmo trans

  ------------------------------------------------------------------------
  -- | Starts a queue;
  --   a queue connects clients with a dealer ('XDealer'), 
  --                   /i.e./ a load balancer for requests,
  --             and servers with a router ('XRouter') that routes responses
  --                   back to the client.
  --  
  --  Parameters:
  --
  --  * 'Z.Context': the /ZMQ/ Context
  --
  --  * 'String': the queue name
  --
  --  * ('AccessPoint', 'LinkType'):
  --                       the access point of the /dealer/ ('XDealer')
  --                       and its link type;
  --                       you usually want to bind the dealer
  --                       so that many clients can connect to it.
  --
  --  * ('AccessPoint', 'LinkType'):
  --                       the access point of the /router/ ('XRouter');
  --                       and its link type;
  --                       you usually want to bind the router
  --                       so that many servers can connect to it.
  --
  --  * 'OnError_': the error handler
  --
  --  * 'Service' -> IO (): the action to run
  --
  --   'withQueue' is implemented by means of 'withDevice' as:
  --   
  --   @  
  --      withQueue ctx name (dealer, ld) (router, lr) onerr act = 
  --        withDevice ctx name noparam (-1)
  --              [pollEntry \"clients\" XDealer dealer ld [],
  --               pollEntry \"server\"  XRouter router lr []]
  --              return return onerr (\_ -> return ()) (\_ -> putThrough) act
  --   @  
  ------------------------------------------------------------------------
  withQueue :: Z.Context                  -> 
               String                     ->
               (AccessPoint, LinkType)    ->
               (AccessPoint, LinkType)    ->
               OnError_                   -> 
               (Service -> IO a)          -> IO a
  withQueue ctx name (dealer, l1) (router, l2) onerr = 
    withDevice ctx name noparam (-1)
          [pollEntry "clients" XDealer dealer l1 [],
           pollEntry "servers" XRouter router l2 []]
          return return onerr (\_ -> return ()) (\_ -> putThrough)

  ------------------------------------------------------------------------
  -- | Starts a Forwarder;
  --   a forwarder connects a publisher and its subscribers.
  --   Note that the forwarder uses a /subscriber/ ('XSub') 
  --   to conntect to the /publisher/ and
  --   a /publisher/ ('XPub') to bind the /subscribers/.
  --  
  --  Parameters:
  --
  --  * 'Z.Context': the /ZMQ/ Context
  --
  --  * 'String': the forwarder name
  --
  --  * 'Topic': the subscription topic
  --
  --  * ('AccessPoint', 'AccessPoint'):
  --                       the access points;
  --                       the first is the /subscriber/ ('XSub'),
  --                       the second is the /publisher/ ('XPub');
  --                       this rule is not enforced 
  --                       by the type system;  
  --                       you have to take care of it on your own!
  --
  --  * 'OnError_': the error handler
  --
  --  * 'Service' -> IO (): the action to run
  --   
  --   'withForwarder' is implemented by means of 'withDevice' as:
  --
  --   @  
  --      withForwarder ctx name topics (sub, pub) onerr act = 
  --        withDevice ctx name noparam (-1)
  --              [pollEntry \"subscriber\" XSub router Connect topics,
  --               pollEntry \"publisher\"  XPub dealer Bind    []]
  --              return return onerr (\_ -> return ()) (\_ -> putThrough) act
  --   @  
  ------------------------------------------------------------------------
  withForwarder :: Z.Context                  -> 
                   String -> [Topic]          -> 
                   (AccessPoint, LinkType)    ->
                   (AccessPoint, LinkType)    ->
                   OnError_                   -> 
                   (Service -> IO a)          -> IO a
  withForwarder ctx name topics (sub, l1) (pub, l2) onerr = 
    withDevice ctx name noparam (-1)
          [pollEntry "subscriber" XSub sub l1 topics, 
           pollEntry "publisher"  XPub pub l2 []]
          return return onerr (\_ -> return ()) (\_ -> putThrough)

  ------------------------------------------------------------------------
  -- | Starts a pipeline;
  --   a pipeline connects a /pipe/
  --   and its /workers/.
  --   Note that the pipeline uses a /puller/ ('XPull')
  --   to conntect to the /pipe/ and
  --   a /pipe/ ('XPipe') to bind the /pullers/.
  --
  --  Parameters:
  --
  --  * 'Z.Context': the /ZMQ/ Context
  --
  --  * 'String': the pipeline name
  --
  --  * ('AccessPoint', 'LinkType'):
  --                       the access point of the /puller/ ('XPull')
  --                       and its link type;
  --                       you usually want to connect the puller 
  --                       to one pipe so that it appears 
  --                       as one puller among others,
  --                       to which the pipe may send jobs.
  --
  --  * ('AccessPoint', 'LinkType'):
  --                       the access point of the /pipe/ ('XPipe');
  --                       and its link type;
  --                       you usually want to bind the pipe
  --                       so that many pullers can connect to it.
  --
  --  * 'OnError_': the error handler
  --
  --  * 'Service' -> IO (): the action to run
  --
  --   'withPipeline' is implemented by means of 'withDevice' as:
  --   
  --   @  
  --      withPipeline ctx name topics (puller, l1) (pusher, l2) onerr act =
  --        withDevice ctx name noparam (-1)
  --              [pollEntry \"pull\"  XPull puller l1 [],
  --               pollEntry \"push\"  XPush pusher l2 []]
  --              return return onerr (\_ -> return ()) (\_ -> putThrough) act
  --   @  
  ------------------------------------------------------------------------
  withPipeline :: Z.Context                   -> 
                   String                     ->
                   (AccessPoint, LinkType)    ->
                   (AccessPoint, LinkType)    ->
                   OnError_                   -> 
                   (Service -> IO a)          -> IO a
  withPipeline ctx name (puller, l1) (pusher, l2) onerr = 
    withDevice ctx name noparam (-1)
          [pollEntry "pull"  XPull puller l1 [], 
           pollEntry "push"  XPipe pusher l2 []]
          return return onerr (\_ -> return ()) (\_ -> putThrough)

  ------------------------------------------------------------------------
  -- | A transformer is an 'E.Iteratee'
  --   to transform streams.
  --   It receives two arguments:
  --   
  --   * a 'Streamer' which provides information on access points;
  --   
  --   * a 'Sequence' which may be used to store chunks of an incoming
  --     stream before they are sent to the target.
  --
  --   Streamer and sequence keep track of the current transformation.
  --   The streamer knows where the stream comes from and 
  --   may be queried about other streams in the device.
  ------------------------------------------------------------------------
  type Transformer o = Streamer o -> S.Seq o -> E.Iteratee o IO ()

  ------------------------------------------------------------------------
  -- | Holds information on streams and the current state of the device;
  --   streamers are passed to transformers.
  ------------------------------------------------------------------------
  data Streamer o = Streamer {
                      strmSrc    :: (Identifier, Z.Poll),
                      strmIdx    :: Map Identifier Z.Poll,
                      strmPoll   :: [Z.Poll],
                      strmOut    :: OutBound o}

  ------------------------------------------------------------------------
  -- | Retrieves the identifier of the source of the current stream
  ------------------------------------------------------------------------
  getStreamSource :: Streamer o -> Identifier
  getStreamSource = fst . strmSrc

  ------------------------------------------------------------------------
  -- | Filters target streams;
  --   the function resembles /filter/ of 'Data.List':
  --   it receives the property of an 'Identifier';
  --   if a 'PollEntry' has this property, it is added to the result set.
  --
  --   The function is intended to select targets for an out-going stream,
  --   typically based on the identifier of the source stream.
  --   The following example selects all poll entries, but the source:
  --
  --   @
  --     broadcast :: Streamer o -> [Identifier]
  --     broadcast s = filterTargets s notSource
  --       where notSource = (/=) (getStreamSource s)
  --   @
  ------------------------------------------------------------------------
  filterTargets :: Streamer o -> (Identifier -> Bool) -> [Identifier]
  filterTargets s f = map fst $ Map.toList $ Map.filterWithKey flt $ strmIdx s
    where flt k _ = f k

  ------------------------------------------------------------------------
  -- $recursive_helpers
  -- The following functions are building blocks
  -- for defining transformers.
  -- The building blocks operate on sequences, stream targets and 
  -- transformers.
  -- They manipulate streams, send them to targets and enter 
  -- a transformer.
  ------------------------------------------------------------------------
  -- | Sends all sequence elements to the targets identified
  --   by the list of 'Identifier' and terminates the outgoing stream.
  --   The transformation continues with the transformer
  --   passed in and an empty sequence. 
  ------------------------------------------------------------------------
  emit :: Streamer o    -> [Identifier] -> S.Seq o -> 
          Transformer o -> E.Iteratee o IO ()
  emit s is os go = tryIO sender >> go s S.empty
    where sender = mapM_ (\i -> sendStreamer s i (sendseq s os True)) is

  ------------------------------------------------------------------------
  -- | Sends all sequence elements to the targets identified
  --   by the list of 'Identifier', but unlike 'emit', 
  --   does not terminate the outgoing stream.
  --   The transformation continues with the transformer
  --   passed in and an empty sequence. 
  --
  --   Note that all outgoing streams, once started, 
  --   have to be terminated before the transformer ends. 
  --   Otherwise, a protocol error will occur.
  ------------------------------------------------------------------------
  emitPart :: Streamer o    -> [Identifier] -> S.Seq o -> 
              Transformer o -> E.Iteratee o IO ()
  emitPart s is os go = tryIO sender >> go s S.empty
    where sender = mapM_ (\i -> sendStreamer s i (sendseq s os False)) is

  ------------------------------------------------------------------------
  -- | Sends one element (/o/) to the targets and continues 
  --   with an empty sequence;
  --   the Boolean parameter determines whether this is the last message
  --   to send. 
  --
  --   Note that all outgoing streams, once started, 
  --   have to be terminated before the transformer ends. 
  --   Otherwise, a protocol error will occur.
  ------------------------------------------------------------------------
  pass :: Streamer o    -> [Identifier] -> o -> Bool ->
          Transformer o -> E.Iteratee o IO ()
  pass s is o lst go = tryIO sender >> go s S.empty
    where sender = mapM_ (\i -> sendStreamer s i 
                                 (sendseq s (S.singleton o) lst)) is

  ------------------------------------------------------------------------
  -- | Sends one element (/o/) to the targets, 
  --   but, unlike 'pass', passes the sequence to the transformer. 
  --   'passBy' does not terminate the outgoing stream.
  ------------------------------------------------------------------------
  passBy :: Streamer o    -> [Identifier] -> o -> S.Seq o -> 
           Transformer o -> E.Iteratee o IO ()
  passBy s is o os go = tryIO sender >> go s os
    where sender = mapM_ (\i -> sendStreamer s i 
                                 (sendseq s (S.singleton o) False)) is

  ------------------------------------------------------------------------
  -- | Terminates the outgoing stream by sending the new element
  --   as last segment to all targets and ends the transformer
  --   by ignoring the rest of the incoming stream.
  ------------------------------------------------------------------------
  end :: Streamer o -> [Identifier] -> o -> E.Iteratee o IO ()
  end s is o = pass s is o True (\_ _ -> go)
    where go = EL.head >>= \mbO ->
               case mbO of
                 Nothing -> return ()
                 Just _  -> go

  ------------------------------------------------------------------------
  -- | Adds a new element to the sequence
  --   and calls the transformer without sending anything
  ------------------------------------------------------------------------
  absorb :: Streamer o    -> o -> S.Seq o ->
            Transformer o -> E.Iteratee o IO ()
  absorb s o os go = go s (os |> o)

  ------------------------------------------------------------------------
  -- | Merges the new element with the last element of the sequence;
  --   if the sequence is currently empty, the new element
  --   will be its only member.
  --   Merged elements appear as one element of the sequence 
  --   in the continuation of the transformation.
  --   The type /o/ must be a 'Monoid', /i.e./,
  --   it must implement /mappend/ and /mempty/.
  --   The function does not send anything.
  ------------------------------------------------------------------------
  merge :: Monoid o => Streamer o    -> o -> S.Seq o ->
                       Transformer o -> E.Iteratee o IO ()
  merge s o os go = 
    let os' = case S.viewr os of
                EmptyR  -> S.singleton o
                xs :> x -> xs |> (x `mappend` o)
     in go s os'

  ------------------------------------------------------------------------
  -- | Transformer that
  --   passes messages one-to-one to all poll entries 
  --   but the current source
  ------------------------------------------------------------------------
  putThrough :: Transformer a
  putThrough s' os' = EL.head >>= \mbo -> go mbo s' os'
    where go mbo s _ = do
            mbo' <- EL.head
            case mbo of
              Nothing -> return ()
              Just x  -> do
                let lst = case mbo' of
                            Nothing -> True
                            Just _  -> False
                let trg = filterTargets s (/= getStreamSource s)
                pass s trg x lst (go mbo')

  ------------------------------------------------------------------------
  -- | Transformer that
  --   ignores the remainder of the current stream;
  --   it is usually used to terminate a transformer.
  ------------------------------------------------------------------------
  ignoreStream :: Transformer a
  ignoreStream _ _ = EL.consume >>= \_ -> return ()

  ------------------------------------------------------------------------
  -- | Transformer that
  --   does nothing but continuing the transformer, from which it is called
  --   and, hence, is identical to /return ()/;
  --   it is usually passed to a transformer combinator,
  --   like 'emit', to continue processing right here 
  --   instead of recursing into another transformer.
  ------------------------------------------------------------------------
  continueHere :: Transformer a
  continueHere _ _ = return ()

  ------------------------------------------------------------------------
  -- Internal
  ------------------------------------------------------------------------
  sendStreamer :: Streamer o -> Identifier -> (Z.Poll -> IO ()) -> IO ()
  sendStreamer s i act = case Map.lookup i (strmIdx s) of
                           Nothing  -> return ()
                           Just p   -> act p

  mapIOSeq :: (a -> IO ()) -> S.Seq a -> IO ()
  mapIOSeq f os = case S.viewl os of
                    EmptyL  -> return ()
                    x :< xs -> f x >> mapIOSeq f xs

  sendseq :: Streamer o -> S.Seq o -> Bool -> Z.Poll -> IO ()
  sendseq s os lst p 
    | lst       = case S.viewr os of
                    EmptyR  -> return ()
                    xs :> x -> mapIOSeq (\o -> dosend s p o False) xs
                               >>              dosend s p x True 
    | otherwise = mapIOSeq (\o -> dosend s p o False) os

  dosend :: Streamer o -> Z.Poll -> o -> Bool -> IO ()
  dosend  s (Z.S sock _) o lst = 
    let flg = if lst then [] else [Z.SndMore]
     in strmOut s o >>= \x -> Z.send sock x flg
  dosend  _ _ _ _ = error "Ouch!"

  ------------------------------------------------------------------------
  -- Creates poll list and enters runDevice 
  ------------------------------------------------------------------------
  device_ :: Timeout                   ->
             [PollEntry]               -> 
             InBound o -> OutBound o   -> 
             OnError_                  ->
             (String -> OnTimeout)     ->
             (String -> Transformer o) ->
             Z.Context -> String       -> 
             String -> String -> IO () -> IO ()
  device_ tmo acs iconv oconv onerr ontmo trans
          ctx name sockname param imReady = do 
    xp <- catch (mkPoll ctx tmo acs Map.empty [] [])
                (\e -> onerr Fatal e name param >> throwIO e)
    m  <- newMVar xp
    finally (runDevice name m iconv oconv onerr ontmo trans 
                       sockname param imReady)
            (withMVar m (mapM_ closeS . xpPoll) >> return ())

  closeS :: Z.Poll -> IO ()
  closeS p = case p of 
               Z.S s _ -> safeClose s
               _       -> return ()

  ------------------------------------------------------------------------
  -- creates and binds or connects all sockets recursively;
  -- on its way, creates the Map from Identifiers to PollItems,
  --             a list of PollItems
  --             and a list of Identifiers with the same order;
  -- finally executes "run"
  ------------------------------------------------------------------------
  mkPoll :: Z.Context -> Timeout     -> 
            [PollEntry]              -> 
            Map Identifier Z.Poll    ->
            [Identifier]             ->
            [Z.Poll]                 -> IO XPoll 
  mkPoll ctx t []     m is ps = return XPoll{xpCtx  = ctx,
                                             xpTmo  = t,
                                             xpMap  = m,
                                             xpIds  = is,
                                             xpPoll = ps}
  mkPoll ctx t (k:ks) m is ps = bracketOnError
    (access ctx (pollType k)
                (pollLink k) 
                (pollOs   k) 
                (pollAdd  k) 
                (pollSub  k))
    (\p -> closeS p >> return [])
    (\p -> do let m'  = Map.insert (pollId k) p m
              let is' = pollId k : is
              let ps' = p:ps
              mkPoll ctx t ks m' is' ps')

  ------------------------------------------------------------------------
  -- finally start the device entering Service.xpoll
  ------------------------------------------------------------------------
  runDevice :: String -> MVar XPoll      -> 
               InBound o -> OutBound o   -> 
               OnError_                  -> 
               (String -> OnTimeout)     ->
               (String -> Transformer o) -> 
               String -> String -> IO () -> IO ()
  runDevice name mxp iconv oconv onerr ontmo trans sockname param imReady = (do
      xp <- readMVar mxp
      Z.withSocket (xpCtx xp) Z.Sub $ \cmd -> do
        trycon      cmd sockname retries
        Z.subscribe cmd ""
        let p = Z.S cmd Z.In
        modifyMVar_ mxp $ \_ -> return xp {xpPoll = p : xpPoll xp}
        imReady
        finally (xpoll False mxp ontmo go param)
                (modifyMVar_ mxp $ \xp' -> 
                   return xp' {xpPoll = tail (xpPoll xp')})) 
      `catch` (\e -> onerr Fatal e name param) -- >> throwIO e)
    where go i poller p =
            case poller of 
              Z.S s _ -> do
                xp <- readMVar mxp
                let strm = Streamer {
                             strmSrc  = (i, poller), 
                             strmIdx  = xpMap  xp,
                             strmPoll = xpPoll xp,
                             strmOut  = oconv}
                eiR <- E.run (rcvEnum s iconv $$ 
                              trans p strm S.empty) 
                case eiR of
                  Left e  -> consumeOnErr s >> onerr Error e name p 
                  Right _ -> return ()
              _ -> error "Ouch!"

  consumeOnErr :: Z.Socket a -> IO ()
  consumeOnErr s = E.run_ (rcvEnum s idIn $$ EL.consume >>= \_ -> return ())

