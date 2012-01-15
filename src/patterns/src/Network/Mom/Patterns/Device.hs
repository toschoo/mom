module Network.Mom.Patterns.Device (
         -- * Device Services
         withDevice,
         withQueue, queue, 
         withForwarder, forward, 
         withPipeline, pipeline,
         -- * Polling
         PollEntry, pollEntry,
         -- * Streamer 
         Streamer, getStreamSource, filterTargets,
         -- * Transformer
         Transformer,
         putThrough,

         -- * Transformer Building Blocks
         -- $recursive_helpers

         emit, emit2, pass, pass2, end,
         absorb, merge, ignore, purge,
         set, reset, 
         -- * Some Helpers
         OnTimeout)
       
where

  import           Types
  import           Service

  import qualified Data.ByteString.Char8  as B
  import qualified Data.Enumerator        as E
  import           Data.Enumerator (($$))
  import qualified Data.Enumerator.List   as EL (head)
  import           Data.Monoid 
  import qualified Data.Map               as Map
  import           Data.Map (Map)
  import qualified Data.Sequence          as S
  import           Data.Sequence ((|>), ViewR(..))
  import qualified System.ZMQ as Z

  ------------------------------------------------------------------------
  -- | Starts a device;
  --
  --   Parameters:
  --
  --   * 'Z.Context' - The /zmq/ context
  --
  --   * 'String' - The device name
  --
  --   * 'String' - The device parameter
  --
  --   * 'Timeout' - The polling timeout:
  --     /< 0/ - listens eternally,
  --     /0/ - returns immediately,
  --     /> 0/ - timeout in microseconds;
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
  --   * 'OnError' - Error handler
  -- 
  --   * 'OnTimeout' - Action to perform on timeout;
  --                   the 'String' represents 
  --                   the current value of the device parameter.
  -- 
  --   * 'Transformer' - The stream transformer;
  --                     the 'String' represents 
  --                     the current value of the device parameter.
  -- 
  --   * 'Service' -> IO () - The action to invoke,
  --                          when the device has been started;
  --                          The 'Service' is used to control the device.
  ------------------------------------------------------------------------
  withDevice :: Z.Context -> String -> String  ->
                Timeout                        -> 
                [PollEntry]                    -> 
                InBound o -> OutBound o        -> 
                OnError_                       ->
                (String -> OnTimeout)          ->
                (String -> Transformer o)      ->
                (Service -> IO ())             -> IO ()
  withDevice ctx name param tmo acs iconv oconv onerr ontmo trans action =
    withService ctx name param service action
    where service = device_ True tmo acs iconv oconv onerr ontmo trans

  ------------------------------------------------------------------------
  -- | Starts a controlled queue;
  --   a queue links Clients with a Dealer, 
  --                   /i.e./ a load balancer for requests,
  --             and Servers with a Router that routes responses
  --                   back to the client.
  --  
  --  Parameters:
  --
  --  * 'Z.Context': the /zmq/ Context
  --
  --  * 'String': the queue name
  --
  --  * ('AccessPoint', 'AccessPoint'): 
  --                       the access points;
  --                       the first must be a /dealer/,
  --                       the second must be a /router/;
  --                       this rule is not enforced 
  --                       by the type system;  
  --                       you have to take care of it on your own!
  --
  --  * 'OnError_': the error handler
  --
  --  * 'Service' -> IO (): the action to run
  --
  --   'withQueue' is implemented by means of 'withDevice' as:
  --   
  --   @  
  --      withQueue ctx name (dealer, router) onerr act = 
  --        withDevice ctx name noparam (-1)
  --              [pollEntry \"clients\" XDealer dealer Bind    \"\",
  --               pollEntry \"server\"  XRouter router Connect \"\"]
  --              return return onerr (\_ -> return ()) (\_ -> putThrough) act
  --   @  
  ------------------------------------------------------------------------
  withQueue :: Z.Context                  -> 
               String                     ->
               (AccessPoint, AccessPoint) ->
               OnError_                   -> 
               (Service -> IO ())         -> IO ()
  withQueue ctx name (dealer, router) onerr act = 
    withDevice ctx name noparam (-1)
          [pollEntry "clients" XDealer dealer Bind "",
           pollEntry "servers" XRouter router Bind ""]
          return return onerr (\_ -> return ()) (\_ -> putThrough) act

  ------------------------------------------------------------------------
  -- | Starts an uncontrolled Queue;
  --   implemented on top of 'Z.device'
  ------------------------------------------------------------------------
  queue :: Z.Context -> AccessPoint -> AccessPoint -> IO ()
  queue ctx src trg = do
    Z.withSocket ctx Z.XRep $ \from -> do
      Z.bind from (acAdd src)
      Z.withSocket ctx Z.XReq $ \to -> do
        Z.bind to (acAdd trg)
        Z.device Z.Queue from to

  ------------------------------------------------------------------------
  -- | Starts a Forwarder;
  --   links a publisher and its subscribers;
  --   implemented by means of 'withDevice' as:
  --  
  --  Parameters:
  --
  --  * 'Z.Context': the /zmq/ Context
  --
  --  * 'String': the forwarder name
  --
  --  * 'String': the subscription topic
  --
  --  * ('AccessPoint', 'AccessPoint'):
  --                       the access points;
  --                       the first must be a /subscriber/,
  --                       the second must be a /publisher/;
  --                       this rule is not enforced 
  --                       by the type system;  
  --                       you have to take care of it on your own!
  --
  --  * 'OnError_': the error handler
  --
  --  * 'Service' -> IO (): the action to run
  --   
  --   @  
  --      withForwarder ctx name topics (sub, pub) onerr act = 
  --        withDevice ctx name noparam (-1)
  --              [pollEntry \"subscriber\" XSub router Connect topics,
  --               pollEntry \"publisher\"  XPub dealer Bind    \"\"]
  --              return return onerr (\_ -> return ()) (\_ -> putThrough) act
  --   @  
  ------------------------------------------------------------------------
  withForwarder :: Z.Context                  -> 
                   String -> String           -> 
                   (AccessPoint, AccessPoint) ->
                   OnError_                   -> 
                   (Service -> IO ())         -> IO ()
  withForwarder ctx name topics (sub, pub) onerr act = 
    withDevice ctx name noparam (-1)
          [pollEntry "subscriber" XSub sub Connect topics, 
           pollEntry "publisher"  XPub pub Bind    ""]
          return return onerr (\_ -> return ()) (\_ -> putThrough) act

  ------------------------------------------------------------------------
  -- | Starts an uncontrolled forwarder;
  --   implemented on top of 'Z.device'
  ------------------------------------------------------------------------
  forward :: Z.Context -> AccessPoint -> AccessPoint -> IO ()
  forward ctx src trg = do
    Z.withSocket ctx Z.Pub $ \from -> do
      Z.bind from (acAdd src)
      Z.withSocket ctx Z.Sub $ \to -> do
        Z.bind to (acAdd trg)
        Z.device Z.Forwarder from to

  ------------------------------------------------------------------------
  -- | Starts pipeline;
  --   linking 'Pipe' and 'Puller';
  --
  --  Parameters:
  --
  --  * 'Z.Context': the /zmq/ Context
  --
  --  * 'String': the pipeline name
  --
  --  * ('AccessPoint', 'AccessPoint'): 
  --                       the access points;
  --                       the first must be a /puller/,
  --                       the second must be a /pipe/;
  --                       this rule is not enforced 
  --                       by the type system;  
  --                       you have to take care of it on your own!
  --
  --  * 'OnError_': the error handler
  --
  --  * 'Service' -> IO (): the action to run
  --
  --   'withPipe' is implemented by means of 'withDevice' as:
  --   
  --   @  
  --      withPipeline ctx name topics (puller, pusher) onerr act = 
  --        withDevice ctx name noparam (-1)
  --              [pollEntry \"pull\"  XPull puller Connect topics,
  --               pollEntry \"push\"  XPush pusher Bind    \"\"]
  --              return return onerr (\_ -> return ()) (\_ -> putThrough) act
  --   @  
  ------------------------------------------------------------------------
  withPipeline :: Z.Context                   -> 
                   String                     ->
                   (AccessPoint, AccessPoint) ->
                   OnError_                   -> 
                   (Service -> IO ())         -> IO ()
  withPipeline ctx name (puller, pusher) onerr act = 
    withDevice ctx name noparam (-1)
          [pollEntry "pull"  XPull puller Connect "", 
           pollEntry "push"  XPipe pusher Bind    ""]
          return return onerr (\_ -> return ()) (\_ -> putThrough) act

  ------------------------------------------------------------------------
  -- | Starts an uncontrolled pipeline;
  --   implemented on top of 'Z.device'
  ------------------------------------------------------------------------
  pipeline :: Z.Context -> AccessPoint -> AccessPoint -> IO ()
  pipeline ctx src trg = do
    Z.withSocket ctx Z.Push $ \from -> do
      Z.bind from (acAdd src)
      Z.withSocket ctx Z.Pull $ \to -> do
        Z.bind to (acAdd trg)
        Z.device Z.Streamer from to

  -- addDevice  :: Service -> PollEntry  -> IO ()
  -- remDevice  :: Service -> Identifier -> IO ()
  -- newTimeout :: Service -> Timeout    -> IO ()

  ------------------------------------------------------------------------
  -- | Type of a 'PollEntry';
  ------------------------------------------------------------------------
  data AccessType = 
         -- | Represents a Service and expects connections from Clients;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.Rep'
         XServer    
         -- | Represents a Client and connects to a Service;
         --   should be used with 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.Req'
         | XClient
         -- | Represents a load balancer, 
         --   expecting connections from clients;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.XRep'
         | XDealer 
         -- | Represents a router
         --   expecting connections from servers;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.XReq'
         | XRouter 
         -- | Represents a publisher;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.Pub'
         | XPub    
         -- | Represents a subscriber;
         --   should be used with 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.Sub'
         | XSub    
         -- | Represents a Pipe;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.Push'
         | XPipe
         -- | Represents a Puller;
         --   should be used with 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.Pull'
         | XPull
         -- | Represents a Peer;
         --   corresponding peers must use complementing 'LinkType';
         --   corresponds to ZMQ Socket Type 'Z.Pair'
         | XPeer   
    deriving (Eq, Show, Read)

  ------------------------------------------------------------------------
  -- | A poll entry describes how to handle an AccessPoint
  ------------------------------------------------------------------------
  data PollEntry = Poll {
                     pollId   :: Identifier,
                     pollAdd  :: String,
                     pollType :: AccessType,
                     pollLink :: LinkType,
                     pollSub  :: String,
                     pollOs   :: [Z.SocketOption]
                   }

  ------------------------------------------------------------------------
  -- | Creates a 'PollEntry';
  --
  --   Parameters:
  --
  --   * 'Identifier': identifies an 'AccessPoint' with a 'String';
  --                    the string shall be unique for one device.
  --
  --   * 'AccessType': the 'AccessType' of this 'AccessPoint'
  --
  --   * 'AccessPoint': the 'AccessPoint' itself
  --
  --   * 'LinkType': how to link to this 'AccessPoint'
  --
  --   * 'String': A subscription topic - 
  --                ignored for all 'AccessPoint', but those
  --                with 'AccessType' 'Sub' 
  ------------------------------------------------------------------------
  pollEntry :: Identifier -> 
               AccessType -> AccessPoint -> LinkType ->
               String     -> PollEntry
  pollEntry i at ac lt sub = Poll {
                               pollId   = i,
                               pollAdd  = acAdd ac,
                               pollType = at,
                               pollLink = lt,
                               pollSub  = sub,
                               pollOs   = acOs ac}
  
  ------------------------------------------------------------------------
  -- | A transformer is an 'E.Iteratee'
  --   to transforms streams.
  --   It receives two arguments:
  --   
  --   * a 'Streamer' which provides information on access points;
  --   
  --   * a 'Sequence' which may be used to store chunks of an incoming
  --     stream before they are send to the target.
  --
  --   Streamer and sequence hold state about the current transformation.
  --   The streamer knows where the stream comes from and 
  --   may be queried about other streams in the device.
  ------------------------------------------------------------------------
  type Transformer o = Streamer o -> S.Seq o -> E.Iteratee o IO ()

  ------------------------------------------------------------------------
  -- | Holds information on streams and the current state of the device;
  --   streamers are passed into transformers by the device.
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
  --   it receives a property of an 'Identifier';
  --   if the 'PollEntry' identifier has this property, it is returned.
  --   Identifiers are used to direct ougoing streams.
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
  -- | Sends all stream elements to the targets identified
  --   by the list of 'Identifier';
  --   then it calls the 'Transformer' with an empty sequence.
  --   The Boolean parameter determines whether the stream 
  --   ends with this sequence. 
  --   Note that all outgoing streams, once started,
  --   have to be terminated before the transformer ends.
  --   Otherwise, a protocol error will occur.
  ------------------------------------------------------------------------
  emit :: Streamer o    -> [Identifier] -> S.Seq o -> Bool ->
          Transformer o -> E.Iteratee o IO ()
  emit s is os lst go = tryIO sender >> go s S.empty
    where sender = mapM_ (\i -> sendStreamer s i (sendseq s os lst)) is

  ------------------------------------------------------------------------
  -- | Like 'emit' but adds a new element 
  --   at the end of the sequence before sending it
  ------------------------------------------------------------------------
  emit2 :: Streamer o    -> [Identifier] -> o -> S.Seq o -> Bool -> 
           Transformer o -> E.Iteratee o IO ()
  emit2 s is o os = emit s is (os |> o)

  ------------------------------------------------------------------------
  -- | Sends one element to the targets and starts the transformer
  --   with an empty sequence
  ------------------------------------------------------------------------
  pass :: Streamer o    -> [Identifier] -> o -> Bool ->
          Transformer o -> E.Iteratee o IO ()
  pass s is o = emit s is (S.singleton o)

  ------------------------------------------------------------------------
  -- | Sends one element to the targets, but leaves the sequence untouched;
  --   the transformer will, hence, continue working 
  --   with sequence passed into 'pass2'
  ------------------------------------------------------------------------
  pass2 :: Streamer o    -> [Identifier] -> o -> S.Seq o -> Bool ->
           Transformer o -> E.Iteratee o IO ()
  pass2 s is o os lst go = tryIO sender >> go s os
    where sender = mapM_ (\i -> sendStreamer s i 
                                 (sendseq s (S.singleton o) lst)) is

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
  -- | Simply ignores the new element
  --   and continues the transformer with the same sequence
  --   and without sending anything
  ------------------------------------------------------------------------
  ignore :: Streamer o -> S.Seq o -> Transformer o -> E.Iteratee o IO ()
  ignore s os go = go s os

  ------------------------------------------------------------------------
  -- | Continues with the sequence passed in
  --   without sending anything
  ------------------------------------------------------------------------
  set :: Streamer o -> S.Seq o -> Transformer o -> E.Iteratee o IO ()
  set s os go = go s os

  ------------------------------------------------------------------------
  -- | Continues the transformer with the new element 
  --   as the only member of the sequence
  --   without sending anything
  ------------------------------------------------------------------------
  reset :: Streamer o -> o -> Transformer o -> E.Iteratee o IO ()
  reset s o = set s (S.singleton o)

  ------------------------------------------------------------------------
  -- | Continues with an empty sequence
  --   without sending anything
  ------------------------------------------------------------------------
  purge :: Streamer o -> Transformer o -> E.Iteratee o IO ()
  purge s go = go s S.empty

  ------------------------------------------------------------------------
  -- | Simple Transformer for devices with two access points;
  --   passes messages one to one to the respectively other access point
  ------------------------------------------------------------------------
  putThrough :: Transformer B.ByteString
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
  -- | What to do on timeout
  ------------------------------------------------------------------------
  type OnTimeout = IO ()

  ------------------------------------------------------------------------
  -- Internal
  ------------------------------------------------------------------------
  sendStreamer :: Streamer o -> Identifier -> (Z.Poll -> IO ()) -> IO ()
  sendStreamer s i act = case Map.lookup i (strmIdx s) of
                           Nothing  -> return ()
                           Just p   -> act p

  mapIOSeq :: (a -> IO ()) -> S.Seq a -> IO ()
  mapIOSeq f os = case S.viewr os of
                    EmptyR  -> return ()
                    xs :> x -> f x >> mapIOSeq f xs

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
  -- enters startPoll passing runDevice as job
  ------------------------------------------------------------------------
  device_ :: Bool -> Timeout           ->
             [PollEntry]               -> 
             InBound o -> OutBound o   -> 
             OnError_                  ->
             (String -> OnTimeout)     ->
             (String -> Transformer o) ->
             Z.Context -> String       -> 
             String -> String          -> IO ()
  device_ controlled tmo acs 
          iconv oconv onerr ontmo trans 
          ctx name sockname param = 
    startPoll ctx acs Map.empty [] [] run
    where run = runDevice ctx name controlled tmo
                          iconv oconv onerr ontmo 
                          trans sockname param

  ------------------------------------------------------------------------
  -- creates and binds or connects all sockets recursively;
  -- on its way, creates the Map from Identifiers to PollItems,
  --             a list of PollItems
  --             and a list of Identifiers with the same order;
  -- finally executes "run"
  ------------------------------------------------------------------------
  startPoll :: Z.Context -> [PollEntry]    -> 
               Map Identifier Z.Poll       ->
               [Identifier]                ->
               [Z.Poll]                    -> 
               (Map Identifier Z.Poll      ->
                [Identifier]               -> 
                [Z.Poll]     -> IO ())     -> IO ()
  startPoll _   []     m is ps run = run m is ps
  startPoll ctx (k:ks) m is ps run = 
    case pollType k of
      XServer -> Z.withSocket ctx Z.Rep  go
      XClient -> Z.withSocket ctx Z.Req  go
      XDealer -> Z.withSocket ctx Z.XRep go
      XRouter -> Z.withSocket ctx Z.XReq go
      XPub    -> Z.withSocket ctx Z.Pub  go
      XPipe   -> Z.withSocket ctx Z.Push go
      XPull   -> Z.withSocket ctx Z.Pull go
      XPeer   -> Z.withSocket ctx Z.Pair go
      XSub    -> Z.withSocket ctx Z.Sub  $ \s -> 
                  Z.subscribe s (pollSub k) >> go s
    where go s = do case pollLink k of 
                      Bind    -> Z.bind    s (pollAdd k)
                      Connect -> trycon    s (pollAdd k) retries
                    let p   = Z.S s Z.In
                    let m'  = Map.insert (pollId k) p m
                    let is' = pollId k : is
                    let ps' = p:ps
                    startPoll ctx ks m' is' ps' run

  ------------------------------------------------------------------------
  -- finally starts the device entering Service.xpoll
  ------------------------------------------------------------------------
  runDevice :: Z.Context -> String       -> 
               Bool      -> Timeout      ->
               InBound o -> OutBound o   -> 
               OnError_                  -> 
               (String -> OnTimeout)     ->
               (String -> Transformer o) -> 
               String -> String          ->
               Map Identifier Z.Poll     ->
               [Identifier]              ->
               [Z.Poll]                  -> IO ()
  runDevice ctx name controlled tmo
            iconv oconv onerr ontmo trans 
            sockname param m is ps = 
    if controlled
      then Z.withSocket ctx Z.Sub $ \cmd -> do
              trycon      cmd sockname retries
              Z.subscribe cmd ""
              let p = Z.S cmd Z.In
              xpoll False (XPoll tmo m is (p:ps)) ontmo go param
      else    xpoll False (XPoll tmo m is ps    ) ontmo go param
    where go (XPoll _  m' _ ps') i poller p =
            case poller of 
              Z.S s _ -> do
                let strm = Streamer {
                             strmSrc  = (i, poller), 
                             strmIdx  = m',
                             strmPoll = ps',
                             strmOut  = oconv}
                eiR <- E.run (rcvEnum s iconv $$ 
                              trans p strm S.empty) 
                case eiR of
                  Left e  -> onerr Error e name
                  Right _ -> return ()
              _ -> error "Ouch!"


