module Network.Mom.Patterns.Device (
         -- * Access Types
         AccessType(..),
         -- * Device Services
         withDevice,
         withQueue, 
         withForwarder, 
         withPipeline, 
         -- * Polling
         PollEntry, pollEntry,
         -- * Device Service Commands
         addDevice, remDevice, changeTimeout,
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
         Identifier,
         Timeout, OnTimeout)
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
  import           Prelude hiding (catch)
  import           Control.Exception (catch, finally, throwIO,
                                      bracketOnError)
  import           Control.Concurrent
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
    where service = device_ tmo acs iconv oconv onerr ontmo trans

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
               (AccessPoint, LinkType)    ->
               (AccessPoint, LinkType)    ->
               OnError_                   -> 
               (Service -> IO ())         -> IO ()
  withQueue ctx name (dealer, l1) (router, l2) onerr act = 
    withDevice ctx name noparam (-1)
          [pollEntry "clients" XDealer dealer l1 "",
           pollEntry "servers" XRouter router l2 ""]
          return return onerr (\_ -> return ()) (\_ -> putThrough) act

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
                   (AccessPoint, LinkType)    ->
                   (AccessPoint, LinkType)    ->
                   OnError_                   -> 
                   (Service -> IO ())         -> IO ()
  withForwarder ctx name topics (sub, l1) (pub, l2) onerr act = 
    withDevice ctx name noparam (-1)
          [pollEntry "subscriber" XSub sub l1 topics, 
           pollEntry "publisher"  XPub pub l2 ""]
          return return onerr (\_ -> return ()) (\_ -> putThrough) act

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
                   (AccessPoint, LinkType)    ->
                   (AccessPoint, LinkType)    ->
                   OnError_                   -> 
                   (Service -> IO ())         -> IO ()
  withPipeline ctx name (puller, l1) (pusher, l2) onerr act = 
    withDevice ctx name noparam (-1)
          [pollEntry "pull"  XPull puller l1 "", 
           pollEntry "push"  XPipe pusher l2 ""]
          return return onerr (\_ -> return ()) (\_ -> putThrough) act

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
  -- Creates poll list and enters runDevice 
  ------------------------------------------------------------------------
  device_ :: Timeout                   ->
             [PollEntry]               -> 
             InBound o -> OutBound o   -> 
             OnError_                  ->
             (String -> OnTimeout)     ->
             (String -> Transformer o) ->
             Z.Context -> String       -> 
             String -> String          -> IO ()
  device_ tmo acs iconv oconv onerr ontmo trans
          ctx name sockname param = do 
    xp <- catch (mkPoll ctx tmo acs Map.empty [] [])
                (\e -> onerr Fatal e name param >> throwIO e)
    m  <- newMVar xp
    finally (runDevice name m iconv oconv onerr ontmo trans sockname param)
            (do _ <- withMVar m (\xp' -> mapM_ closeS (xpPoll xp'))
                return ())

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
               String -> String          -> IO ()
  runDevice name mxp iconv oconv onerr ontmo trans sockname param = (do
      xp <- readMVar mxp
      Z.withSocket (xpCtx xp) Z.Sub $ \cmd -> do
        trycon      cmd sockname retries
        Z.subscribe cmd ""
        let p = Z.S cmd Z.In
        modifyMVar_ mxp $ \_ -> return xp {xpPoll = p : xpPoll xp}
        finally (xpoll False mxp ontmo go param)
                (modifyMVar_ mxp $ \xp' -> 
                   return xp' {xpPoll = tail (xpPoll xp')})) 
      `catch` (\e -> onerr Fatal e name param >> throwIO e)
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
                  Left e  -> onerr Error e name p
                  Right _ -> return ()
              _ -> error "Ouch!"

