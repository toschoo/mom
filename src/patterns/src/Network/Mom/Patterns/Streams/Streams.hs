{-# LANGUAGE CPP, DeriveDataTypeable, RankNTypes #-}
module Network.Mom.Patterns.Streams.Streams (
         withStreams,
         runReceiver, runSender,
         Streamer, 
         StreamConduit, StreamSink, StreamAction,
         filterStreams, getSource,
         stream, part, passAll, pass1, ignoreStream, 
         Controller, Control, internal,
         stop, pause, resume, send, receive,
         AccessType(..), parseAccess,
         LinkType(..), link, parseLink, 
         PollEntry(..)
       )
where

  import           Control.Monad.Trans (liftIO)
  import           Control.Monad (when)
  import           Control.Applicative ((<$>))
  import           Prelude hiding (catch)
  import           Control.Exception (SomeException, 
                                      bracket, bracketOnError, finally,
                                      catch, throwIO)
  import           Control.Concurrent
  import           Data.Conduit (($$))
  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
  import           Data.Char (toLower)
  import           Data.Map (Map)
  import qualified Data.Map as Map
  import qualified System.ZMQ            as Z

  import           Factory
  import           Network.Mom.Patterns.Streams.Types

  ------------------------------------------------------------------------
  -- | Streams
  ------------------------------------------------------------------------
  withStreams :: Context      -> 
                 Service      ->
                 Timeout      ->
                 [PollEntry]  ->
                 StreamAction -> -- on timeout
                 OnError_     -> -- error handler
                 StreamSink   -> -- stream handler
                 Control a    -> IO a
  withStreams ctx sn tmo pes
              onTmo
              onErr
              onStream
              ctrl = do
      running <- newEmptyMVar
      ready   <- newEmptyMVar
      catch (startService ready running)
            (\e -> do onErr Fatal e "Service is going down"
                      throwIO (e::SomeException))
    where startService ready running = do
            cmdN <- cmdName sn
            _    <- forkIO $ finally (runStreams cmdN ready `catch` 
                                       \e -> do onErr Fatal e 
                                                  "Service is going down"
                                                throwIO (e::SomeException))
                                     (putMVar running ())
            Z.withSocket ctx Z.XReq $ \cmd -> do
              _ <- readMVar ready
              Z.connect cmd cmdN
              let c = Controller {ctrlCtx  = ctx,
                                  ctrlCmd  = cmd,
                                  ctrlOpts = []}
              finally (ctrl c)
                      (finally (stop c)
                               (takeMVar running))
          runStreams cmdN ready = 
            Z.withSocket ctx Z.XReq $ \cmd -> bracket
                         (do Z.bind cmd cmdN
                             let c = Z.S cmd Z.In
                             (m, is, ps) <- mkPoll ctx pes Map.empty [] []
                             return (m, c, is, ps))
                         (\(_, _, _,  ps) -> mapM_ closeS ps)
                         (\(m, c, is, ps) -> do xp <- newMVar PollSt {
                                                        polMap  = m,
                                                        polCmd  = c,
                                                        polIs   = is,
                                                        polPs   = ps,
                                                        polTmo  = tmo,
                                                        polCont = True
                                                }
                                                putMVar ready () 
                                                poll xp)

          ----------------------------------------------------------------
          -- Poll
          ----------------------------------------------------------------
          poll :: PollT ()
          poll xp = do
            c <- psCmd xp
            (_, ps) <- psPolls xp

            (x:ss)   <- Z.poll (c:ps) tmo
            case x of
              Z.S _ Z.In -> do catch (handleCmd xp)
                                     (\e -> do onErr Critical e 
                                                     "Can't handle command"
                                               cleanStream c)
                               q <- psContinue xp
                               when q $ poll xp
              _            -> handleStream ss xp >> poll xp
           
          ----------------------------------------------------------------
          -- Handle Streams
          ----------------------------------------------------------------
          handleStream :: [Z.Poll] -> PollT () 
          handleStream ss xp = do
            m <- psMap xp
            c <- psCmd xp
            (is, ps) <- psPolls xp
            case getStream is ss of
              Nothing -> onTmo Streamer {strmSrc  = Nothing, 
                                         strmIdx  = m,
                                         strmCmd  = c,
                                         strmPoll = ps} `catch` 
                           \e -> onErr Error e "Timeout Action failed"
              Just (i, p) -> catch (
                C.runResourceT $ readPoll p $$
                                   onStream Streamer {
                                               strmSrc  = Just (i, p),
                                               strmIdx  = m,
                                               strmCmd  = c,
                                               strmPoll = ps}) (
                \e -> onErr Error e "Stream handling failed" >> cleanStream p)

          ----------------------------------------------------------------
          -- Handle Commands
          ----------------------------------------------------------------
          handleCmd :: PollT ()
          handleCmd xp = do
            m <- psMap xp
            c <- psCmd xp
            (_, ps) <- psPolls xp
            case c of
              Z.S s _ -> do 
                x <- B.unpack <$> Z.receive s []
                case x of
                  "send" -> cmdSend s Streamer {strmSrc  = Nothing,
                                                strmIdx  = m,
                                                strmCmd  = c,
                                                strmPoll = ps}
                  "stop" -> cmdStop ps >> setPsContinue False xp
                  "pause" -> cmdPause s
                  "resume" -> return () -- ignore
                  "test" -> putStrLn "test successful"
                  _      -> undefined
              _ -> throwIO $ Ouch "Ouch! Not a poller in handleCmd!"

  ------------------------------------------------------------------------
  -- Simplify parameter passing in streams
  ------------------------------------------------------------------------
  data PollState = PollSt {
                     polMap  :: Map Identifier Z.Poll,
                     polCmd  :: Z.Poll,
                     polIs   :: [Identifier],
                     polPs   :: [Z.Poll],
                     polTmo  :: Timeout,
                     polCont :: Bool}

  type PollT r = MVar PollState -> IO r

  psPolls :: PollT ([Identifier], [Z.Poll])
  psPolls m = withMVar m $ \p -> return (polIs p, polPs p)

  psMap :: PollT (Map Identifier Z.Poll)
  psMap m = withMVar m (return . polMap)

  psCmd :: PollT Z.Poll
  psCmd m = withMVar m (return . polCmd)

  psContinue :: PollT Bool
  psContinue m = withMVar m (return . polCont)

  setPsContinue :: Bool -> PollT ()
  setPsContinue t m = modifyMVar_ m $ \p -> return p{polCont = t}

  ------------------------------------------------------------------------
  -- Get first stream with input
  ------------------------------------------------------------------------
  getStream :: [Identifier] -> [Z.Poll] -> Maybe (Identifier, Z.Poll)
  getStream _ [] = Nothing
  getStream [] _ = Nothing
  getStream (i:is) (p:ps) = case p of
                              Z.S _ Z.In -> Just (i, p)
                              _          -> getStream is ps

  ------------------------------------------------------------------------
  -- Receive from poll entry
  ------------------------------------------------------------------------
  readPoll :: Z.Poll -> Source
  readPoll p = case p of
                 Z.S s _ -> recv s
                 _       -> liftIO $ throwIO $ Ouch "Ouch! No socket in poll"

  ------------------------------------------------------------------------
  -- Remove all messages from stream
  ------------------------------------------------------------------------
  cleanStream :: Z.Poll -> IO ()
  cleanStream p = case p of
                    Z.S s _ -> go s
                    _       -> return ()
    where go s = do
            m <- Z.moreToReceive s
            when m $ Z.receive s [] >>= \_ -> go s

  ------------------------------------------------------------------------
  -- Traditional receive
  ------------------------------------------------------------------------
  recv :: Z.Socket a -> Source 
  recv s = liftIO (Z.receive s []) >>= \x -> do
           C.yield x
           m <- liftIO $ Z.moreToReceive s
           when m $ recv s

  ------------------------------------------------------------------------
  -- Receive with tmo
  ------------------------------------------------------------------------
  recvTmo :: Z.Socket a -> Z.Timeout -> Source
  recvTmo s tmo =  liftIO (Z.poll [Z.S s Z.In] tmo) >>= \[s'] ->
    case s' of
      Z.S _ Z.In -> recv s 
      _          -> return ()

  ------------------------------------------------------------------------
  -- | Receiver Sink
  ------------------------------------------------------------------------
  runReceiver :: Z.Socket a -> Z.Timeout -> SinkR o -> IO o
  runReceiver s tmo snk = C.runResourceT $ recvTmo s tmo $$ snk

  ------------------------------------------------------------------------
  -- | Sender Source
  ------------------------------------------------------------------------
  runSender :: Z.Socket a -> Source -> IO ()
  runSender s src = C.runResourceT $ src $$ relay s

  ------------------------------------------------------------------------
  -- Create cmdName
  ------------------------------------------------------------------------
  cmdName :: Service -> IO String
  cmdName sn = do
    u <- show <$> mkUniqueId
    return $ "inproc://_" ++ sn ++ "_" ++ u
            
  ------------------------------------------------------------------------
  -- Creates a socket, binds or links it and sets the socket options
  ------------------------------------------------------------------------
  access :: Context -> AccessType -> LinkType -> [Z.SocketOption] ->
            String  -> [Service]  -> IO Z.Poll
  access ctx a l os u ts = 
    case a of 
      ServerT -> Z.socket ctx Z.Rep  >>= go
      ClientT -> Z.socket ctx Z.Req  >>= go
      DealerT -> Z.socket ctx Z.XReq >>= go
      RouterT -> Z.socket ctx Z.XRep >>= go
      PubT    -> Z.socket ctx Z.Pub  >>= go
      PipeT   -> Z.socket ctx Z.Push >>= go
      PullT   -> Z.socket ctx Z.Pull >>= go
      PeerT   -> Z.socket ctx Z.Pair >>= go
      SubT    -> Z.socket ctx Z.Sub  >>= \s -> 
                   mapM_ (Z.subscribe s) ts >> go s
   where go s = do setSockOs s os
                   case l of
                     Bind    -> Z.bind s u
                     Connect -> trycon s u retries
                   return $ Z.S s Z.In

  ------------------------------------------------------------------------
  -- creates and binds or connects all sockets recursively;
  -- on its way, creates the Map from Identifiers to PollItems,
  --             a list of PollItems
  --             and a list of Identifiers with the same order;
  -- finally executes "run"
  ------------------------------------------------------------------------
  mkPoll :: Context               -> 
            [PollEntry]           -> 
            Map Identifier Z.Poll ->
            [Identifier]          ->
            [Z.Poll]              -> IO (Map Identifier Z.Poll, 
                                         [Identifier], [Z.Poll])
  mkPoll _   []     m is ps = return (m, is, ps)
  mkPoll ctx (k:ks) m is ps = bracketOnError
    (access ctx (pollType k)
                (pollLink k) 
                (pollOs   k) 
                (pollAdd  k) 
                (pollSub  k))
    (\p -> closeS p >> return (m, [], []))
    (\p -> mkPoll ctx ks (Map.insert (pollId k) p m) 
                         (pollId k:is) (p:ps)) 

  ------------------------------------------------------------------------
  -- Close socket in a poll entry
  ------------------------------------------------------------------------
  closeS :: Z.Poll -> IO ()
  closeS p = case p of 
               Z.S s _ -> safeClose s
               _       -> return ()

  ------------------------------------------------------------------------
  -- safely close a socket 
  ------------------------------------------------------------------------
  safeClose :: Z.Socket a -> IO ()
  safeClose s = catch (Z.close s)
                      (\e -> let _ = (e::SomeException)
                              in return ())

  ------------------------------------------------------------------------
  -- handle stop command
  ------------------------------------------------------------------------
  cmdStop :: [Z.Poll] -> IO ()
  cmdStop = mapM_ closeS 

  ------------------------------------------------------------------------
  -- handle pause command
  ------------------------------------------------------------------------
  cmdPause :: Z.Socket a -> IO ()
  cmdPause s = do
    x <- B.unpack <$> Z.receive s []
    case x of
      "resume" -> return ()
      _        -> cmdPause s

  ------------------------------------------------------------------------
  -- handle send command
  ------------------------------------------------------------------------
  cmdSend :: Z.Socket a -> Streamer -> IO ()
  cmdSend cmd s = do
    ok <- Z.moreToReceive cmd
    if ok then do ds  <- getDest cmd []
                  C.runResourceT $ recv cmd $$ passAll s ds
          else throwIO $ ProtocolExc "Abrupt end of send command"

  ------------------------------------------------------------------------
  -- get destination to send to
  ------------------------------------------------------------------------
  getDest :: Z.Socket a -> [Identifier] -> IO [Identifier]
  getDest cmd is = do
    i <- B.unpack <$> Z.receive cmd []
    if null i then if null is then throwIO $ ProtocolExc "No identifiers" 
                              else return is
              else do ok <- Z.moreToReceive cmd
                      if ok then getDest cmd (i:is)
                            else throwIO $ ProtocolExc 
                                   "Incomplete identifier list"

  ------------------------------------------------------------------------
  -- relay stream to a socket
  ------------------------------------------------------------------------
  relay :: Z.Socket a -> Sink
  relay s = do
    mbX <- C.await
    case mbX of
      Nothing -> return ()
      Just x  -> go x
     where go x = do
             mbN <- C.await 
             case mbN of
               Nothing -> liftIO (Z.send s x [])
               Just n  -> liftIO (Z.send s x [Z.SndMore]) >> go n
  
  ------------------------------------------------------------------------
  -- send one message to many streams
  ------------------------------------------------------------------------
  multiSend :: [(Identifier, Z.Poll)] -> B.ByteString -> [Z.Flag] -> IO ()
  multiSend ps m os = go ps
    where go []         = return ()
          go ((_,p):pp) = sndPoll p >> go pp
          sndPoll p = case p of
                        Z.S s _ -> Z.send s m os
                        _       -> throwIO $ Ouch "Ouch! Not a Poll!"

  ------------------------------------------------------------------------
  -- Find sockets corresponding to identifiers
  ------------------------------------------------------------------------
  idsToSocks :: Streamer -> [Identifier] -> Either String [(Identifier, Z.Poll)]
  idsToSocks s = getSocks 
    where getSock i | i == internal = Right $ strmCmd s
                    | otherwise     =
            case Map.lookup i (strmIdx s) of
              Nothing -> Left $ "Unknown identifier" ++ i 
              Just p  -> Right p
          getSocks []     = Right []
          getSocks (i:is) = 
            case getSock i of
              Left  e -> Left e
              Right p -> 
                case getSocks is of
                  Left  e  -> Left e
                  Right ps -> Right ((i,p):ps)
       
  ------------------------------------------------------------------------
  -- | Holds information on streams and the current state of the device;
  --   streamers are passed to transformers.
  ------------------------------------------------------------------------
  data Streamer = Streamer {
                     strmSrc  :: Maybe (Identifier, Z.Poll),
                     strmIdx  :: Map Identifier Z.Poll,
                     strmCmd  :: Z.Poll,
                     strmPoll :: [Z.Poll]}

  ------------------------------------------------------------------------
  -- | Conduit with Streamer
  ------------------------------------------------------------------------
  type StreamConduit = Streamer -> Conduit B.ByteString ()

  ------------------------------------------------------------------------
  -- | Sink with Streamer
  ------------------------------------------------------------------------
  type StreamSink    = Streamer -> Sink

  ------------------------------------------------------------------------
  -- | IO Action with Streamer (Timeout)
  ------------------------------------------------------------------------
  type StreamAction  = Streamer -> IO ()

  ------------------------------------------------------------------------
  -- | Get current source
  ------------------------------------------------------------------------
  getSource :: Streamer -> Identifier
  getSource s = case strmSrc s of
                  Nothing    -> ""
                  Just (i,_) -> i

  ------------------------------------------------------------------------
  -- | Filter subset of streams; usually you want to filter
  --   a subset of streams to relay an incoming stream.
  ------------------------------------------------------------------------
  filterStreams :: Streamer -> (Identifier -> Bool) -> [Identifier]
  filterStreams s p = map fst $ Map.toList $ 
                                Map.filterWithKey (\k _ -> p k) $ strmIdx s

  ------------------------------------------------------------------------
  -- | Pass all segments of an incoming stream to a list of outgoing streams
  ------------------------------------------------------------------------
  passAll :: Streamer -> [Identifier] -> Sink
  passAll s ds = 
    case idsToSocks s ds of 
      Left e   -> liftIO $ throwIO $ ProtocolExc e 
      Right ss -> do mbI <- C.await -- C.awaitForever $ \i -> do
                     case mbI of
                       Nothing -> return ()
                       Just i  -> go ss i
    where go ss i = do 
            mbN <- C.await
            case mbN of
              Nothing -> liftIO (multiSend ss i [])
              Just n  -> liftIO (multiSend ss i [Z.SndMore]) >> go ss n

  ------------------------------------------------------------------------
  -- | Pass one segment and ignore the remainder of the stream
  ------------------------------------------------------------------------
  pass1 :: Streamer -> [Identifier] -> Sink
  pass1 s ds = 
    case idsToSocks s ds of
      Left e   -> liftIO $ throwIO $ ProtocolExc e 
      Right ss -> do
        mbX <- C.await 
        case mbX of
          Nothing -> return ()
          Just x  -> liftIO (multiSend ss x []) >> ignoreStream

  ------------------------------------------------------------------------
  -- | Ignore an incoming stream
  ------------------------------------------------------------------------
  ignoreStream :: Sink
  ignoreStream = do mb <- C.await
                    case mb of 
                      Nothing -> return ()
                      Just _  -> ignoreStream

  ------------------------------------------------------------------------
  -- | Send the segments to a list of outgoing streams
  --   without terminating the stream
  ------------------------------------------------------------------------
  part :: Streamer -> [Identifier] -> [B.ByteString] -> Sink
  part s ds ms = 
    case idsToSocks s ds of
      Left  e  -> liftIO $ throwIO $ ProtocolExc e
      Right ss -> go ss
    where go ss = liftIO $ mapM_ (\x -> multiSend ss x [Z.SndMore]) ms

  ------------------------------------------------------------------------
  -- | Send the segments to a list of outgoing streams
  --   terminating the stream
  ------------------------------------------------------------------------
  stream :: Streamer -> [Identifier] -> [B.ByteString] -> Sink
  stream s ds ms = 
    case idsToSocks s ds of
      Left e   -> liftIO $ throwIO $ ProtocolExc e 
      Right ss -> go ms ss
    where go [] _      = return ()
          go [x] ss    = liftIO (multiSend ss x [])
          go (x:xs) ss = liftIO (multiSend ss x [Z.SndMore]) >> go xs ss

  ------------------------------------------------------------------------
  -- | The internal stream represented by the 'Controller'
  ------------------------------------------------------------------------
  internal :: String
  internal = "_internal" 

  ------------------------------------------------------------------------
  -- | Controller
  ------------------------------------------------------------------------
  data Controller = Controller {
                      ctrlCtx  :: Context,
                      ctrlCmd  :: Z.Socket Z.XReq,
                      ctrlOpts :: [Z.SocketOption]}

  ------------------------------------------------------------------------
  -- | Control Action
  ------------------------------------------------------------------------
  type Control a = Controller -> IO a

  ------------------------------------------------------------------------
  -- Get the socket from a controller and send a string to it
  ------------------------------------------------------------------------
  sndCmd :: String -> Controller -> IO ()
  sndCmd cmd ctrl = let s = ctrlCmd ctrl
                     in Z.send s (B.pack cmd) []

  ------------------------------------------------------------------------
  -- | Stop streams
  ------------------------------------------------------------------------
  stop :: Controller -> IO ()
  stop = sndCmd "stop"

  ------------------------------------------------------------------------
  -- | Pause streams
  ------------------------------------------------------------------------
  pause :: Controller -> IO ()
  pause = sndCmd "pause"

  ------------------------------------------------------------------------
  -- | Resume streams
  ------------------------------------------------------------------------
  resume :: Controller -> IO ()
  resume = sndCmd "resume"

  {- ????
  ignore :: Controller -> [Identifier] -> IO ()
  ignore c is = return ()

  attend :: Controller -> [Identifier] -> IO ()
  attend c is = return ()

  rmStream :: Identifier -> [Identifier] -> [Z.Poll] -> ([Identifier], [Z.Poll])
  rmStream _ [] ps = ([], [])
  rmStream _ is [] = ([], [])
  rmStream x (i:is) (p:ps) | x == i    = (is, ps)
                           | otherwise = (i : rmStream is, p : rmStream ps)

  -}

  ------------------------------------------------------------------------
  -- | Receive a stream through the controller
  ------------------------------------------------------------------------
  receive :: Controller -> Timeout -> SinkR (Maybe a) -> IO (Maybe a)
  receive c tmo snk = C.runResourceT $ recvTmo (ctrlCmd c) tmo $$ snk

  ------------------------------------------------------------------------
  -- | Send a stream through the controller
  ------------------------------------------------------------------------
  send :: Controller -> [Identifier] -> Source -> IO ()
  send c is src = 
    let s = ctrlCmd c
     in do Z.send s (B.pack "send") [Z.SndMore]
           sendIds s
           C.runResourceT $ src $$ relay s
    where sendIds s  = do mapM_ (sendId s) is
                          Z.send s B.empty [Z.SndMore]
          sendId s i = Z.send s (B.pack i) [Z.SndMore]

  ------------------------------------------------------------------------
  -- | Defines the type of a 'PollEntry';
  --   the names of the constructors are similar to ZMQ socket types
  --   but with some differences to keep the terminology in line
  --   with basic patterns.
  --   The leading \"X\" stands for \"Access\" 
  --   (not for \"eXtended\" as in XRep and XReq).
  ------------------------------------------------------------------------
  data AccessType = 
         -- | Represents a server and expects connections from clients;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.Rep'
         ServerT    
         -- | Represents a client and connects to a server;
         --   should be used with 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.Req'
         | ClientT
         -- | Represents a load balancer, 
         --   expecting connections from clients;
         --   may be used with 'Bind' or 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.XRep'
         | RouterT 
         -- | Represents a router
         --   expecting connections from servers;
         --   may be used with 'Bind' or 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.XReq'
         | DealerT 
         -- | Represents a publisher;
         --   should be used with 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.Pub'
         | PubT    
         -- | Represents a subscriber;
         --   should be used with 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.Sub'
         | SubT    
         -- | Represents a Pipe;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.Push'
         | PipeT
         -- | Represents a Puller;
         --   should be used with 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.Pull'
         | PullT
         -- | Represents a Peer;
         --   corresponding peers must use complementing 'LinkType';
         --   corresponds to ZMQ Socket Type 'Z.Pair'
         | PeerT   
    deriving (Eq, Show, Read)

  ------------------------------------------------------------------------
  -- | Safely read 'AccessType';
  --   ignores the case of the input string
  --   (/e.g./ \"servert\" -> 'ServerT')
  ------------------------------------------------------------------------
  parseAccess :: String -> Maybe AccessType
  parseAccess s = case map toLower s of
                    "servert" -> Just ServerT    
                    "clientt" -> Just ClientT
                    "routert" -> Just RouterT 
                    "dealert" -> Just DealerT 
                    "pubt"    -> Just PubT    
                    "subt"    -> Just SubT    
                    "pipet"   -> Just PipeT
                    "pullt"   -> Just PullT
                    "peert"   -> Just PeerT
                    _         -> Nothing

  ------------------------------------------------------------------------
  -- | A poll entry describes how to handle how to access a stream
  ------------------------------------------------------------------------
  data PollEntry = Poll {
                     pollId   :: Identifier,
                     pollAdd  :: String,
                     pollType :: AccessType,
                     pollLink :: LinkType,
                     pollSub  :: [Service],
                     pollOs   :: [Z.SocketOption]
                   }
    deriving (Show, Read)

  instance Eq PollEntry where
    x == y = pollId x == pollId y

  -------------------------------------------------------------------------
  -- | Safely read 'LinkType';
  --   ignores the case of the input string
  --   and, besides \"bind\" and \"connect\", 
  --   also accepts \"bin\", \"con\" and \"conn\";
  --   intended for use with command line parameters
  -------------------------------------------------------------------------
  parseLink :: String -> Maybe LinkType 
  parseLink s = case map toLower s of
                  "bind"    -> Just Bind
                  "bin"     -> Just Bind
                  "b"       -> Just Bind
                  "c"       -> Just Connect
                  "con"     -> Just Connect
                  "conn"    -> Just Connect
                  "connect" -> Just Connect
                  _         -> Nothing

  -------------------------------------------------------------------------
  -- | Binds or connects a socket to an address
  -------------------------------------------------------------------------
  link :: LinkType -> Z.Socket a -> String -> [Z.SocketOption] -> IO ()
  link t s add os = do setSockOs s os
                       case t of
                         Bind    -> Z.bind s add 
                         Connect -> trycon s add 10

  ------------------------------------------------------------------------
  -- some helpers
  ------------------------------------------------------------------------
  retries :: Int
  retries = 100

  ------------------------------------------------------------------------
  -- try n times to connect
  -- this is particularly useful for "inproc" sockets:
  -- the socket, in this case, must be bound before we can connect to it.
  ------------------------------------------------------------------------
  trycon :: Z.Socket a -> String -> Int -> IO ()
  trycon sock add i = catch (Z.connect sock add) 
                            (\e -> if i <= 0 
                                     then throwIO (e::SomeException)
                                     else do
                                       threadDelay 1000
                                       trycon sock add (i-1))

  -------------------------------------------------------------------------
  -- Sets Socket Options
  -------------------------------------------------------------------------
  setSockOs :: Z.Socket a -> [Z.SocketOption] -> IO ()
  setSockOs s = mapM_ (Z.setOption s)

