{-# LANGUAGE CPP, DeriveDataTypeable, RankNTypes #-}
module Network.Mom.Patterns.Streams.Streams
where

  import           Control.Monad.Trans (liftIO)
  import           Control.Monad (unless, when)
  import           Control.Applicative ((<$>))
  import           Prelude hiding (catch)
  import           Control.Exception (Exception, SomeException, 
                                      bracket, bracketOnError, finally,
                                      catch, try, throwIO)
  import           Control.Concurrent
  import           Data.Conduit (($$), ($=), (=$=))
  import           Data.Typeable (Typeable)
  import qualified Data.Conduit          as C
  import qualified Data.Conduit.List     as CL
  import qualified Data.ByteString.Char8 as B
  import           Data.Char (toLower)
  import           Data.List (intercalate)
  import           Data.List.Split (endBy)
  import           Data.Map (Map)
  import qualified Data.Map as Map
  import qualified System.ZMQ            as Z

  import           Factory
  import           Network.Mom.Patterns.Streams.Types

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
            m <- psMap xp
            c <- psCmd xp
            (is, ps) <- psPolls xp

            (x:ss)   <- Z.poll (c:ps) tmo
            case x of
              Z.S cmd Z.In -> do catch (handleCmd xp)
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
                                   (onStream Streamer {
                                               strmSrc  = Just (i, p),
                                               strmIdx  = m,
                                               strmCmd  = c,
                                               strmPoll = ps})) (
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
                  "addstream" -> cmdAddStream ctx s xp
                  "remove"    -> cmdRemove xp
                  "test" -> putStrLn "test successful"
                  _      -> undefined
              _ -> throwIO $ Ouch "Ouch! Not a poller in handleCmd!"

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

  psTmo :: PollT Timeout
  psTmo m = withMVar m (return . polTmo)

  psContinue :: PollT Bool
  psContinue m = withMVar m (return . polCont)

  setPsContinue :: Bool -> PollT ()
  setPsContinue t m = modifyMVar_ m $ \p -> return p{polCont = t}

  getStream :: [Identifier] -> [Z.Poll] -> Maybe (Identifier, Z.Poll)
  getStream _ [] = Nothing
  getStream [] _ = Nothing
  getStream (i:is) (p:ps) = case p of
                              Z.S _ Z.In -> Just (i, p)
                              _          -> getStream is ps

  readPoll :: Z.Poll -> Source
  readPoll p = case p of
                 Z.S s _ -> recv s
                 _       -> liftIO $ throwIO $ Ouch "Ouch! No socket in poll"

  cleanStream :: Z.Poll -> IO ()
  cleanStream p = case p of
                    Z.S s _ -> go s
                    _       -> return ()
    where go s = do
            m <- Z.moreToReceive s
            when m $ Z.receive s [] >>= \_ -> go s

  recv :: Z.Socket a -> Source 
  recv s = liftIO (Z.receive s []) >>= \x -> do
           C.yield x
           m <- liftIO $ Z.moreToReceive s
           when m $ recv s

  recvTmo :: Z.Socket a -> Z.Timeout -> Source
  recvTmo s tmo =  liftIO (Z.poll [Z.S s Z.In] tmo) >>= \[s'] ->
    case s' of
      Z.S _ Z.In -> recv s 
      _          -> return ()

  runReceiver :: Z.Socket a -> Z.Timeout -> SinkR o -> IO o
  runReceiver s tmo snk = C.runResourceT $ recvTmo s tmo $$ snk

  runSender :: Z.Socket a -> Source -> IO ()
  runSender s src = C.runResourceT $ src $$ relay s

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

  closeS :: Z.Poll -> IO ()
  closeS p = case p of 
               Z.S s _ -> safeClose s
               _       -> return ()

  safeClose :: Z.Socket a -> IO ()
  safeClose s = catch (Z.close s)
                      (\e -> let _ = (e::SomeException)
                              in return ())

  cmdStop :: [Z.Poll] -> IO ()
  cmdStop = mapM_ closeS 

  cmdPause :: Z.Socket a -> IO ()
  cmdPause s = do
    x <- B.unpack <$> Z.receive s []
    case x of
      "resume" -> return ()
      _        -> cmdPause s

  cmdSend :: Z.Socket a -> Streamer -> IO ()
  cmdSend cmd s = do
    ok <- Z.moreToReceive cmd
    if ok then do ds  <- getDest cmd []
                  C.runResourceT $ recv cmd $$ passAll s ds
          else throwIO $ ProtocolExc "Abrupt end of send command"

  cmdAddStream :: Context -> Z.Socket a -> PollT ()
  cmdAddStream ctx cmd xp = catch (do
      ok <- Z.moreToReceive cmd
      if not ok then throwIO $ ProtocolExc "Abrupt end of addStream command"
        else do 

          i  <- B.unpack <$> identifier 
          a  <- B.unpack <$> address
          mA <- (parseAccess . B.unpack)  <$> actype
          mL <- (parseLink   . B.unpack)  <$> linktype
          ts <- (endBy ","   . B.unpack)  <$> topics
          os <- (map read . endBy "," . B.unpack) <$> options -- no safe read!

          at <- case mA of
            Nothing -> throwIO $ ProtocolExc $ 
                         "addStream: Unknown access type: '" ++ show mA ++ "'"
            Just x  -> return x
          lt <- case mL of
            Nothing -> throwIO $ ProtocolExc $
                         "addStream: Unknown link type: '" ++ show mL ++ "'"
            Just x  -> return x

          m        <- psMap xp
          (is, ps) <- psPolls xp
          (m', is', ps') <- mkPoll ctx [Poll i a at lt ts os] m is ps

          modifyMVar_ xp $ \p -> 
            return p{polMap = m',
                     polIs  = is',
                     polPs  = ps'}

          Z.send cmd B.empty []
        )
        (\e -> do Z.send cmd (B.pack "error") [Z.SndMore]
                  Z.send cmd (B.pack $ show (e::SomeException)) [])
      
    where identifier = getChunk cmd "addStream: No identifiers"
          address    = getChunk cmd "addStream: No socket address"
          actype     = getChunk cmd "addStream: No access type"
          linktype   = getChunk cmd "addStream: No link type"
          topics     = getChunk cmd "addStream: No topics"
          options    = getChunk cmd "addStream: No options"

  cmdRemove :: Streamer -> PollT ()
  cmdRemove = undefined
  --  case idsToSock 
  --   i <- B.unpack <$> identifier

  rmStream :: Identifier -> [Identifier] -> [Z.Poll] -> ([Identifier], [Z.Poll])
  rmStream _ [] ps = ([], [])
  rmStream _ is [] = ([], [])
  rmStream x (i:is) (p:ps) | x == i    = (is, ps)
                           | otherwise = (i : rmStream is, p : rmStream ps)

  getChunk :: Z.Socket a -> String -> IO B.ByteString
  getChunk s e = do
    ok <- Z.moreToReceive s
    if not ok 
      then throwIO $ ProtocolExc e -- send response instead of exc
      else Z.receive s []

  getDest :: Z.Socket a -> [Identifier] -> IO [Identifier]
  getDest cmd is = do
    i <- B.unpack <$> Z.receive cmd []
    if null i then if null is then throwIO $ ProtocolExc "No identifiers" 
                              else return is
              else do ok <- Z.moreToReceive cmd
                      if ok then getDest cmd (i:is)
                            else throwIO $ ProtocolExc 
                                   "Incomplete identifier list"
                      
  data More = More | NoMore
    deriving (Show)

  more :: More -> Bool
  more More = True
  more _    = False

  relay :: Z.Socket a -> Sink
  relay s = do
    mbX <- C.await
    case mbX of
      Nothing -> return ()
      Just x  -> do
        mbN <- C.await
        case mbN of
          Nothing -> liftIO (Z.send s x [])
          Just n  -> liftIO (Z.send s x [Z.SndMore]) >> go n
     where go x = do
             mbN <- C.await 
             case mbN of
               Nothing -> liftIO (Z.send s x [])
               Just n  -> liftIO (Z.send s x [Z.SndMore]) >> go n
  
  multiSend :: [(Identifier, Z.Poll)] -> B.ByteString -> More -> IO ()
  multiSend ps m t = go ps
    where go []         = return ()
          go ((i,p):pp) = sndPoll p >> go pp
          sndPoll p = case p of
                        Z.S s x -> Z.send s m flg
                        _       -> throwIO $ Ouch "Ouch! Not a Poll!"
          flg  = if more t then [Z.SndMore] else []  

  idsToSocks :: Streamer -> [Identifier] -> Either String [(Identifier, Z.Poll)]
  idsToSocks s ds = getSocks ds
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

  type StreamConduit = Streamer -> Conduit B.ByteString ()
  type StreamSink    = Streamer -> Sink
  type StreamAction  = Streamer -> IO ()

  getSource :: Streamer -> Identifier
  getSource s = case strmSrc s of
                  Nothing    -> ""
                  Just (i,_) -> i

  filterStreams :: Streamer -> (Identifier -> Bool) -> [Identifier]
  filterStreams s p = map fst $ Map.toList $ 
                                Map.filterWithKey (\k _ -> p k) $ strmIdx s

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
              Nothing -> liftIO (multiSend ss i NoMore)
              Just n  -> liftIO (multiSend ss i   More) >> go ss n

  pass1 :: Streamer -> [Identifier] -> Sink
  pass1 s ds = 
    case idsToSocks s ds of
      Left e   -> liftIO $ throwIO $ ProtocolExc e 
      Right ss -> do
        mbX <- C.await 
        case mbX of
          Nothing -> return ()
          Just x  -> liftIO (multiSend ss x NoMore) >> ignoreStream

  passPart :: Streamer -> [Identifier] -> Sink
  passPart s ds = 
    case idsToSocks s ds of
      Left  e  -> liftIO $ throwIO $ ProtocolExc e 
      Right ss -> go ss
    where go ss = C.awaitForever $ \i -> liftIO $ multiSend ss i More

  ignoreStream :: Sink
  ignoreStream = do mb <- C.await
                    case mb of 
                      Nothing -> return ()
                      Just _  -> ignoreStream

  part :: Streamer -> [Identifier] -> [B.ByteString] -> Sink
  part s ds ms = 
    case idsToSocks s ds of
      Left  e  -> liftIO $ throwIO $ ProtocolExc e
      Right ss -> go ss
    where go ss = liftIO $ mapM_ (\x -> multiSend ss x More) ms

  stream :: Streamer -> [Identifier] -> [B.ByteString] -> Sink
  stream s ds ms = 
    case idsToSocks s ds of
      Left e   -> liftIO $ throwIO $ ProtocolExc e 
      Right ss -> go ms ss
    where go [] _      = return ()
          go [x] ss    = liftIO (multiSend ss x NoMore)
          go (x:xs) ss = liftIO (multiSend ss x More) >> go xs ss

  ------------------------------------------------------------------------
  -- Controller
  ------------------------------------------------------------------------
  internal :: String
  internal = "_internal" 

  data Controller = Controller {
                      ctrlCtx  :: Context,
                      ctrlCmd  :: Z.Socket Z.XReq,
                      ctrlOpts :: [Z.SocketOption]}

  type Control a = Controller -> IO a

  sndCmd :: String -> Controller -> IO ()
  sndCmd cmd ctrl = let s = ctrlCmd ctrl
                     in Z.send s (B.pack cmd) []

  stop :: Controller -> IO ()
  stop = sndCmd "stop"

  pause :: Controller -> IO ()
  pause = sndCmd "pause"

  resume :: Controller -> IO ()
  resume = sndCmd "resume"

  remove :: Controller -> Identifier -> IO ()
  remove c i = do
    let s = ctrlCmd c
     in do Z.send s (B.pack "remove") [Z.SndMore]
           Z.send s (B.pack i) []

  ignore :: Controller -> [Identifier] -> IO ()
  ignore c is = return ()

  attend :: Controller -> [Identifier] -> IO ()
  attend c is = return ()
  
  changeTmo :: Controller -> IO ()
  changeTmo c = return ()

  send :: Controller -> [Identifier] -> Source -> IO ()
  send c is src = 
    let s = ctrlCmd c
     in do Z.send s (B.pack "send") [Z.SndMore]
           sendIds s
           C.runResourceT $ src $$ relay s
    where sendIds s  = do mapM_ (sendId s) is
                          Z.send s B.empty [Z.SndMore]
          sendId s i = Z.send s (B.pack i) [Z.SndMore]

  addStream :: Controller -> Identifier       ->
                             String           ->
                             AccessType       ->
                             LinkType         ->
                             [String]         ->
                             [Z.SocketOption] -> IO ()
  addStream c i a at lt ts os = 
    let s   = ctrlCmd c
        ts' = intercalate "," ts
        os' = intercalate "," $ map show os
     in do Z.send s (B.pack "addstream") [Z.SndMore]
           Z.send s (B.pack i)           [Z.SndMore]
           Z.send s (B.pack a)           [Z.SndMore]
           Z.send s (B.pack $ show at)   [Z.SndMore]
           Z.send s (B.pack $ show lt)   [Z.SndMore]
           Z.send s (B.pack ts')         [Z.SndMore]
           Z.send s (B.pack os')         []
           ok <- runReceiver s 10000 CL.consume
           case ok of
             [x] | B.null x  -> return ()
                 | otherwise -> 
                       throwIO $ ProtocolExc $ "Unknown result: " ++ show x
             []     -> throwIO $ ProtocolExc $ "Timeout waiting for ack"
             (x:xs) -> throwIO $ ProtocolExc $ 
                                    "Error processing command: " ++ show xs

  receive :: Controller -> Timeout -> SinkR (Maybe a) -> IO (Maybe a)
  receive c tmo snk = C.runResourceT $ recvTmo (ctrlCmd c) tmo $$ snk

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
  -- | A poll entry describes how to handle an 'AccessPoint'
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
  -- binds or connects to the address
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

  ------------------------------------------------------------------------
  -- either with the arguments flipped 
  ------------------------------------------------------------------------
  ifLeft :: Either a b -> (a -> c) -> (b -> c) -> c
  ifLeft e l r = either l r e
 
  -------------------------------------------------------------------------
  -- Sets Socket Options
  -------------------------------------------------------------------------
  setSockOs :: Z.Socket a -> [Z.SocketOption] -> IO ()
  setSockOs s = mapM_ (Z.setOption s)

