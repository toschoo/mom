module Service (
          Service, srvContext, srvName, srvId,
          stop, pause, resume, changeParam, changeOption,
          addDevice, remDevice, changeTimeout,
          withService, poll, xpoll, XPoll(..),
          periodic, periodicSend
          , Command(..), DevCmd(..) -- if test
          )
where

  import           Factory
  import           Types

  import qualified Data.ByteString.Char8 as B
  import           Data.Time.Clock
  import           Data.Map (Map)
  import qualified Data.Map as Map

  import           Control.Concurrent 
  import           Control.Applicative ((<$>))
  import           Control.Monad
  import           Prelude hiding (catch)
  import           Control.Exception (bracket, finally)
  import qualified System.ZMQ as Z

  ------------------------------------------------------------------------
  -- | Generic Service data type;
  --   'Service' is passed to application-defined actions
  --   used with background services, namely
  --   withServer, withPeriodicPub, withSub, withPuller and withDevice. 
  ------------------------------------------------------------------------
  data Service = Service {
                   srvCtx    :: Z.Context,
                   -- | Obtains the service name
                   srvName   :: String,
                   srvCmd    :: Z.Socket Z.Pub,
                   srvId     :: ThreadId
                 }

  ------------------------------------------------------------------------
  -- | Obtains the 'Z.Context' from 'Service'
  ------------------------------------------------------------------------
  srvContext :: Service -> Z.Context
  srvContext = srvCtx

  ------------------------------------------------------------------------
  -- Stops a service - used internally only
  ------------------------------------------------------------------------
  stop  :: Service -> IO ()
  stop = sendCmd STOP

  ------------------------------------------------------------------------
  -- | Pauses the 'Service'
  ------------------------------------------------------------------------
  pause :: Service -> IO ()
  pause = sendCmd PAUSE

  ------------------------------------------------------------------------
  -- | Resumes the 'Service'
  ------------------------------------------------------------------------
  resume :: Service -> IO ()
  resume = sendCmd RESUME

  ------------------------------------------------------------------------
  -- | Changes the 'Service' control parameter
  ------------------------------------------------------------------------
  changeParam :: Service -> Parameter -> IO ()
  changeParam s c = sendCmd (APP c) s

  ------------------------------------------------------------------------
  -- | Changes SocketOption
  ------------------------------------------------------------------------
  changeOption :: Service -> Z.SocketOption -> IO ()
  changeOption s o = sendCmd (OPT o) s

  ------------------------------------------------------------------------
  -- | Adds a 'PollEntry' to a device;
  --   the 'Service', of course, must be a device, 
  --   the command is otherwise ignored.
  ------------------------------------------------------------------------
  addDevice  :: Service -> PollEntry -> IO ()
  addDevice s p = sendDevCmd (ADD p) s

  ------------------------------------------------------------------------
  -- | Removes a 'PollEntry' from a device;
  --   the 'Service', of course, must be a device, 
  --   the command is otherwise ignored.
  ------------------------------------------------------------------------
  remDevice :: Service -> Identifier -> IO ()
  remDevice s i = sendDevCmd (REM i) s

  ------------------------------------------------------------------------
  -- | Changes the timeout of a device;
  --   the 'Service', of course, must be a device,
  --   the command is otherwise ignored.
  ------------------------------------------------------------------------
  changeTimeout :: Service -> Timeout -> IO ()
  changeTimeout s t = sendDevCmd (TMO t) s

  ------------------------------------------------------------------------
  -- Service commands
  ------------------------------------------------------------------------
  data Command = STOP | PAUSE  | RESUME 
               | DEVICE DevCmd      -- device specific commands
               | APP String         -- change control parameter 
               | OPT Z.SocketOption -- change socket option
    deriving (Eq, Show, Read)

  ------------------------------------------------------------------------
  -- Device-specific commands
  ------------------------------------------------------------------------
  data DevCmd = ADD PollEntry | REM Identifier | TMO Z.Timeout
    deriving (Eq, Show, Read)

  ------------------------------------------------------------------------
  -- Send a command
  ------------------------------------------------------------------------
  sendCmd :: Command -> Service -> IO ()
  sendCmd c s = Z.send (srvCmd s) (B.pack $ show c) []

  ------------------------------------------------------------------------
  -- Send device-specific command
  ------------------------------------------------------------------------
  sendDevCmd :: DevCmd -> Service -> IO ()
  sendDevCmd d = sendCmd (DEVICE d)

  ------------------------------------------------------------------------
  -- Parse a command string
  ------------------------------------------------------------------------
  readCmd :: String -> Either String Command
  readCmd s = case s of
               "STOP"   -> Right STOP
               "PAUSE"  -> Right PAUSE
               "RESUME" -> Right RESUME
               x        -> 
                 if take 4 x == "APP " 
                   then Right $ read x
                   else 
                     if take 4 x == "OPT "
                       then Right $ read x
                       else 
                         if take 6 x == "DEVICE"
                           then Right $ read x
                           else Left  $ "No Command: " ++ x

  ------------------------------------------------------------------------
  -- The work horse behind "with*" services
  -- - starts the service in a separate thread and
  --   waits until it is ready
  -- - executes the control action
  -- - stops the service and waits for its termination
  ------------------------------------------------------------------------
  withService :: Z.Context -> String -> String -> 
                (Z.Context -> String -> String -> String -> IO () -> IO ()) ->
                (Service -> IO a) -> IO a
  withService ctx name param service action = do
    running <- newEmptyMVar
    ready   <- newEmptyMVar
    Z.withSocket ctx Z.Pub $ \cmd -> do
      sn <- ("inproc://srv_" ++) <$> show <$> mkUniqueId
      Z.bind cmd sn
      bracket (start sn cmd ready running) 
              (\s -> stop s >> takeMVar running)
              (doAction ready)
    where start sn cmd ready m = do
            let imReady = putMVar ready ()
            tid <- forkIO $ finally (service ctx name sn param imReady) 
                                    (putMVar m ())
            return $ Service ctx name cmd tid
          doAction ready srv = takeMVar ready >>= \_ -> action srv

  ------------------------------------------------------------------------
  -- Poll on a command socket and the service socket
  ------------------------------------------------------------------------
  poll :: Bool -> [Z.Poll] -> (String -> IO ()) -> String -> IO ()
  poll paused poller rcv param 
    | paused    = handleCmd paused poller rcv param
    | otherwise = do
        [c, s] <- Z.poll poller (-1)
        case c of 
          Z.S _ Z.In -> handleCmd paused poller rcv param
          _          -> 
            case s of
              Z.S _ Z.In -> rcv param >> poll paused poller rcv param
              _          ->              poll paused poller rcv param

  ------------------------------------------------------------------------
  -- Handle a message received on the command socket
  ------------------------------------------------------------------------
  handleCmd :: Bool -> [Z.Poll] -> (String -> IO ()) -> String -> IO ()
  handleCmd paused poller@[Z.S sock _, _] rcv param = do
    x <- Z.receive sock []
    case readCmd $ B.unpack x of
      Left  _   -> poll paused poller rcv param -- ignore
      Right cmd -> case cmd of
                     STOP   -> return ()
                     PAUSE  -> poll True   poller rcv param
                     RESUME -> poll False  poller rcv param
                     APP p  -> poll paused poller rcv p
                     OPT o  -> changeOpt poller o >> 
                               poll paused poller rcv param
                     _      -> poll paused poller rcv param -- ignore
  handleCmd _ _ _ _ = ouch "invalid poller in 'handleCmd'!"

  ------------------------------------------------------------------------
  -- Change a socket option
  ------------------------------------------------------------------------
  changeOpt :: [Z.Poll] -> Z.SocketOption -> IO ()
  changeOpt (_:Z.S s _:_) o = Z.setOption s o
  changeOpt _             _ = return ()

  ------------------------------------------------------------------------
  -- XPoll descriptor for device services
  ------------------------------------------------------------------------
  data XPoll = XPoll {
                 xpCtx   :: Z.Context,
                 xpTmo   :: Z.Timeout,
                 xpMap   :: Map Identifier Z.Poll,
                 xpIds   :: [Identifier],
                 xpPoll  :: [Z.Poll]
               }

  ------------------------------------------------------------------------
  -- Remove a poll entry
  ------------------------------------------------------------------------
  xpDelete :: Identifier -> XPoll -> XPoll
  xpDelete i xp = let (p:pp)     = xpPoll xp
                      (is, ps)   = go (xpIds xp) pp
                   in xp {xpMap  = Map.delete i $ xpMap xp,
                          xpIds  = is,
                          xpPoll = p:ps}
    where  go _        []     = ([], [])
           go []       _      = ([], [])
           go (d:ds) (p:ps) = 
             if i == d then              go ds ps
               else let (  ds',   ps') = go ds ps
                     in (d:ds', p:ps')

  ------------------------------------------------------------------------
  -- Polling for device-based services:
  -- - a command socket
  -- - and a set of device sockets
  ------------------------------------------------------------------------
  xpoll :: Bool -> MVar XPoll -> 
           (String -> IO ()) ->
           (Identifier -> Z.Poll -> String -> IO ()) -> String -> IO ()
  xpoll paused mxp ontmo rcv param 
    | paused    = handleCmdX paused mxp ontmo rcv param
    | otherwise = do
        xp     <- readMVar mxp
        (c:ss) <- Z.poll (xpPoll xp) (xpTmo xp)
        case c of 
          Z.S _ Z.In -> handleCmdX paused mxp ontmo rcv param
          _          -> go (xpIds xp) ss
    where go _      []     = ontmo param >>
                             xpoll paused mxp ontmo rcv param
          go (i:is) (s:ss) =
            case s of
              Z.S _ Z.In -> rcv i s param >>
                            xpoll paused mxp ontmo rcv param
              _          -> go is ss
          go _      _     = ouch "Invalid xpoll entries"

  ------------------------------------------------------------------------
  -- Handle messages received through the command socket
  -- of a device service
  ------------------------------------------------------------------------
  handleCmdX :: Bool -> MVar XPoll    -> 
                (String -> IO ())     -> 
                (Identifier -> Z.Poll -> String -> IO ()) -> String -> IO ()
  handleCmdX paused mxp ontmo rcv param = do
    xp <- readMVar mxp
    case xpPoll xp of
      (Z.S sock _ : _) -> do
        x <- Z.receive sock []
        case readCmd $ B.unpack x of
          Left e    -> do putStrLn $ e ++ ": " ++ B.unpack x
                          xpoll paused mxp ontmo rcv param
          Right cmd -> case cmd of
                         STOP     -> return ()
                         PAUSE    -> xpoll True   mxp ontmo rcv param
                         RESUME   -> xpoll False  mxp ontmo rcv param
                         APP p    -> xpoll paused mxp ontmo rcv p
                         OPT _    -> xpoll paused mxp ontmo rcv param -- opt!
                         DEVICE d -> do modifyMVar_ mxp 
                                          (\_ -> handleDevCmd d xp)
                                        xpoll False mxp ontmo rcv param
      _ -> ouch "invalid poller in 'handleCmdX'!"

  ------------------------------------------------------------------------
  -- Handle a device command
  ------------------------------------------------------------------------
  handleDevCmd :: DevCmd -> XPoll -> IO XPoll
  handleDevCmd d xp = 
    case d of
      TMO t -> return   xp {xpTmo = t}
      REM i -> case Map.lookup i (xpMap xp) of
                 Just (Z.S s _) -> safeClose s >> return (xpDelete i xp)
                 _              -> return xp
      ADD p -> do
        s <- access (xpCtx   xp)
                    (pollType p) 
                    (pollLink p) 
                    (pollOs   p) 
                    (pollAdd  p) 
                    (pollSub  p)
        case xpPoll xp of
          (c:ss) -> do let i = pollId p
                       return xp {xpPoll = c:s:ss,
                                  xpIds  = i:xpIds xp,
                                  xpMap  = Map.insert i s $ xpMap xp}
          _      -> return xp

  ------------------------------------------------------------------------
  -- Publish periodically
  ------------------------------------------------------------------------
  periodicSend :: Bool -> Z.Timeout -> Z.Socket Z.Sub -> (String -> IO ()) -> String -> IO ()
  periodicSend paused period cmd send param = do
    release <- getCurrentTime
    periodicSend_ paused period release cmd send param

  periodicSend_ :: Bool -> Z.Timeout -> UTCTime -> Z.Socket Z.Sub -> (String -> IO ()) -> String -> IO ()
  periodicSend_ paused period release cmd send param
    | paused    = handleCmdSnd True period release cmd send param 
    | otherwise = send param >> handleCmdSnd paused period release cmd send param 

  ------------------------------------------------------------------------
  -- Poll on a publisher's command socket
  ------------------------------------------------------------------------
  handleCmdSnd :: Bool -> Z.Timeout -> UTCTime -> Z.Socket Z.Sub -> (String -> IO ()) -> String -> IO ()
  handleCmdSnd paused period release sock send param = do
    [Z.S _ evt] <- Z.poll [Z.S sock Z.In] 0
    case evt of 
      Z.In   -> do
        x        <- Z.receive sock []
        release' <- waitNext period release
        case readCmd $ B.unpack x of
          Left  _   -> periodicSend_ paused period release' sock send param
          Right cmd -> 
            case cmd of
              STOP   -> return ()
              PAUSE  -> periodicSend_ True   period release' sock send param
              RESUME -> periodicSend_ False  period release' sock send param
              APP p  -> periodicSend_ paused period release' sock send p
              _      -> periodicSend_ paused period release' sock send param
      _ -> do
        release' <- waitNext period release
        periodicSend_ paused period release' sock send param

  ------------------------------------------------------------------------
  -- Doing something periodically
  ------------------------------------------------------------------------
  periodic :: Z.Timeout -> IO () -> IO ()
  periodic period act = getCurrentTime                 >>= go
    where go release  = act >> waitNext period release >>= go 

  ------------------------------------------------------------------------
  -- Wait for the next release point
  ------------------------------------------------------------------------
  waitNext :: Z.Timeout -> UTCTime -> IO UTCTime
  waitNext period release = do
     now <- getCurrentTime
     let next = release `timeAdd` period
     if (now `timeAdd` 1) >= next
       then return now
       else do
         let sleepTime = next `diffUTCTime` now
         threadDelay (nominal2mu sleepTime)
         getCurrentTime

  ------------------------------------------------------------------------
  -- Some time processing helpers 
  ------------------------------------------------------------------------
  timeAdd :: UTCTime -> Z.Timeout -> UTCTime
  timeAdd t p = mu2nominal p `addUTCTime` t

  mu2nominal :: Z.Timeout -> NominalDiffTime
  mu2nominal m = (fromIntegral m / 1000000)::NominalDiffTime

  nominal2mu :: NominalDiffTime -> Int
  nominal2mu n = ceiling (n * (fromIntegral (1000000::Int)))
