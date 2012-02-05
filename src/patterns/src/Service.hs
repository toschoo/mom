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
  -- | Generic Service data type
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
  changeParam :: Service -> String -> IO ()
  changeParam s c = sendCmd (APP c) s

  ------------------------------------------------------------------------
  -- | Changes SocketOption
  ------------------------------------------------------------------------
  changeOption :: Service -> Z.SocketOption -> IO ()
  changeOption s o = sendCmd (OPT o) s

  ------------------------------------------------------------------------
  -- | Add a 'PollEntry' to a device;
  --   the 'Service' must be a device, of  course,
  --   the command is otherwise ignored.
  ------------------------------------------------------------------------
  addDevice  :: Service -> PollEntry -> IO ()
  addDevice s p = sendDevCmd (ADD p) s

  ------------------------------------------------------------------------
  -- | Remove a 'PollEntry' from a device;
  --   the 'Service' must be a device, of  course,
  --   the command is otherwise ignored.
  ------------------------------------------------------------------------
  remDevice :: Service -> Identifier -> IO ()
  remDevice s i = sendDevCmd (REM i) s

  ------------------------------------------------------------------------
  -- | Change the timeout of a device;
  --   the 'Service' must be a device, of  course,
  --   the command is otherwise ignored.
  ------------------------------------------------------------------------
  changeTimeout :: Service -> Timeout -> IO ()
  changeTimeout s t = sendDevCmd (TMO t) s

  data Command = STOP | PAUSE  | RESUME 
               | DEVICE DevCmd | APP String | OPT Z.SocketOption
    deriving (Eq, Show, Read)

  data DevCmd = ADD PollEntry | REM Identifier | TMO Z.Timeout
    deriving (Eq, Show, Read)

  sendCmd :: Command -> Service -> IO ()
  sendCmd c s = Z.send (srvCmd s) (B.pack $ show c) []

  sendDevCmd :: DevCmd -> Service -> IO ()
  sendDevCmd d = sendCmd (DEVICE d)

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

  withService :: Z.Context -> String -> String -> 
                (Z.Context -> String -> String -> String -> IO () -> IO ()) -> 
                (Service -> IO a) -> IO a
  withService ctx name param service action = do
    running <- newEmptyMVar
    ready   <- newEmptyMVar
    x <- Z.withSocket ctx Z.Pub $ \cmd -> do
           sn <- ("inproc://srv_" ++) <$> show <$> mkUniqueId
           Z.bind cmd sn
           bracket (start sn cmd ready running) stop (doAction ready)
    _ <- takeMVar running
    return x
    where start sn cmd ready m = do
            let imReady = putMVar ready ()
            tid <- forkIO $ (service ctx name sn param imReady) `finally` (putMVar m ())
            return $ Service ctx name cmd tid
          doAction ready srv = takeMVar ready >>= \_ -> action srv

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

  changeOpt :: [Z.Poll] -> Z.SocketOption -> IO ()
  changeOpt (_:Z.S s _:_) o = Z.setOption s o
  changeOpt _             _ = return ()

  data XPoll = XPoll {
                 xpCtx   :: Z.Context,
                 xpTmo   :: Z.Timeout,
                 xpMap   :: Map Identifier Z.Poll,
                 xpIds   :: [Identifier],
                 xpPoll  :: [Z.Poll]
               }

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
          go _      _     = error "Ouch!"

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
                         DEVICE d -> do modifyMVar_ mxp 
                                          (\_ -> handleDevCmd d xp)
                                        xpoll False mxp ontmo rcv param
      _ -> ouch "invalid poller in 'handleCmdX'!"

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

  periodicSend :: Bool -> Millisecond -> Z.Socket Z.Sub -> (String -> IO ()) -> String -> IO ()
  periodicSend paused period cmd send param = do
    release <- getCurrentTime
    periodicSend_ paused period release cmd send param

  periodicSend_ :: Bool -> Millisecond -> UTCTime -> Z.Socket Z.Sub -> (String -> IO ()) -> String -> IO ()
  periodicSend_ paused period release cmd send param
    | paused    = handleCmdSnd True period release cmd send param 
    | otherwise = send param >> handleCmdSnd paused period release cmd send param 

  handleCmdSnd :: Bool -> Millisecond -> UTCTime -> Z.Socket Z.Sub -> (String -> IO ()) -> String -> IO ()
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

  ouch :: String -> a
  ouch s = error $ "Ouch! You hit a bug, please report: " ++ s

  periodic :: Int -> IO () -> IO ()
  periodic period act = getCurrentTime                 >>= go
    where go release  = act >> waitNext period release >>= go 

  waitNext :: Int -> UTCTime -> IO UTCTime
  waitNext period release = do
     now <- getCurrentTime
     let next = release `timeAdd` period
     if (now `timeAdd` 10) >= next
       then return now
       else do
         let sleepTime = next `diffUTCTime` now
         threadDelay (1000 * nominal2ms sleepTime)
         getCurrentTime

  timeAdd :: UTCTime -> Int -> UTCTime
  timeAdd t p = ms2nominal p `addUTCTime` t

  ms2nominal :: Int -> NominalDiffTime
  ms2nominal m = fromIntegral m / (1000::NominalDiffTime)

  nominal2ms :: NominalDiffTime -> Int
  nominal2ms n = ceiling (n * (fromIntegral (1000::Int)))
