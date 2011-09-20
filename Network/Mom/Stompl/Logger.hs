module Logger (
          startLogger, logX,
          Log.Priority(..))
where

  -- Config:
  -- - Log File
  -- - Log Level
  -- - Log syslog (yes or no)
  
  import qualified System.Log.Logger as Log
  import System.Log.Handler.Simple
  import System.Log.Handler (setFormatter)
  import System.Log.Formatter

  import System.Time
  import System.Locale
  import Text.Printf

  import Control.Monad
  import Control.Monad.State

  import Types
  import Config

  picoPerMicro :: Integer
  picoPerMicro = 1000000000

  type Logger a = StateT Config IO a

  startLogger :: Config -> IO ()
  startLogger cfg = do
    updLogger cfg
    evalStateT runLogger cfg

  updLogger :: Config -> IO ()
  updLogger cfg = do
    let f = getLogFile  cfg
    let p = getLogLevel cfg
    h <- fileHandler f p >>= \h0 -> return $
           setFormatter h0 $ varFormatter [("time", showTime)] "[$time: $loggername : $prio] $msg"
    Log.updateGlobalLogger Log.rootLoggerName $ Log.setLevel p . Log.setHandlers [h]

  showTime :: IO String 
  showTime = do
    t <- getClockTime
    c <- toCalendarTime t 
    let p   = (ctPicosec c) `div` picoPerMicro
    let frm = "%Y-%m-%d %H:%M:%S." ++ (printf "%03i" p) ++ " %Z " 
    return $ formatCalendarTime defaultTimeLocale frm c

  runLogger :: Logger ()
  runLogger =
    forever $ do
      r <- waitRequest 
      logMessage r

  waitRequest :: Logger LogMsg
  waitRequest = do
    c <- get
    liftIO $ readLog c

  logMessage :: LogMsg -> Logger ()
  logMessage (LogMsg name prio msg) = do
    liftIO $ Log.logM name prio msg

  logMessage (CfgLogMsg name file prio) = do
    old <- get
    let p = getLogLevel old
    let f = getLogFile  old
    let n = getName     old
    if n /= name ||
       f /= file ||
       p /= prio 
      then do
        put $ (setName name . setLogFile file . setLogLevel prio) old
        if p /= prio || 
           f /= file
          then 
            changeLogger f file prio
          else 
            return ()
      else  return ()

  changeLogger :: FilePath -> FilePath -> Log.Priority -> Logger()
  changeLogger old_file new_file new_prio = 
    if old_file /= new_file
      then do
        cfg <- get
        liftIO $ updLogger cfg -- ensure that logger does not get broken
      else
        liftIO $ Log.updateGlobalLogger Log.rootLoggerName $ Log.setLevel new_prio

  logX :: (LogMsg -> IO ()) -> String -> Log.Priority -> String -> IO ()
  logX wLog n p m = 
    wLog $ LogMsg n p m

