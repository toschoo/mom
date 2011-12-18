{-# OPTIONS -fglasgow-exts -fno-cse #-}
module Logger (startLogger, logX)
where

  import           Types
  import           Config
  
  import qualified System.Log.Logger as Log
  import           System.Log.Handler.Simple
  import           System.Log.Handler.Log4jXML
  import           System.Log.Handler (setFormatter)
  import           System.Log.Formatter (varFormatter)
  import           System.IO (Handle, stdout, stderr)
  import           System.Locale (defaultTimeLocale)
  import           System.IO.Unsafe

  import           Data.Time
  import           Data.Typeable (Typeable)
  import           Text.Printf
  import           Network (connectTo, PortID(..))

  import           Control.Monad
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Exception
  import           Prelude hiding (catch)

  data LoggerException = LoggerException    String
                       | DowngradeException String
    deriving (Eq, Show, Read, Typeable)

  instance Exception LoggerException

  startLogger :: IO ThreadId
  startLogger = forkIO startLogging 

  startLogging :: IO ()
  startLogging = 
    catches (updLogger >> runLogger) [Handler handleLoggerEx,
                                      Handler handleSomeEx]
    where handleLoggerEx e = 
            case e of
              DowngradeException _ -> 
                throwIO $ LoggerException "Panic! Logger down!"
              _ -> do 
                putStrLn $ "Panic! No logger: " ++ show (e::LoggerException) ++
                           " - Trying to downgrade..."
                downgradeLogger >> runLogger

          handleSomeEx e   = do
             putStrLn $ "Panic! No logger: " ++ show (e::SomeException) ++
                        " - Trying to downgrade..."
             downgradeLogger >> runLogger

  updLogger :: IO ()
  updLogger = do
    l   <- getCfgLogger
    p   <- getCfgLogLevel
    frm <- getCfgLogFormat
    h   <- case l of
           StdLog hdl -> case frm of 
                           Native -> nativeStreamHandler hdl p
                           Log4j  -> log4jStreamHandler' hdl p
           FileLog f  -> case frm of
                           Native -> nativeFileHandler f p
                           Log4j  -> log4jFileHandler' f p
           SrvLog host port -> do 
             hdl <- connectTo host (PortNumber $ fromIntegral port)
             case frm of 
               Native -> nativeStreamHandler hdl p
               Log4j  -> log4jStreamHandler' hdl p
    Log.updateGlobalLogger Log.rootLoggerName $ Log.setLevel p . Log.setHandlers [h]

  downgradeLogger :: IO ()
  downgradeLogger = do
    t <- getCfgLogType
    p <- getCfgLogLevel
    h <- case t of
           Server -> do
             f <- getCfgLogFile
             if null f
               then  nativeStreamHandler stderr p
               else  nativeFileHandler   f      p   
           File   -> nativeStreamHandler stderr p
           _      -> 
             throwIO $ DowngradeException "Panic! Cannot downgrade logger!"
    Log.updateGlobalLogger Log.rootLoggerName $ Log.setLevel p . Log.setHandlers [h]

  nativeStreamHandler :: Handle -> Priority -> IO (GenericHandler Handle)
  nativeStreamHandler h p = 
    streamHandler h p >>= \h0 -> return $
      setFormatter h0 $ varFormatter [("time", putTime)]
        "$time [$loggername]  $prio - $msg"

  nativeFileHandler :: FilePath -> Priority -> IO (GenericHandler Handle)
  nativeFileHandler f p = 
    fileHandler f p >>= \h0 -> return $
      setFormatter h0 $ varFormatter [("time", putTime)]
        "$time [$loggername]  $prio - $msg"

  picoPerMilli :: Integer
  picoPerMilli = 1000000000

  putTime :: IO String
  putTime = frmTime <$> getCurrentTime

  frmTime :: UTCTime -> String
  frmTime t = 
    let ps  = (read (formatTime defaultTimeLocale "%q" t))::Integer 
        ms  = ps `div` picoPerMilli
        frm = "%Y-%m-%d-%H:%M:%S." ++ (printf "%03i" ms) ++ " %Z"
     in formatTime defaultTimeLocale frm t

  runLogger :: IO ()
  runLogger =
    forever $ do
      r <- waitLogRequest 
      case r of 
        l@(LogReq _ _ _) -> logMessage l
        LogCfgReq        -> updLogger

  logMessage :: LogRequest -> IO ()
  logMessage (LogReq name prio msg) = Log.logM name prio msg

  logX :: String -> Log.Priority -> String -> IO ()
  logX n p m = buildName n >>= \l -> putLogRequest $ LogReq l p m

  buildName :: String -> IO String
  buildName s = do
    bn <- getCfgName
    h  <- getCfgHost
    return $ "\\mq(" ++ bn ++ "@" ++ h ++ ")." ++ s

