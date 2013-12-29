{-# Language DeriveDataTypeable #-}
module Types
where

  import           Network.Mom.Stompl.Client.Queue 
  import qualified Network.Mom.Stompl.Frame as F
  import           System.Timeout
  import           Data.Time.Clock
  import           Data.Char (isDigit)
  import qualified Data.ByteString.Char8 as B
  import           Data.Typeable (Typeable)
  import           Codec.MIME.Type (Type, nullType)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, 
                                      Exception, SomeException, Handler(..),
                                      AsyncException(ThreadKilled),
                                      bracket, catch, catches, finally)
  import           Control.Concurrent 
  import           Control.Monad (forever, unless, void)


  data StatusCode = OK | BadRequest | Forbidden | NotFound | Timeout
    deriving (Eq)

  instance Show StatusCode where
    show OK         = "200"
    show BadRequest = "400"
    show Forbidden  = "403"
    show NotFound   = "404"
    show Timeout    = "408"

  instance Read StatusCode where
    readsPrec _ r = case r of
                      "200" -> [(OK,"")]
                      "400" -> [(BadRequest,"")]
                      "403" -> [(Forbidden,"")]
                      "404" -> [(NotFound,"")]
                      "408" -> [(Timeout,"")]
                      _     -> undefined
                      
  readStatusCode :: String -> Either String StatusCode
  readStatusCode s = case s of
                       "200" -> Right OK
                       "400" -> Right BadRequest
                       "403" -> Right Forbidden
                       "404" -> Right NotFound
                       "408" -> Right Timeout
                       _     -> Left $ "Unknown status code: " ++ s

  type JobName = String
  type QName   = String

  nobody     :: OutBound ()
  nobody _ = return B.empty

  ignorebody :: InBound ()
  ignorebody _ _ _ _ = return ()

  bytesOut :: OutBound B.ByteString
  bytesOut = return

  bytesIn :: InBound B.ByteString
  bytesIn _ _ _ = return

  -----------------------------------------------------------------------
  -- Error and Exception
  -----------------------------------------------------------------------
  reThrowHandler :: Criticality -> String -> OnError -> [Handler a] 
  reThrowHandler c s onErr = [
    Handler (\e -> throwIO (e::AsyncException)),
    Handler (\e -> onErr c e s >> throwIO e)]

  ignoreHandler :: Criticality -> String -> OnError -> [Handler ()]
  ignoreHandler c s onErr = [
    Handler (\e -> throwIO (e::AsyncException)),
    Handler (\e -> onErr c e s)]

  killAndWait :: MVar () -> ThreadId -> IO ()
  killAndWait m tid = do killThread tid
                         void $ takeMVar m

  withThread :: IO () -> IO r -> IO r
  withThread thrd action = do
    stp <- newEmptyMVar
    bracket (forkIO $ finally thrd (putMVar stp ()))
            (killAndWait stp)
            (\_ -> action)

  -------------------------------------------------------------------------
  -- | Indicates criticality of the error event
  -------------------------------------------------------------------------
  data Criticality = 
                     -- | The current operation 
                     --   (/e.g./ processing a request)
                     --   has not terminated properly,
                     --   but the service is able to continue;
                     --   the error may have been caused by a faulty
                     --   request or other temporal conditions.
                     Error 
                     -- | The event has impact on the process,
                     --   leaving it in an unkown state.
                   | Critical 
                     -- | The service cannot recover and will terminate
                   | Fatal
    deriving (Eq, Ord, Show, Read)

  data PatternsException = TimeoutX          String
                         | BadStatusCodeX    String
                         | NotOKX StatusCode String
                         | HeaderX String    String
                         | MissingHbX        String
                         | UnacceptableHbX   Int
                         | NoProviderX       String
    deriving (Show, Read, Typeable, Eq)

  instance Exception PatternsException

  -----------------------------------------------------------------------
  -- Heartbeat
  -----------------------------------------------------------------------
  minPos :: Int -> Int -> Int
  minPos x y | x <= 0 && y <= 0 = -1
             | x <= 0           = y
             | y <= 0           = x
             | otherwise        = min x y

  strToBeat :: String -> Maybe Int
  strToBeat s | all isDigit s = Just $ read s
              | otherwise     = Nothing

  data HB = HB {
              hbMe     :: Int,
              hbMeNext :: UTCTime
            }
  
  mkHB :: Int -> IO HB
  mkHB me = do
    now <- getCurrentTime
    return HB {
            hbMe     = me,
            hbMeNext = timeAdd now me}

  tolerance :: Int
  tolerance = 10

  nextHB :: UTCTime -> Bool -> Int -> UTCTime
  nextHB now t p = let tol = if t then tolerance else 1
                    in timeAdd now $ tol * p

  -----------------------------------------------------------------------
  -- Adding a period to a point in time
  -----------------------------------------------------------------------
  timeAdd :: UTCTime -> Int -> UTCTime
  timeAdd t p = ms2nominal p `addUTCTime` t

  -----------------------------------------------------------------------
  -- Convert milliseconds to seconds
  -----------------------------------------------------------------------
  ms2nominal :: Int -> NominalDiffTime
  ms2nominal m = fromIntegral m / (1000::NominalDiffTime)

  type OnError  = Criticality         -> 
                  SomeException       -> 
                  String              -> IO ()


