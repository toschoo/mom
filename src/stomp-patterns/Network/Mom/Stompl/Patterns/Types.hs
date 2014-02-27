{-# Language DeriveDataTypeable #-}
module Types
where

  import           Network.Mom.Stompl.Client.Queue 
  import           Data.Time.Clock
  import qualified Data.ByteString.Char8 as B
  import           Data.Typeable (Typeable)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, 
                                      Exception, SomeException, Handler(..),
                                      AsyncException(..),
                                      bracket, finally)
  import           Control.Concurrent 
  import           Control.Monad (void)


  ------------------------------------------------------------------------
  -- | Status code to communicate the state of a request
  --   between two applications. 
  --   The wire format is inspired by HTTP status codes:
  --
  --   * OK (200): Everything is fine
  --
  --   * BadRequest (400): Syntax error in the request message
  --
  --   * Forbidden (403): Not used
  --
  --   * NotFound (404): For the requested job no provider is available
  --
  --   * Timeout (408): Timeout expired
  ------------------------------------------------------------------------
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
                      
  ------------------------------------------------------------------------
  -- | Safe StatusCode parser 
  --   ('StatusCode' is instance of 'Read',
  --    but 'read' would cause an error on an invalid StatusCode)
  ------------------------------------------------------------------------
  readStatusCode :: String -> Either String StatusCode
  readStatusCode s = case s of
                       "200" -> Right OK
                       "400" -> Right BadRequest
                       "403" -> Right Forbidden
                       "404" -> Right NotFound
                       "408" -> Right Timeout
                       _     -> Left $ "Unknown status code: " ++ s

  -- | Name of a service, task or topic
  type JobName = String

  -- | Name of a Stomp queue
  type QName   = String

  -- | OutBound converter for messages of type ()
  nobody     :: OutBound ()
  nobody _ = return B.empty

  -- | InBound converter for messages of type ()
  ignorebody :: InBound ()
  ignorebody _ _ _ _ = return ()

  -- | OutBound converter for messages of type 'B.ByteString'
  bytesOut :: OutBound B.ByteString
  bytesOut = return

  -- | InBound converter for messages of type 'B.ByteString'
  bytesIn :: InBound B.ByteString
  bytesIn _ _ _ = return

  -----------------------------------------------------------------------
  -- Error and Exception
  -----------------------------------------------------------------------
  reThrowHandler :: String -> OnError -> [Handler a] 
  reThrowHandler s onErr = [
    Handler (\e -> throwIO (e::AsyncException)),
    Handler (\e -> onErr e s >> throwIO e)]

  ignoreHandler :: String -> OnError -> [Handler ()]
  ignoreHandler s onErr = [
    Handler (\e -> throwIO (e::AsyncException)),
    Handler (\e -> onErr e s)]

  killAndWait :: MVar () -> ThreadId -> IO ()
  killAndWait m tid = do killThread tid
                         void $ takeMVar m

  withThread :: IO () -> IO r -> IO r
  withThread thrd action = do
    stp <- newEmptyMVar
    bracket (forkIO $ finally thrd (putMVar stp ()))
            (killAndWait stp)
            (\_ -> action)

  ------------------------------------------------------------------------
  -- | Patterns Exception
  ------------------------------------------------------------------------
  data PatternsException = 
                         -- | Timout expired
                         TimeoutX            String
                         -- | Invalid status code 
                         | BadStatusCodeX    String
                         -- | Status code other than OK
                         | NotOKX StatusCode String
                         -- | Error on Header identified by the first String
                         | HeaderX String    String
                         -- | Thrown on missing heartbeat 
                         --   (after tolerance of 10 missing heartbeats)
                         | MissingHbX        String
                         -- | Heartbeat proposed by registry
                         --   out of acceptable range
                         | UnacceptableHbX   Int
                         -- | No provider for the requested job available
                         | NoProviderX       String
                         -- | Application-defined exception
                         | AppX              String
    deriving (Show, Read, Typeable, Eq)

  instance Exception PatternsException

  ------------------------------------------------------------------------
  -- | Error Handler:
  --
  --   * 'SomeException': Exception that led the invocation;
  --
  --   * String: Name of the entity (client name, server name, /etc./)
  ------------------------------------------------------------------------
  type OnError  = SomeException -> String -> IO ()

  ------------------------------------------------------------------------
  -- | Heartbeat controller type
  ------------------------------------------------------------------------
  data HB = HB {
              hbMe     :: Int,
              hbMeNext :: UTCTime
            }
  
  ------------------------------------------------------------------------
  -- | Create a heartbeat controller;
  --   receives the heartbeat in milliseconds.
  ------------------------------------------------------------------------
  mkHB :: Int -> IO HB
  mkHB me = do
    now <- getCurrentTime
    return HB {
            hbMe     = me,
            hbMeNext = timeAdd now me}

  -- HB tolerance 
  tolerance :: Int
  tolerance = 10

  ------------------------------------------------------------------------
  -- Comput the next hearbeat from current one
  ------------------------------------------------------------------------
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

  -----------------------------------------------------------------------
  -- Convert 'NominalDiffTime' to microseconds
  -----------------------------------------------------------------------
  nominal2us :: NominalDiffTime -> Int
  nominal2us m = round (fact * realToFrac m :: Double)
    where fact = 10.0^(6::Int)


