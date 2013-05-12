{-# LANGUAGE DeriveDataTypeable #-}
module Network.Mom.Patterns.Streams.Types
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
  import qualified Data.ByteString.Char8 as B
  import           Data.Char (toLower)
  import           Data.Map (Map)
  import qualified Data.Map as Map
  import qualified System.ZMQ            as Z

  ------------------------------------------------------------------------
  -- Conduits
  ------------------------------------------------------------------------
  type Identifier = String
  type Service    = String
  type Timeout    = Z.Timeout
  type Context    = Z.Context
  type Size       = Z.Size

  withContext :: Z.Size -> (Context -> IO a) -> IO a
  withContext = Z.withContext

  type Identity    = B.ByteString
  type Body        = [B.ByteString]

  type RIO     = C.ResourceT IO
  type Source  = C.Source RIO B.ByteString -- make it a producer!
  type Sink    = C.Sink       B.ByteString RIO ()

  type SinkR a = C.Sink       B.ByteString RIO a -- make it a consumer!

  type Conduit o r = C.ConduitM   B.ByteString o RIO r

  streamList :: [B.ByteString] -> Source
  streamList = mapM_ C.yield 

  propagate :: B.ByteString -> Conduit B.ByteString ()
  propagate = C.yield

  propagateList :: [B.ByteString] -> Conduit B.ByteString ()
  propagateList ms = mapM_ propagate ms

  passThrough :: Conduit B.ByteString ()
  passThrough = do mbX <- C.await
                   case mbX of
                     Nothing -> return ()
                     Just x  -> C.yield x

  drop1 :: Conduit o ()
  drop1 = C.await >>= \_ -> return ()

  dropX :: Int -> Conduit o ()
  dropX 0 = return ()
  dropx i | i < 0     = return ()
          | otherwise = drop1 >> dropX (i-1)

  empty :: Conduit o ()
  empty = chunk B.empty "Missing Separator"

  chunk :: B.ByteString -> String -> Conduit o ()
  chunk p e = do
    mb <- C.await
    case mb of
      Nothing -> liftIO (throwIO $ ProtocolExc "Incomplete Message")
      Just x  -> unless (x == p) $ liftIO (throwIO $ ProtocolExc (e ++ ": " ++ 
                                                                  show x))
  getChunk :: Conduit o B.ByteString
  getChunk = do
    mb <- C.await
    case mb of
      Nothing -> liftIO (throwIO $ ProtocolExc "Incomplete Message")
      Just x  -> return x

  consume :: SinkR [B.ByteString]
  consume = go []
    where go rs = do mbR <- C.await
                     case mbR of
                       Nothing -> return $ reverse rs
                       Just r  -> go (r:rs)

  -------------------------------------------------------------------------
  -- | How to link to an 'AccessPoint'
  -------------------------------------------------------------------------
  data LinkType = 
         -- | Bind the address
         Bind 
         -- | Connect to the address
         | Connect
    deriving (Show, Read)

  ------------------------------------------------------------------------
  -- | Error handler for all services but servers;
  --   receives the 'Criticality' of the error event,
  --   the exception, the service name 
  --   
  --   A good policy is
  --   to terminate or restart the service
  --   when a 'Fatal' error occurs
  --   and to continue, if possible,
  --   on a plain 'Error'.
  --   The error handler, additionally, may log the incident
  --   or inform an administrator.
  ------------------------------------------------------------------------
  type OnError_  = Criticality         -> 
                   SomeException       -> 
                   String              -> IO ()

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
                     --   Note that if an application-defined 'E.Iteratee' or
                     --   'E.Enumerator' results in 'SomeException'
                     --   (by means of 'E.throwError'),
                     --   the incident is classified as 'Error';
                     --   if it throws an IO Error, however,
                     --   the incident is classified as 'Fatal'.
                     Error 
                     -- | One worker thread is lost ('Server' only)
                   | Critical 
                     -- | The service cannot recover and will terminate
                   | Fatal
    deriving (Eq, Ord, Show, Read)

  -------------------------------------------------------------------------
  -- Exception
  -------------------------------------------------------------------------
  data StreamException = SocketExc   String
                       | IOExc       String
                       | ProtocolExc String
                       | AppExc      String
                       | Ouch        String
    deriving (Show, Read, Typeable, Eq)

  instance Exception StreamException




