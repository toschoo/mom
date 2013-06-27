{-# LANGUAGE DeriveDataTypeable,RankNTypes #-}
module Network.Mom.Patterns.Streams.Types
where

  import           Prelude hiding (catch)
  import           Control.Exception (Exception, SomeException) 
  import           Data.Typeable (Typeable)
  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
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
  type Source  = C.Source RIO B.ByteString  
  type Sink    = C.Sink B.ByteString RIO () 
  type SinkR r = C.Sink B.ByteString RIO r 

  type Conduit o r = C.ConduitM   B.ByteString o RIO r

  streamList :: [B.ByteString] -> C.Producer RIO B.ByteString 
  streamList = mapM_ C.yield 

  passThrough :: Conduit B.ByteString ()
  passThrough = do mbX <- C.await
                   case mbX of
                     Nothing -> return ()
                     Just x  -> C.yield x >> passThrough

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

