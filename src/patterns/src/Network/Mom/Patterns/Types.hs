{-# LANGUAGE DeriveDataTypeable,RankNTypes #-}
-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Types.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
--
-- Fundamental streaming types 
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Types
where

  import           Prelude hiding (catch)
  import           Control.Exception (Exception, SomeException) 
  import           Data.Typeable (Typeable)
  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
  import qualified System.ZMQ            as Z

  ------------------------------------------------------------------------
  -- * Conduits
  ------------------------------------------------------------------------
  -- | The IO Resource transformer.
  --   See the conduit package for details
  type RIO     = C.ResourceT IO

  -- | A stream source
  type Source  = C.Source RIO B.ByteString  

  -- | A stream sink without return type
  type Sink    = C.Sink B.ByteString RIO () 

  -- | A stream sink wit return type
  type SinkR r = C.Sink B.ByteString RIO r 

  -- | A conduit that links source and sink
  --   applying some transformation to the stream.
  --   Input is always 'B.ByteString',
  --   output and return type may vary.
  type Conduit o r = C.ConduitM B.ByteString o RIO r

  -- | Simplified Conduit where output
  --   is always 'B.ByteString' 
  --   and no final value is returned.
  type Conduit_ = Conduit B.ByteString ()

  -- | Streaming the elements of a list
  streamList :: [B.ByteString] -> C.Producer RIO B.ByteString 
  streamList = mapM_ C.yield 

  -- | Pass the stream through without applying
  --   any transformation to it
  passThrough :: Conduit B.ByteString ()
  passThrough = C.awaitForever C.yield 

  ------------------------------------------------------------------------
  -- * Link Type
  ------------------------------------------------------------------------
  -------------------------------------------------------------------------
  -- | A /zeromq/ 'AccessPoint'
  --   can be bound or connected to its address.
  --   Only one peer can bind the address,
  --   all other parties have to connect.
  -------------------------------------------------------------------------
  data LinkType = 
         -- | Bind the address
         Bind 
         -- | Connect to the address
         | Connect
    deriving (Show, Read)

  ------------------------------------------------------------------------
  -- * Error Handling
  ------------------------------------------------------------------------
  ------------------------------------------------------------------------
  -- | Error handler for all services 
  --   that are implemented as background services,
  --   /e.g./ servers and brokers.
  --   The handler receives the 'Criticality' of the error event,
  --   the exception and an additional descriptive string.
  --   
  --   A good policy is
  --   to terminate or restart the service
  --   when a 'Fatal' or 'Critical' error occurs
  --   and to continue, if possible,
  --   on a plain 'Error'.
  --   The error handler may perform additional, user-defined actions, 
  --   such as logging the incident or 
  --   sending an SMS.
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
                     Error 
                     -- | The event has impact on the process,
                     --   leaving it in an unkown state.
                   | Critical 
                     -- | The service cannot recover and will terminate
                   | Fatal
    deriving (Eq, Ord, Show, Read)

  -------------------------------------------------------------------------
  -- | Stream Exception
  -------------------------------------------------------------------------
  data StreamException = 
              -- | low-level error
              SocketExc   String
              -- | IO error
              | IOExc       String
              -- | Protocol error
              | ProtocolExc String 
              -- | Application-defined error
              | AppExc      String
              -- | Internal error, indicating a code error in library
              | Ouch        String
    deriving (Show, Read, Typeable, Eq)

  instance Exception StreamException

  ------------------------------------------------------------------------
  -- * Some convenient definitions
  ------------------------------------------------------------------------
  -- | String identifying a stream in the streams device
  type Identifier = String

  -- | String identifying a service provided, /e.g./ by a /server/
  type Service    = String

  -- | Identity of a communication peer,
  --   needed for complex patterns (/e.g./ broker)
  type Identity    = B.ByteString

  -- | Message body,
  --   needed for complex patterns (/e.g./ broker)
  type Body        = [B.ByteString]

  ------------------------------------------------------------------------
  -- | Milliseconds 
  ------------------------------------------------------------------------
  type Msec = Int

  -- | Reexport from zeromq (timeout in microseconds)
  type Timeout    = Z.Timeout
  -- | Reexport from zeromq
  type Context    = Z.Context
  -- | Reexport from zeromq
  type Size       = Z.Size

  -- | Reexport from zeromq
  withContext :: Size -> (Context -> IO a) -> IO a
  withContext = Z.withContext

