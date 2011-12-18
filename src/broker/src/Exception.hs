{-# OPTIONS -fglasgow-exts #-}
-------------------------------------------------------------------------------
-- |
-- Module     : Exception.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- Exceptions for the mq30 lq lmq lamq lambq lambdaQ ...
-------------------------------------------------------------------------------
module Exception (StompException(..), 
                  try, reportError, report, Priority(..),
                  throwProtocol, throwQueue, throwTx, 
                  throwSocket, throwConnect, ouch)
where

  import Control.Exception hiding (try)
  import Prelude           hiding (catch)
  import Control.Applicative ((<$>))
  import Control.Monad
  import Data.Typeable (Typeable)
  import System.Log (Priority(..))
  import Logger

  import Data.Time     -- to Logger!
  import System.Locale -- to Logger!
  import Text.Printf

  ------------------------------------------------------------------------
  -- | The Stompl Client uses exceptions to communicate errors
  --   to the user application.
  ------------------------------------------------------------------------
  data StompException =
                       SocketException     String
                       | ProtocolException String
                       | QueueException    String
                       | TxException       String
                       | ConnectException  String
                       | OuchException     String
    deriving (Show, Read, Typeable, Eq)

  instance Exception StompException

  ------------------------------------------------------------------------
  -- Catches any 'StompException',
  -- including asynchronous exceptions coming from internal threads
  ------------------------------------------------------------------------
  try :: IO a -> IO (Either StompException a)
  try act = (Right <$> act) `catch` (return . Left)

  throwProtocol :: String -> IO a
  throwProtocol = throwX ProtocolException 

  throwQueue :: String -> IO a
  throwQueue = throwX QueueException 

  throwTx :: String -> IO a
  throwTx = throwX TxException 

  throwSocket :: String -> IO a
  throwSocket = throwX SocketException 

  throwConnect :: String -> IO a
  throwConnect = throwX ConnectException 

  ouch :: String -> IO a
  ouch = throwX OuchException 

  throwX :: Exception e => (String -> e) -> String -> IO a
  throwX e s = throwIO (e s)

  reportError :: Exception e => String -> Priority -> (String -> e) -> String -> IO ()
  reportError n p e s = reportString n p s >> throwX e s  

  report :: String -> Priority -> String -> IO ()
  report n p s = reportString n p s

  reportString :: String -> Priority -> String -> IO ()
  reportString n p s = logX n p s
