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
                  try, reportError, reportSilent, Priority(..),
                  throwProtocol, throwQueue, throwTx, 
                  throwSocket, throwConnect, ouch)
where

  import Control.Exception hiding (try)
  import Prelude           hiding (catch)
  import Control.Applicative ((<$>))
  import Control.Monad
  import Data.Typeable (Typeable)
  import System.Log (Priority(..))

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

  reportError :: Exception e => Bool -> Priority -> (String -> e) -> String -> IO ()
  reportError err p e s = do reportString p s -- log! 
                             when err $ throwX e s  

  reportSilent :: Priority -> String -> IO ()
  reportSilent p s = reportString p s

  -- to Logger! --
  reportString :: Priority -> String -> IO ()
  reportString p s = do
    t  <- frmTime <$> getCurrentTime
    let l     = 11 - length ps
        f     = "%" ++ show l ++ "s"
        colon = printf f ": "
        ps    = show p 
    putStrLn $ t ++ " - " ++ ps ++ colon ++ s

  picoPerMilli :: Integer
  picoPerMilli = 1000000000

  frmTime :: UTCTime -> String
  frmTime t = 
    let ps  = (read (formatTime defaultTimeLocale "%q" t))::Integer 
        ms  = ps `div` picoPerMilli
        frm = "%Y-%m-%d-%H:%M:%S." ++ (printf "%03i" ms) ++ " %Z"
     in formatTime defaultTimeLocale frm t
    
