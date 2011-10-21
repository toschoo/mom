{-# OPTIONS -fglasgow-exts #-}
module Network.Mom.Stompl.Client.Exception (
                          StomplException(..), try, convertError)
where

  import Control.Exception hiding (try)
  import Prelude           hiding (catch)
  import Control.Applicative ((<$>))
  import Data.Typeable (Typeable)

  data StomplException = SocketException   String
                       | ProtocolException String
                       | QueueException    String
                       | TxException       String
                       | ConnectException  String
                       | ConvertException  String
                       | MimeException     String
                       | OuchException     String
                       | RethrownException String
    deriving (Show, Read, Typeable, Eq)

  instance Exception StomplException

  try :: IO a -> IO (Either StomplException a)
  try act = (Right <$> act) `catch` (return . Left)

  convertError :: String -> IO StomplException
  convertError e = throwIO $ ConvertException e

