{-# OPTIONS -fglasgow-exts #-}
module Network.Mom.Stompl.Exception
where

  import Control.Exception hiding (catch)
  import Data.Typeable (Typeable)

  data StomplException = SocketException   String
                       | ProtocolException String
                       | QueueException    String
                       | ConnectException  String
                       | ConvertException  String
                       | MimeException     String
                       | OuchException     String
                       | RethrownException String
    deriving (Show, Read, Typeable, Eq)

  instance Exception StomplException
