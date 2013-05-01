{-# LANGUAGE DeriveDataTypeable #-}
module Network.Mom.Patterns.Broker.Common
where

  import           Network.Mom.Patterns.Streams.Streams
  import           Network.Mom.Patterns.Streams.Types

  import qualified Data.ByteString.Char8  as B
  import qualified Data.ByteString        as BB
  import           Data.Word
  import           Data.Typeable (Typeable)
  import qualified Data.Conduit as C
  import qualified Data.Conduit.List as CL
  import           Control.Monad (unless)
  import           Control.Monad.Trans (liftIO)
  import           Control.Exception (throwIO)

  mdpC01, mdpW01 :: B.ByteString
  mdpC01 = B.pack "MDPC01"
  mdpW01 = B.pack "MDPW01"

  xReady, xRequest, xReply, xHeartBeat, xDisc :: B.ByteString
  xReady     = BB.pack [0x01]
  xRequest   = BB.pack [0x02]
  xReply     = BB.pack [0x03]
  xHeartBeat = BB.pack [0x04]
  xDisc      = BB.pack [0x05]

  type ServiceName = String

  data FrameType = ReadyT | RequestT | ReplyT | HeartBeatT | DisconnectT
    deriving (Eq, Show, Read)

  frameType :: Conduit o FrameType
  frameType = do
    mbT <- C.await
    case mbT of
      Nothing -> liftIO (throwIO $ ProtocolExc 
                           "Incomplete Message: No Frame Type")
      Just t  | t == xHeartBeat -> return HeartBeatT
              | t == xReady     -> return ReadyT
              | t == xReply     -> return ReplyT
              | t == xDisc      -> return DisconnectT
              | t == xRequest   -> return RequestT
              | otherwise       -> liftIO (throwIO $ ProtocolExc $ 
                                     "Unknown Frame: " ++ B.unpack t)

  identities :: Conduit o [Identity]
  identities = go []
    where go is = do
            mbI <- C.await
            case mbI of
              Nothing -> liftIO (throwIO $ ProtocolExc 
                                   "Incomplete Message: No Identity")
              Just i  | B.null i  -> 
                          if null is 
                            then liftIO (throwIO $
                                   ProtocolExc  
                                      "Incomplete Message: No identities")
                            else return is
                      | otherwise -> empty >> go (i:is) 

  toIs :: [Identity] -> [B.ByteString]
  toIs = foldr toI []
    where toI i is  = [i, B.empty] ++ is
