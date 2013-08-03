{-# LANGUAGE DeriveDataTypeable,RankNTypes #-}
-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Broker/Common.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Majordomo common definitions
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Broker.Common
where

  import           Network.Mom.Patterns.Types

  import qualified Data.ByteString.Char8  as B
  import qualified Data.ByteString        as BB
  import qualified Data.Conduit as C
  import           Control.Monad (unless)
  import           Control.Monad.Trans (liftIO)
  import           Control.Exception (Exception, throwIO) 
  import           Data.Typeable (Typeable)

  import           Control.Applicative ((<$>))

  ------------------------------------------------------------------------
  -- | Majordomo protocol client/worker version 1
  ------------------------------------------------------------------------
  mdpC01, mdpW01 :: B.ByteString
  mdpC01 = B.pack "MDPC01"
  mdpW01 = B.pack "MDPW01"

  ------------------------------------------------------------------------
  -- | Message types (ready, request, reply, heartbeat, disconnect)
  ------------------------------------------------------------------------
  xReady, xRequest, xReply, xHeartBeat, xDisc :: B.ByteString
  xReady     = BB.pack [0x01]
  xRequest   = BB.pack [0x02]
  xReply     = BB.pack [0x03]
  xHeartBeat = BB.pack [0x04]
  xDisc      = BB.pack [0x05]

  ------------------------------------------------------------------------
  -- | Service name 
  ------------------------------------------------------------------------
  type ServiceName = String

  ------------------------------------------------------------------------
  -- | Majordomo Management Interface (MMI) -
  --   \"mmi.service\" 
  ------------------------------------------------------------------------
  mmiHdr, mmiSrv :: B.ByteString 
  mmiHdr = B.pack "mmi."
  mmiSrv = B.pack "service"
  
  ------------------------------------------------------------------------
  -- | Majordomo Management Interface -- responses:
  --   Found (\"200\"), NotFound (\"404\"), NotImplemented (\"501\")
  ------------------------------------------------------------------------
  mmiFound, mmiNotFound, mmiNimpl :: B.ByteString
  mmiFound    = B.pack "200"
  mmiNotFound = B.pack "404"
  mmiNimpl    = B.pack "501"

  ------------------------------------------------------------------------
  -- | Client -> Broker: send request
  ------------------------------------------------------------------------
  mdpCSndReq :: ServiceName -> Conduit B.ByteString ()
  mdpCSndReq sn = mapM_ C.yield [B.empty, mdpC01, B.pack sn] >> passThrough

  ------------------------------------------------------------------------
  -- | Client -> Broker: receive request
  ------------------------------------------------------------------------
  mdpCRcvReq :: Conduit o (Identity, B.ByteString)
  mdpCRcvReq = do i <- identity
                  protocol
                  sn <- getChunk 
                  return (i, sn)
    where  protocol = chunk mdpC01 "Unknown Protocol - expected Client 0.1"

  ------------------------------------------------------------------------
  -- | Broker -> Client: send reply
  ------------------------------------------------------------------------
  mdpCSndRep :: B.ByteString -> [Identity] -> Conduit B.ByteString ()
  mdpCSndRep sn is = mapM_ C.yield hdr >> passThrough
    where hdr = toIs is ++ [mdpC01, sn]

  ------------------------------------------------------------------------
  -- | Broker -> Client: receive reply
  ------------------------------------------------------------------------
  mdpCRcvRep :: ServiceName -> Conduit B.ByteString ()
  mdpCRcvRep sn = empty >> protocol >> serviceName >> passThrough
    where protocol    = chunk mdpC01 "Unknown Protocol - expected Client 0.1"
          serviceName = chunk (B.pack sn) ("Wrong service - expected: " ++ sn)

  ------------------------------------------------------------------------
  -- | Broker -> Server: send request 
  ------------------------------------------------------------------------
  mdpWSndReq :: Identity -> [Identity] -> Conduit B.ByteString ()
  mdpWSndReq w is = mapM_ C.yield hdr >> passThrough
    where hdr = [w, B.empty, mdpW01, xRequest] ++ toIs is ++ [B.empty]

  ------------------------------------------------------------------------
  -- | Broker -> Server: receive request 
  ------------------------------------------------------------------------
  mdpWRcvReq :: Conduit o WFrame
  mdpWRcvReq = do 
    empty >> protocol
    t <- frameType
    case t of
      HeartBeatT  -> return $ WBeat B.empty
      DisconnectT -> return $ WDisc B.empty
      RequestT    -> WRequest <$> envelope
      x           -> liftIO $ throwIO $ MDPExc $
                       "Unexpected Frame from Broker: " ++ show x
    where protocol = chunk mdpW01 ("Unknown Protocol from Broker " ++
                                   " -- expected: Worker 0.1")

  ------------------------------------------------------------------------
  -- | Server -> Broker: send reply
  ------------------------------------------------------------------------
  mdpWSndRep :: [Identity] -> Conduit B.ByteString ()
  mdpWSndRep is = streamList hdr >> passThrough
    where hdr = [B.empty, -- identity delimiter
                 mdpW01,
                 xReply] ++ toIs is ++ [B.empty]
 
  ------------------------------------------------------------------------
  -- | Server -> Broker: receive reply
  ------------------------------------------------------------------------
  mdpWRcvRep :: Conduit o WFrame
  mdpWRcvRep = do
    w <- identity
    protocol
    t <- frameType
    case t of
      HeartBeatT  -> return $ WBeat w
      DisconnectT -> return $ WDisc w
      ReadyT      -> WReady w <$> getSrvName
      ReplyT      -> getRep w
      x           -> liftIO $ throwIO $ MDPExc $
                       "Unexpected Frame from Worker: " ++ show x
    where protocol   = chunk mdpW01 "Unknown Protocol from Worker"
          getSrvName = getChunk
          getRep w   = do is <- envelope
                          return $ WReply w is 

  ------------------------------------------------------------------------ 
  -- | Broker \<-\> Server: send heartbeat 
  ------------------------------------------------------------------------
  mdpWBeat :: Conduit B.ByteString ()
  mdpWBeat = streamList [B.empty,
                         mdpW01,
                         xHeartBeat]

  ------------------------------------------------------------------------ 
  -- | Server -> Broker: send connect request (ready)
  ------------------------------------------------------------------------
  mdpWConnect :: ServiceName -> Source
  mdpWConnect sn = streamList [B.empty, -- identity delimiter
                               mdpW01, 
                               xReady, 
                               B.pack sn]

  ------------------------------------------------------------------------ 
  -- | Server -\> Broker: disconnect 
  ------------------------------------------------------------------------
  mdpWDisconnect :: Source
  mdpWDisconnect = streamList [B.empty, -- identity delimiter
                               mdpW01,
                               xDisc]

  ------------------------------------------------------------------------ 
  -- | Broker -> Server: disconnect
  ------------------------------------------------------------------------
  mdpWBrkDisc :: Identity -> Source
  mdpWBrkDisc i = streamList [i] >> mdpWDisconnect

  ------------------------------------------------------------------------
  -- | Broker / Server protocol:
  --  Heartbeat, Ready, Reply, Request, Disconnect
  ------------------------------------------------------------------------
  data WFrame = WBeat    Identity
              | WReady   Identity B.ByteString
              | WReply   Identity [Identity]
              | WRequest          [Identity]
              | WDisc    Identity
    deriving (Eq, Show)

  ------------------------------------------------------------------------
  -- | Worker Frame Type
  ------------------------------------------------------------------------
  data FrameType = ReadyT | RequestT | ReplyT | HeartBeatT | DisconnectT
    deriving (Eq, Show, Read)

  ------------------------------------------------------------------------
  -- | Get frame type
  ------------------------------------------------------------------------
  frameType :: Conduit o FrameType
  frameType = do
    mbT <- C.await
    case mbT of
      Nothing -> liftIO (throwIO $ MDPExc
                           "Incomplete Message: No Frame Type")
      Just t  | t == xHeartBeat -> return HeartBeatT
              | t == xReady     -> return ReadyT
              | t == xReply     -> return ReplyT
              | t == xDisc      -> return DisconnectT
              | t == xRequest   -> return RequestT
              | otherwise       -> liftIO (throwIO $ MDPExc $ 
                                     "Unknown Frame: " ++ B.unpack t)

  ------------------------------------------------------------------------
  -- | Get empty segment
  ------------------------------------------------------------------------
  empty :: Conduit o ()
  empty = chunk B.empty "Missing Separator"

  ------------------------------------------------------------------------
  -- | Check segment contents
  ------------------------------------------------------------------------
  chunk :: B.ByteString -> String -> Conduit o ()
  chunk p e = do
    mb <- C.await
    case mb of
      Nothing -> liftIO (throwIO $ MDPExc $ "Incomplete Message: " ++ e)
      Just x  -> unless (x == p) $ liftIO (throwIO $ MDPExc (e ++ ": " ++ 
                                                                  show x))
  ------------------------------------------------------------------------
  -- | Get segment contents
  ------------------------------------------------------------------------
  getChunk :: Conduit o B.ByteString
  getChunk = do
    mb <- C.await
    case mb of
      Nothing -> liftIO (throwIO $ MDPExc "Incomplete Message")
      Just x  -> return x

  ------------------------------------------------------------------------
  -- | Get identity
  ------------------------------------------------------------------------
  identity :: Conduit o Identity
  identity = do
    mbI <- C.await
    case mbI of
      Nothing -> liftIO (throwIO $ MDPExc
                           "Incomplete Message: No Identity")
      Just i  | B.null i  -> 
                  liftIO (throwIO $ MDPExc
                           "Incomplete Message: Empty Identity")
              | otherwise -> empty >> return i

  ------------------------------------------------------------------------
  -- | Get block of identities ("envelope")
  ------------------------------------------------------------------------
  envelope :: Conduit o [Identity]
  envelope = go []
    where go is = do
            mbI <- C.await
            case mbI of
              Nothing -> liftIO (throwIO $ MDPExc
                                   "Incomplete Message: No Identity")
              Just i  | B.null i  -> 
                          if null is 
                            then liftIO (throwIO $ MDPExc
                                      "Incomplete Message: No identities")
                            else return is
                      | otherwise -> empty >> go (i:is) 

  ------------------------------------------------------------------------
  -- | Create envelope [(identity, B.empty)]
  ------------------------------------------------------------------------
  toIs :: [Identity] -> [B.ByteString]
  toIs = foldr toI []
    where toI i is  = [i, B.empty] ++ is

  -------------------------------------------------------------------------
  -- | MDP Exception
  -------------------------------------------------------------------------
  data MDPException = 
         -- | Server-side exception
         ServerExc   String
         -- | Client-side exception
         | ClientExc   String
         -- | Broker exception
         | BrokerExc   String
         -- | Generic Protocol
         | MDPExc   String
         -- | MMI Protocol
         | MMIExc   String
         -- | SingleBroker error
         --   (another broker is already running in the same process)
         | SingleBrokerExc String -- move to Broker.Common
    deriving (Show, Read, Typeable, Eq)

  instance Exception MDPException

