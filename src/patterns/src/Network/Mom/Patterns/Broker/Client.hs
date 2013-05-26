{-# Language RankNTypes #-}
module Network.Mom.Patterns.Broker.Client (
                   Client, withClient, checkService,
                   request, checkReceive)
where

  import qualified Data.ByteString.Char8  as B

  import           Control.Monad.Trans (liftIO)
  import           Control.Applicative ((<$>))
  import           Control.Exception   (throwIO)
  import           Data.Conduit (($=), (=$))
  import qualified Data.Conduit          as C
  import qualified System.ZMQ as Z

  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
  import           Network.Mom.Patterns.Broker.Common 

  data Client = Client {clSock    :: Z.Socket Z.XReq,
                        clService :: Service}

  withClient :: Z.Context        ->
                Service          -> 
                String           ->
                LinkType         ->
                (Client -> IO a) -> IO a
  withClient ctx srv add _ act =
    Z.withSocket ctx Z.XReq $ \s -> do
      link Connect s add []
      act $ Client s srv

  checkService :: Client -> Timeout -> IO (Maybe Bool)
  checkService c tmo = do
    runSender   (clSock c) $ mdpSrc mmi (C.yield $ B.pack $ clService c)
    runReceiver (clSock c) tmo $ mdpSnk mmi mmiResponse 
    where mmi = B.unpack $ mmiHdr `B.append` mmiSrv
          mmiResponse = do
            mbX <- C.await
            case mbX of
              Nothing -> return Nothing
              Just x  | x == mmiFound    -> return $ Just True
                      | x == mmiNotFound -> return $ Just False
                      | x == mmiNimpl    -> liftIO (throwIO $
                          ProtocolExc "MMI Service Request not available")
                      | otherwise        -> liftIO (throwIO $
                          ProtocolExc $ "Unexpected response code "  ++
                                        "from mmi.service request: " ++
                                        B.unpack x)
                        
  request :: Client -> Timeout -> Source -> SinkR (Maybe a) -> IO (Maybe a)
  request c tmo src snk = do
    runSender   (clSock c)     $ mdpSrc (clService c) src
    runReceiver (clSock c) tmo $ mdpSnk (clService c) snk

  checkReceive :: Client -> Timeout -> SinkR (Maybe a) -> IO (Maybe a)
  checkReceive c tmo snk = 
    runReceiver (clSock c) tmo $ mdpSnk (clService c) snk
    
  mdpSrc :: Service -> Source -> Source
  mdpSrc sn src = src $= mdpCSndReq sn 

  mdpSnk :: Service -> SinkR (Maybe a) -> SinkR (Maybe a)
  mdpSnk sn snk = mdpCRcvRep sn =$ snk
