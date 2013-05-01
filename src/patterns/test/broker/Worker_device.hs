module Network.Mom.Patterns.Broker.Worker
where

  import           Helper
  import           Network.Mom.Patterns.Basic (
                      Context, Parameter, OnError_, Service,
                      AccessPoint(..), LinkType(..),
                      Client, withClient, request, askFor, forwardDealer,
                      Dealer, withDealer, give, obtain, forwardClient)
  import           Network.Mom.Patterns.Device
  import           Network.Mom.Patterns.Enumerator (just)
  import           Network.Mom.Patterns.Broker.Common
  import qualified Data.Enumerator as E
  import           Data.Enumerator (($$))
  import qualified Data.Enumerator.List as EL
  import qualified Data.ByteString as B
  import qualified Data.ByteString.Char8 as BC
  import qualified Data.Sequence as S
  import           Control.Applicative ((<$>))
  import           Control.Monad (when, unless, forever)
  import           Control.Monad.Trans (liftIO)
  import           Control.Concurrent 
  import           Control.Exception (SomeException, finally, throwIO)
  import           System.IO.Unsafe

  wrap :: Context      -> ServiceName        ->
          AccessPoint  -> AccessPoint        -> 
          OnError_     -> (Service -> IO ()) -> IO ()
  wrap ctx sn acBroker acServer onerr act = do
    cmd <- newChan
    withDevice ctx "WDP" sn (-1) 
                   [pollEntry "server"   XClient acServer   Bind    [],
                    pollEntry "broker"   XRouter acBroker   Connect [],
                    pollEntry "internal" XRouter acInternal Bind    []]
                   return return onerr 
                   (\_ -> return ()) 
                   (handleStream cmd) $ \s -> finally (init cmd s) disc
    where init cmd s = forkIO (control cmd ctx acBroker s) >>= \_ -> act s
          disc       = withClient ctx acInternal return return $ \c -> 
                         askFor c (just xDiscc)

  control :: Chan B.ByteString -> Context -> 
             AccessPoint -> Service -> IO ()
  control cmd ctx b s = 
    withDealer ctx acInternal Connect return return $ \d -> do
      give d (just xInit)
      ds <- newEmptyMVar
      forever $ do
        x <- readChan cmd
        case B.unpack x of
          [0x03] -> pauseBroker  ds
          [0x04] -> resumeBroker ds
          _      -> throwIO $ OuchExc "Unknown controller request"
    where pauseBroker ds = do
            e <- isEmptyMVar ds
            when (e) $ do 
               remDevice s "broker"
               putMVar ds $ pollEntry "broker" XRouter b Connect []
            writeChan cmd xOk
            putStrLn "paused"
          resumeBroker ds = do
            e <- isEmptyMVar ds
            unless (e) $ do pe <- takeMVar ds
                            addDevice s pe
            writeChan cmd xOk
            putStrLn "resumed"

  xOk, xInit, xDiscc, xPause, xResume :: B.ByteString
  xOk     = B.pack [0x00]
  xInit   = B.pack [0x01]
  xDiscc  = B.pack [0x02]
  xPause  = B.pack [0x03]
  xResume = B.pack [0x04]

  acInternal :: AccessPoint
  acInternal = Address "inproc://_wrapMDP" [] -- protect!!!

  ------------------------------------------------------------------------
  -- Connect to Broker and handle Messages 
  ------------------------------------------------------------------------
  handleStream :: Chan B.ByteString -> ServiceName -> 
                  Transformer B.ByteString
  handleStream cmd sn s os =
    case getStreamSource s of
      "internal" -> handleInternal     sn s os
      "broker"   -> handleBroker   cmd s os
      "server"   -> handleWorker   cmd s os
      _          -> return ()

  handleInternal :: ServiceName -> Transformer B.ByteString
  handleInternal sn s os = do
    x <- getChunk
    case x of
      xInit -> sendReady  sn s os -- watch out for case with variable
      xDisc -> disconnect    s os
      _     -> return ()

  sendReady :: ServiceName -> Transformer B.ByteString
  sendReady sn s os = let trg = filterTargets s (== "broker")
                       in emit s trg connect continueHere
    where connect = S.fromList [B.empty, -- identity delimiter
                                B.empty,
                                mdpW01, 
                                xReady, 
                                BC.pack sn]

  ------------------------------------------------------------------------
  -- Disconnect from Broker
  ------------------------------------------------------------------------
  disconnect :: Transformer B.ByteString
  disconnect s os = let trg = filterTargets s (== "broker")
                     in emit s trg disc continueHere
    where disc = S.fromList [B.empty, -- identity delimiter
                             B.empty, 
                             mdpW01,
                             xDisc]
      
  ------------------------------------------------------------------------
  -- Broker Messages 
  ------------------------------------------------------------------------
  data Frame = RequestFrame [Identity] 
             | ReplyFrame   [Identity] 
             | BeatFrame
             | DiscFrame
    deriving (Eq, Show)

  handleBroker :: Chan B.ByteString -> Transformer B.ByteString
  handleBroker cmd s os = do
     f <- getFrame 
     case f of
       RequestFrame is -> handleRequest cmd is s os -- ms
       BeatFrame       -> let trg = filterTargets s (== "broker")
                           in emit s trg beat continueHere
       DiscFrame       -> E.throwError $ DiscExc "Broker disconnects"
       _               -> E.throwError $ OuchExc "Unknown Frame"
    where beat = S.fromList [B.empty,
                             B.empty,
                             mdpW01,
                             xHeartBeat]

  {-# NOINLINE _req #-}
  _req :: MVar [Identity]
  _req = unsafePerformIO newEmptyMVar 
 
  ------------------------------------------------------------------------
  -- Handle Request Message
  ------------------------------------------------------------------------
  handleRequest :: Chan B.ByteString -> [Identity] -> Transformer B.ByteString
  handleRequest cmd is s os = do
    let trg = filterTargets s (== "server")
    ok <- liftIO $ tryPutMVar _req is
    unless ok $ E.throwError $ OuchExc "Error in request handler"
    liftIO $ putStrLn "passing through"
    passThrough trg s os
    -- liftIO $ writeChan cmd xPause
    -- _ <- liftIO $ readChan cmd
    -- return ()

  handleWorker :: Chan B.ByteString -> Transformer B.ByteString
  handleWorker cmd s os = do
    liftIO $ putStrLn "handle worker"
    -- liftIO $ writeChan cmd xResume
    -- _ <- liftIO $ readChan cmd
    let trg = filterTargets s (== "broker")
    mbIs <- liftIO $ tryTakeMVar _req 
    case mbIs of
      Nothing -> E.throwError $ ServerExc "Unexpected response from server"
      Just is -> do liftIO $ putStrLn "emit"
                    emitPart s trg (reply is) (passThrough trg)
    where reply is = S.fromList ([B.empty, -- identity delimiter
                                  B.empty,
                                  mdpW01,
                                  xReply] ++ toIs is ++ [B.empty])
                          
  ------------------------------------------------------------------------
  -- Read request
  ------------------------------------------------------------------------
  getFrame :: E.Iteratee B.ByteString IO Frame
  getFrame = do
    empty -- identity delimiter
    empty
    protocol
    t <- frameType
    case t of
      HeartBeatT  -> return BeatFrame
      DisconnectT -> return DiscFrame
      RequestT    -> RequestFrame <$> identities -- getReq
      x           -> E.throwError $ ProtocolExc $ 
                       "Unexpected Frame: " ++ show x
    where protocol = chunk mdpW01 "Unknwon Protocol"

