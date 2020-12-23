module Main
where

  import           Network.Mom.Stompl.Frame
  import qualified Data.ByteString.Char8 as B
  import           Data.Char (isDigit)
  import qualified Data.Conduit as C
  import           Data.Conduit ((.|))
  import           Data.Conduit.Network.TLS
  import           Control.Concurrent
  import           Control.Exception (finally)
  import           Control.Monad.Trans (liftIO, MonadIO)
  import           System.Environment
  import           Network.Socket
  import           Network.Connection
  import           Data.Conduit.Network

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [p,q,f] -> if all isDigit p
                   then withSocketsDo $ conAndSend (read p) q f
                   else error ("Port is not numeric: " ++ p)
      _      -> error ("I need a port, a queue name and a message " ++
                       "and nothing else.")

  cnc :: Frame
  cnc = mkConnect "guest" "guest" "" (0,0) [(1,3)] "" []

  sendf :: AppData -> Frame -> IO ()
  sendf ad f = C.runConduitRes (C.yield (putFrame f) .| appSink ad)

  recvf :: AppData -> IO B.ByteString
  recvf ad = do
    ch <- newChan
    m  <- newEmptyMVar
    t  <- forkIO (finally (C.runConduitRes $ appSource ad .| pipeSink ch) 
                          (putMVar m ()))
    r  <- readChan ch
    killThread t
    takeMVar m
    return r
    
  pipeSink :: MonadIO m => Chan B.ByteString -> C.ConduitT B.ByteString C.Void m ()
  pipeSink ch = C.awaitForever (\i -> liftIO (writeChan ch i))

  outSink :: MonadIO m => C.ConduitT B.ByteString C.Void m ()
  outSink = C.awaitForever (\i -> liftIO (print i))

  conAndSend :: Int -> String -> String -> IO ()
  conAndSend p _  _ = let tlss = TLSSettingsSimple {
                                   settingDisableCertificateValidation = True,
                                   settingDisableSession               = False,
                                   settingUseServerName                = False}
                          cfg  = (tlsClientConfig p $ B.pack "localhost"){
                                   tlsClientTLSSettings=tlss}
                       in do print $ tlsClientTLSSettings cfg
                             runTLSClient cfg $ \ad -> do
                               sendf ad cnc
                               putStrLn "sent!"
                               i <- recvf ad
                               print i
                               -- receiver ad
                               -- forever $ threadDelay 10000000

  receiver :: AppData -> IO ()
  receiver ad = C.runConduitRes (appSource ad .| outSink)
    {-
    withConnection "127.0.0.1" p [] [] $ \c -> do
      let conv = return 
      q <- newWriter c "Test-Q" qn [] [] conv
      writeQ q nullType [] m
    -}
