{-# Language BangPatterns #-}
module Main
where

  import Network.Mom.Stompl.Frame
  import Network.Mom.Stompl.Parser
  import qualified Data.ByteString.Char8 as B
  import           Data.Char (isDigit)
  import           Data.String (fromString)
  import qualified Data.Conduit as C
  import           Data.Conduit ((.|))
  import           Data.Conduit.Network
  import           Data.Conduit.Network.TLS
  import           Control.Monad (forever)
  import           Control.Monad.Trans (liftIO)
  import           Control.Monad.IO.Class (MonadIO)
  import           Control.Concurrent
  import           System.Environment
  import           Network.Socket

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [p,q] -> if all isDigit p
                 then withSocketsDo $ conAndListen (read p) q 
                 else error ("Port is not numeric: " ++ p)
      _      -> error ("I need a port and a queue name " ++
                       "and nothing else.")

  conAndListen :: Int -> String -> IO ()
  conAndListen p _  = let hp   = fromString "*"
                          cfg  = tlsConfig hp p "test/keys/broker.crt"
                                                "test/keys/broker.ks"
                       in runTCPServerTLS cfg session
    where session ad = do
            ch <- newChan
            _  <- forkIO (sender ad ch)
            C.runConduitRes (appSource ad .| pipeSink ch)

  sender :: AppData -> Chan B.ByteString -> IO ()
  sender ad ch = C.runConduitRes (pipeSource ch .| parse .| handleFrame ad .| appSink ad)

  check :: MonadIO m => C.ConduitT B.ByteString B.ByteString m ()
  check = C.awaitForever (\i -> if B.null i then liftIO $ putStrLn "NULL!"
                                            else C.yield i)
 
  pipeSink :: MonadIO m => Chan B.ByteString -> C.ConduitT B.ByteString C.Void m ()
  pipeSink ch = C.awaitForever (\i -> liftIO (writeChan ch i))

  pipeSource :: MonadIO m => Chan B.ByteString -> C.ConduitT () B.ByteString m ()
  pipeSource ch = forever (liftIO (readChan ch) >>= C.yield)

  outSnk :: (MonadIO m, Show i) => C.ConduitT i C.Void m ()
  outSnk = C.awaitForever (\i -> liftIO (print i))

  parse :: MonadIO m => C.ConduitT B.ByteString Frame m ()
  parse = C.awaitForever (\i -> case stompAtOnce i of
                                  Left  e -> liftIO $ putStrLn ("error " ++ e)
                                  Right f -> C.yield f)

  handleFrame :: MonadIO m => AppData -> C.ConduitT Frame B.ByteString m () 
  handleFrame _ = C.awaitForever $ \f -> 
    case typeOf f of
      Connect{} -> case conToCond "" "ses1" (0,0) [(1,3)] f of
                    Nothing -> liftIO (putStrLn $ "Error: " ++ show f)
                    Just c  -> C.yield (putFrame c)
      _         -> liftIO (putStrLn $ "Not Connect: " ++ show f)

  sendf :: MonadIO m => AppData -> Frame -> C.ConduitT () Frame m ()
  sendf ad f = C.yield (putFrame f) .| appSink ad
