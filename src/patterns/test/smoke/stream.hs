module Main 
where

  import           Control.Monad.Trans
  import           Control.Monad (forever)
  import           Control.Concurrent
  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
  
  import           Network.Mom.Patterns.Streams 
  import qualified System.ZMQ as Z

  main :: IO ()
  main = Z.withContext 1 $ \ctx -> do
           ready <- newEmptyMVar
           _ <- forkIO (ping ctx ready)
           _ <- forkIO (pong ctx ready)
           forever $ threadDelay 100000

  ping :: Z.Context -> MVar () -> IO ()
  ping ctx ready = withStreams ctx "pong" (-1)
                         [Poll "ping" "inproc://ping" PeerT Bind [] []]
                         (\_ -> return ())
                         (\_ _ _ -> return ())
                         pinger $ \c -> do
               putMVar ready ()
               putStrLn "starting game!"
               send c ["ping"] startPing
               putStrLn "game started!"
               forever $ threadDelay 100000
    where startPing = C.yield $ B.pack "ping"

  pong :: Z.Context -> MVar () -> IO ()
  pong ctx ready = do 
    _ <- takeMVar ready
    withStreams ctx "ping" (-1)
                [Poll "pong" "inproc://ping" PeerT Connect [] []]
                (\_ -> return ())
                (\_ _ _ -> return ())
                pinger $ \_ -> forever $ threadDelay 100000

  pinger :: StreamSink
  pinger s = C.awaitForever $ \i -> 
               let x = B.unpack i 
                in do liftIO $ putStrLn x
                      liftIO $ threadDelay 500000
                      case x of
                        "ping" -> stream s ["pong"] [B.pack "pong"]
                        "pong" -> stream s ["ping"] [B.pack "ping"]
                        _      -> return ()
               
           
