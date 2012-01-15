module Main 
where

  import           Helper
  import           Network.Mom.Patterns -- Patterns.Device
  import           Control.Concurrent
  import           Control.Monad

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    (p1, p2, _) <- getPorts
    let puller = Address ("tcp://localhost:" ++ show p1) []
    let pusher = Address ("tcp://*:"         ++ show p2) []
    withContext 1 $ \ctx -> 
      withPipeline ctx "Pipeline" (puller,pusher) onErr_ wait
  
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000
