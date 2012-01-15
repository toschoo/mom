module Main 
where

  import           Helper
  import           Network.Mom.Patterns -- Patterns.Device
  import           Control.Concurrent

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    (p1, p2, _) <- getPorts
    let dealer = Address ("tcp://*:" ++ show p1) []
    let router = Address ("tcp://*:" ++ show p2) []
    withContext 1 $ \ctx -> 
      withQueue ctx "Simple Queue" (dealer,router) onErr_ $ \s ->
        untilInterrupt $ do
          putStrLn $ "Waiting for " ++ srvName s ++ "..."
          threadDelay 1000000

