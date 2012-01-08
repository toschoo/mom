module Main 
where

  import           Helper
  import           Network.Mom.Device -- Patterns.Device
  import           Network.Mom.Patterns -- Patterns.Device
  import           Control.Concurrent
  import           Control.Monad

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    (p1, p2, _) <- getPorts
    let dealer = Address ("tcp://*:" ++ show p1) []
    let router = Address ("tcp://*:" ++ show p2) []
    withContext 1 $ \ctx -> 
      withQueue ctx "Simple Queue" noparam (dealer,router) onErr_ wait
  
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000

