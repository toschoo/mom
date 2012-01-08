module Main 
where

  import Helper
  import Network.Mom.Device -- Patterns.Device
  import Network.Mom.Patterns -- Patterns.Device

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    (p1, p2, _) <- getPorts
    let dealer = Address ("tcp://*:" ++ show p1) []
    let router = Address ("tcp://*:" ++ show p2) []
    withContext 1 $ \ctx -> queue ctx dealer router 
  
