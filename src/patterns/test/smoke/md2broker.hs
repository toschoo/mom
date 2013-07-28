module Main 
where

  import           Control.Monad (forever)
  import           Control.Concurrent
  import           Network.Mom.Patterns.Broker.Broker
  import           Network.Mom.Patterns.Types

  main :: IO ()
  main = withContext 1 $ \ctx -> 
             withBroker ctx "broker" 1000000
                        "tcp://*:5555" 
                        "tcp://*:5556" 
                        showErr $ \_ -> forever $ threadDelay 100000

  showErr :: OnError_
  showErr _ e m = putStrLn $ m ++ ": " ++  show e
