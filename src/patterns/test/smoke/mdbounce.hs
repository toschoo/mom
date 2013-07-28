module Main 
where

  import           Control.Monad (forever)
  import           Control.Concurrent
  import           Network.Mom.Patterns.Broker.Server
  import           Network.Mom.Patterns.Types

  main :: IO ()
  main = withContext 1 $ \ctx -> 
             withServer ctx "echo" 1000 "tcp://localhost:5556" 
                        showErr
                        bounce $ \_ -> forever $ threadDelay 500000
    where bounce = passThrough

  showErr :: OnError_
  showErr _ e m = putStrLn $ m ++ ": " ++  show e
