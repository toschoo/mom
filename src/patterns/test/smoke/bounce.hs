module Main 
where

  import           Control.Monad (forever)
  import           Control.Concurrent
  import           Network.Mom.Patterns.Basic.Server
  import           Network.Mom.Patterns.Types

  main :: IO ()
  main = withContext 1 $ \ctx -> 
             withServer ctx "olleh" "tcp://*:5555" Bind
                        showErr
                        bounce $ \_ -> forever $ threadDelay 100000
    where bounce = passThrough

  showErr :: OnError_
  showErr _ e m = putStrLn $ m ++ ": " ++  show e
