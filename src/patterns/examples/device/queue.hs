module Main 
where

  import           Helper (getPorts, untilInterrupt, address, onErr_)
  import           Network.Mom.Patterns
  import           Control.Concurrent

  main :: IO ()
  main = do
    ((l1, p1), (l2, p2), _) <- getPorts
    let dealer = address l1 "tcp" "localhost" p1 []
    let router = address l2 "tcp" "localhost" p2 []
    withContext 1 $ \ctx -> 
      withQueue ctx "Simple Queue" (dealer, l1) (router, l2) onErr_ $ \s ->
        untilInterrupt $ do
          putStrLn $ srvName s ++ " up and running..."
          threadDelay 1000000
