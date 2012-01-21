module Main 
where

  import           Helper (getPorts, address, untilInterrupt, onErr_)
  import           Network.Mom.Patterns
  import           Control.Concurrent

  main :: IO ()
  main = do
    ((l1, p1), (l2, p2), _) <- getPorts
    let sub = address l1 "tcp" "localhost" p1 []
    let pub = address l2 "tcp" "localhost" p2 []
    withContext 1 $ \ctx -> 
      withForwarder ctx "Weather Forwarder" "" 
                    (sub, l1) (pub, l2) onErr_ $ \s ->
        untilInterrupt $ do
          putStrLn $ srvName s ++ " up and running..."
          threadDelay 1000000
