module Main 
where

  import           Helper (getPorts, address, onErr_, untilInterrupt)
  import           Network.Mom.Patterns
  import           Control.Concurrent

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    ((l1, p1), (l2, p2), _) <- getPorts
    let puller = address l1 "tcp" "localhost" p1 []
    let pusher = address l2 "tcp" "localhost" p2 []
    withContext 1 $ \ctx -> 
      withPipeline ctx "Pipeline" (puller, l1) (pusher, l2) onErr_ $ \s ->
         untilInterrupt $ do
           putStrLn $ srvName s ++ " up and running ..."
           threadDelay 1000000
