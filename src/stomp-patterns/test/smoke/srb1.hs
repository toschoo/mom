module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Bridge
  import Network.Socket
  import Control.Monad (forever)
  import Control.Concurrent

  main :: IO ()
  main = withSocketsDo tstPub

  tstPub :: IO ()
  tstPub = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withServiceBridge c c "Test" "olleh" 
                        "/q/source/olleh/server"
                        "/q/target/olleh/client"
                        "/q/target/olleh/server"
                        ("/q/reg/1", 500000, (500,0,1000))
                        onerr $ forever $ do threadDelay 100000
    where onerr e m = putStrLn $ "Error in " ++ m ++ ": " ++ show e
