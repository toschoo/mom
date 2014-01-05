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
      withForwarder c c "Test" "Pub1" 
                        "/q/source/pub1" 
                        "/q/source/sub1" 
                        "/q/target/pub1" (-1) onerr $
        forever $ do threadDelay 100000
    where onerr e m = putStrLn $ "Error in " ++ m ++ ": " ++ show e
