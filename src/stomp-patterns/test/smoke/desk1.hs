module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Desk
  import Network.Socket
  import Control.Monad (forever)
  import Control.Concurrent

  main :: IO ()
  main = withSocketsDo tstPub

  tstPub :: IO ()
  tstPub = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withDesk c "Test" "/q/desks/reg1" (0,5000) onerr
                 "/q/desks/1" $ forever $ threadDelay 100000
    where onerr e m = putStrLn $ "Error in " ++ m ++ ": " ++ show e
