module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import Network.Mom.Stompl.Patterns.Balancer
  import Network.Socket
  import Control.Monad (forever)
  import Control.Concurrent
  import System.Environment
  import System.Exit

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [rq, sq] -> withSocketsDo $ tstPub rq sq
      _   -> do
        putStrLn "I need a register queue and service queue and nothing else."
        exitFailure


  tstPub :: QName -> QName -> IO ()
  tstPub rq sq = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withBalancer c "Test" rq (0,5000) sq onerr $
        forever $ threadDelay 5000000
    where onerr e m = putStrLn $ "Error in " ++ m ++ ": " ++ show e
