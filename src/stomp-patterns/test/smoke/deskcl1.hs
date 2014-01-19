module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import Network.Mom.Stompl.Patterns.Desk
  import System.Environment
  import System.Exit
  import Network.Socket

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [m] -> withSocketsDo $ tstRequest m
      _   -> do
        putStrLn "I need a job name and nothing else."
        exitFailure

  tstRequest :: String -> IO ()
  tstRequest j = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withClient c "Test" j
                 ("/q/mychannel", [], [], ignorebody)
                 ("/q/desks/1",   [], [],     nobody) $ \cl -> do
        (sc,qs) <- requestProvider cl (-1) j 1
        case sc of
          OK -> mapM_ putStrLn qs
          _  -> putStrLn $ "Error: " ++ show sc

