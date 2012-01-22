module Main 
where

  ------------------------------------------------------------------------
  -- May receive input that is sent as request to the server
  -- For instance a file name for the file server!
  ------------------------------------------------------------------------

  import           Helper (getOs, address, outit)
  import           Network.Mom.Patterns
  import           Control.Exception
  import           Control.Concurrent

  main :: IO ()
  main = do
    (l, p, xs) <- getOs
    case xs of
      [x] -> rcv l p x
      _   -> rcv l p "test"
    
  rcv :: LinkType -> Int -> String -> IO ()
  rcv l p req = withContext 1 $ \ctx -> do
    let ap = address l "tcp" "localhost" p []
    withClient ctx ap outString inString $ \s -> do
      ei <- try $ askFor s (just req)
      case ei of
        Left  e -> putStrLn $ "Error: " ++ show (e::SomeException)
        Right _ -> wait s
    where wait s = checkFor s outit >>= \mbei ->
            case mbei of
              Nothing        -> do putStrLn "Waiting..."
                                   threadDelay 10000 >> wait s
              Just (Left e)  -> putStrLn $ "Error: " ++ show e
              Just (Right _) -> putStrLn "Ready!"

