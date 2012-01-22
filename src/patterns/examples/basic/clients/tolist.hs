module Main 
where

  import           Helper (getOs, address)
  import           Network.Mom.Patterns
  import           Control.Exception

  main :: IO ()
  main = do
    (l, p, _) <- getOs 
    withContext 1 $ \ctx -> do
      let ap = address l "tcp" "localhost" p []
      withClient ctx ap outString inString $ \c -> do
        ei <- request c (just "test") toList
        case ei of
          Left  e -> putStrLn $ "Error: " ++ show (e::SomeException)
          Right x -> mapM_ putStrLn x
