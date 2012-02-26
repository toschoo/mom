module Main 
where

  import           Helper (getOs, address)
  import           Network.Mom.Patterns
  import           Control.Exception

  main :: IO ()
  main = do
    (l, p, xs) <- getOs
    withContext 1 $ \ctx -> do
      let ap = address l "tcp" "localhost" p []
      withClient ctx ap outString inString $ \c -> do
        ei <- request c (mkRequest xs) append
        case ei of
          Left  e -> putStrLn $ "Error: " ++ show (e::SomeException)
          Right m -> putStrLn   m
    where mkRequest xs = if null xs then just "test" else just $ head xs
