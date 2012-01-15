module Main 
where

  import           Helper (getOs, address)
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B
  import           Control.Exception

  main :: IO ()
  main = do
    (l, p, _) <- getOs 
    withContext 1 $ \ctx -> do
      let ap = address l "tcp" "localhost" p []
      withClient ctx ap (return . B.pack) (return . B.unpack) $ \c -> do
        ei <- request c enum toList
        case ei of
          Left  e -> putStrLn $ "Error: " ++ show (e::SomeException)
          Right x -> mapM_ putStrLn x
    where enum = once (return . Just) "test"
