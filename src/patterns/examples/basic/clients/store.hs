module Main 
where

  import           Helper (getOs, address)
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B
  import           Control.Exception

  main :: IO ()
  main = do
    (l, p, ns) <- getOs
    case ns of 
      [n] -> withContext 1 $ \ctx -> do
               let ap = address l "tcp" "localhost" p []
               withClient ctx ap 
                          (return . B.pack) 
                          (return . B.unpack) $ \c -> do
                 ei <- request c (just "") (store $ save n)
                 case ei of
                   Left e  -> putStrLn $ "Error: " ++ show (e::SomeException)
                   Right _ -> return ()
      _ -> error "I need a file name!"

  save :: FilePath -> String -> IO ()
  save p s = appendFile p (s ++ "\n")
