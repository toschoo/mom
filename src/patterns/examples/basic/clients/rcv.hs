module Main 
where

  ------------------------------------------------------------------------
  -- May receive input that is sent as request to the server
  -- For instance a file name for the file server!
  ------------------------------------------------------------------------

  import           Helper (getOs, address, outit)
  import           Network.Mom.Patterns
  import           Control.Exception

  main :: IO ()
  main = do
    (l, p, is) <- getOs
    case is of
      [x] -> rcv l p x
      _   -> rcv l p "test"
    
  rcv :: LinkType -> Int -> String -> IO ()
  rcv l p req = withContext 1 $ \ctx -> 
    withClient ctx (address l "tcp" "localhost" p [])
                   outString inString $ \s -> do
      ei <- request s (just req) outit
      case ei of
        Left e  -> putStrLn $ "Error: " ++ show (e::SomeException)
        Right _ -> return ()

