{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Helper
  import           Network.Mom.Patterns
  import qualified Data.Enumerator       as E
  import qualified Data.ByteString.Char8 as B
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
    withClient ctx ap (return . B.pack) (return . B.unpack) $ \s -> do
      ei <- try $ askFor s (enum req)
      case ei of
        Left  e -> putStrLn $ "Error: " ++ show (e::SomeException)
        Right _ -> wait s
    where wait s = checkFor s outit >>= \mbei ->
            case mbei of
              Nothing        -> do putStrLn "Waiting..."
                                   threadDelay 10000 >> wait s
              Just (Left e)  -> putStrLn $ "Error: " ++ show e
              Just (Right _) -> putStrLn "Ready!"

  enum :: String -> E.Enumerator String IO ()
  enum = once (return . Just)
 
