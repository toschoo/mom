{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Helper
  import           Network.Mom.Patterns
  import qualified Data.Enumerator       as E
  import qualified Data.ByteString.Char8 as B
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
              (return . B.pack) (return . B.unpack) $ \s -> do
      ei <- request s (enum req) outit
      case ei of
        Left e  -> putStrLn $ "Error: " ++ show (e::SomeException)
        Right _ -> return ()

  enum :: String -> E.Enumerator String IO ()
  enum = once (return . Just)
 
