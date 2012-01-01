{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Network.Mom.Patterns
  import qualified System.ZMQ as Z
  import           System.Environment
  import qualified Data.Enumerator       as E
  import qualified Data.Enumerator.List  as EL
  import qualified Data.ByteString.Char8 as B
  import           Control.Exception
  import           Control.Concurrent
  import           Control.Monad.Trans

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [x] -> rcv x
      _   -> rcv "test"
    
  rcv :: String -> IO ()
  rcv req = Z.withContext 1 $ \ctx -> do
    let ap = Address "tcp://localhost:5555" []
    withClient ctx ap (return . B.pack) (return . B.unpack) $ \s -> do
      ei <- try $ askFor s (enum req)
      case ei of
        Left  e -> putStrLn $ "Error: " ++ show (e::SomeException)
        Right _ -> wait s
    where wait s = checkFor s it >>= \mbei ->
            case mbei of
              Nothing        -> do putStrLn "Waiting..."
                                   threadDelay 10000 >> wait s
              Just (Left e)  -> putStrLn $ "Error: " ++ show e
              Just (Right _) -> putStrLn "Ready!"

  it :: E.Iteratee String IO ()
  it = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> liftIO (putStrLn i) >> it

  enum :: String -> E.Enumerator String IO ()
  enum = once (return . Just)
 
