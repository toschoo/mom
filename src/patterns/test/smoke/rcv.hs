{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Network.Mom.Patterns
  import qualified System.ZMQ as Z
  import qualified Data.Enumerator       as E
  import qualified Data.Enumerator.List  as EL
  import qualified Data.ByteString.Char8 as B
  import           Control.Exception
  import           Control.Monad.Trans

  main :: IO ()
  main = Z.withContext 1 $ \ctx -> do
    let ap = Address "tcp://localhost:5555" []
    withService ctx ap (return . B.pack) (return . B.unpack) $ \s -> do
      ei <- request s "test" it
      case ei of
        Left e  -> putStrLn $ "Error: " ++ show (e::SomeException)
        Right _ -> return ()

  it :: E.Iteratee String IO ()
  it = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> liftIO (putStrLn i) >> it 
