{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Network.Mom.Patterns
  import qualified System.ZMQ as Z
  import           System.Environment
  import qualified Data.Enumerator        as E
  import qualified Data.Enumerator.Binary as EB
  import qualified Data.Enumerator.List   as EL
  import qualified Data.ByteString.Char8  as B
  import           Control.Exception
  import           Control.Monad.Trans

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [x] -> doit x
      _   -> error "I need a file name"
    
  doit :: FilePath -> IO ()
  doit f = Z.withContext 1 $ \ctx -> do
    let ap = Address "tcp://localhost:5557" []
    withPipe ctx ap return $ \p -> do
      ei <- push p (EB.enumFile f)
      case ei of
        Left e  -> putStrLn $ "Error: " ++ show (e::SomeException)
        Right _ -> return ()
