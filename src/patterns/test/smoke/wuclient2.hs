{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Network.Mom.Patterns
  import           System.Environment
  import qualified Data.Enumerator.List  as EL
  import qualified Data.ByteString.Char8 as B
  import           Control.Monad.Trans

  main :: IO ()
  main = do
    os <- getArgs
    let topic = case os of
                  [x] -> x       
                  _   -> "10001" 
    withContext 1 $ \ctx -> 
      subscribe ctx "Weather Report" topic
                (Address "tcp://localhost:5556" [])
                (return . B.unpack)
                (\e nm _ _ -> putStrLn $ "Error in Subscription " ++ nm ++ 
                                         ": " ++ show e)
                output 

  output :: Dump String 
  output c = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> liftIO (putStrLn i) >> output c 
