{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Network.Mom.Patterns
  import           System.Environment
  import qualified Data.Enumerator       as E
  import qualified Data.Enumerator.List  as EL
  import qualified Data.ByteString.Char8 as B
  import           Control.Monad.Trans
  import           Control.Concurrent
  import           Control.Monad
  import           Control.Exception

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    os <- getArgs
    let topic = case os of
                  [x] -> x       
                  _   -> "10001" 
    withContext 1 $ \ctx -> 
      withSub ctx "Weather Report" noparam topic
              (Address "tcp://localhost:5557" [])
              (return . B.unpack)
              (\e nm _ _ -> putStrLn $ "Error in Subscription " ++ nm ++ 
                                       ": " ++ show e)
              output wait
  
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000
           
  output :: String -> Dump String
  output p c = do
    -- _ <- liftIO (throwIO $ AssertionFailed "Test!!!")
    -- _ <- E.throwError (AssertionFailed "Test!!!")
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> liftIO (putStrLn i) >> output p c 

