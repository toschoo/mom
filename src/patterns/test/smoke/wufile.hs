{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Network.Mom.Patterns
  import           System.Environment
  import qualified Data.Enumerator.List  as EL
  import qualified Data.ByteString.Char8 as B
  import           Control.Monad.Trans
  import           Control.Concurrent
  import           Control.Monad
  import           Control.Applicative ((<$>))

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
              (Address "tcp://localhost:5556" [])
              (return . B.unpack)
              (\e nm _ _ -> putStrLn $ "Error in Subscription " ++ nm ++ 
                                       ": " ++ show e)
              output wait
  
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000
           
  output :: String -> Dump String
  output _ _ = go Nothing
    where go mbf = do
            mbi <- EL.head
            case mbi of
              Nothing -> return ()
              Just i  -> do
                f <- case mbf of
                       Nothing -> liftIO (selectFile i) 
                       Just f  -> return f
                liftIO (appendFile f $ i ++ "\n") >> go (Just f)

  selectFile :: String -> IO String
  selectFile _ = return "test/out/sub.txt"

