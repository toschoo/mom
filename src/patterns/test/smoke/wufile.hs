{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Helper
  import           Network.Mom.Patterns
  import qualified Data.Enumerator.List  as EL
  import qualified Data.ByteString.Char8 as B
  import           Control.Monad.Trans
  import           Control.Concurrent
  import           Control.Monad

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    (l, p, ts) <- getOs
    let topic = case ts of
                  [x] -> x       
                  _   -> "10001" 
    withContext 1 $ \ctx -> 
      withSub ctx "Weather Report" noparam topic
              (address l "tcp" "localhost" p [])
              (return . B.unpack)
              onErr_ toFile wait
  
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000
           
  toFile :: String -> Dump String
  toFile _ _ = go Nothing
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

