{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Helper
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B
  import           Control.Concurrent
  import           Control.Monad

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    (l, p, _) <- getOs
    withContext 1 $ \ctx -> 
      withPuller ctx "Worker" noparam 
            (address l "tcp" "localhost" p [])
            (return . B.unpack)
            onErr_ (\_ -> output) wait
         
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000
           
