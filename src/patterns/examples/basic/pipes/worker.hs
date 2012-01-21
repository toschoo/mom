{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Helper (getOs, address, onErr_, 
                           output, untilInterrupt)
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B
  import           Control.Concurrent (threadDelay)

  main :: IO ()
  main = do
    (l, p, _) <- getOs
    withContext 1 $ \ctx -> 
      withPuller ctx "Worker" noparam 
            (address l "tcp" "localhost" p [])
            (return . B.unpack)
            onErr_ output $ \s -> untilInterrupt $ do
              putStrLn $ srvName s ++ " is working"
              threadDelay 1000000
           
