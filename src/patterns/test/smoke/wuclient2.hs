{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Helper
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = do
    (l, p, ts) <- getOs
    let topic = case ts of
                  [x] -> x       
                  _   -> "10001" 
    withContext 1 $ \ctx -> 
      subscribe ctx "Weather Report" topic
                (address l "tcp" "localhost" p [])
                (return . B.unpack)
                onErr_ output 

