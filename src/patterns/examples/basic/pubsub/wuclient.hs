module Main 
where

  import           Helper (getOs, address, output, onErr_, untilInterrupt)
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B
  import           Control.Concurrent

  main :: IO ()
  main = do
    (_, p, ts) <- getOs
    let topic = case ts of
                  [x] -> x       
                  _   -> "10001" 
    withContext 1 $ \ctx -> 
      withSub ctx "Weather Report" noparam [topic]
              (address Connect "tcp" "localhost" p [])
              (return . B.unpack)
              onErr_ output $ \s -> untilInterrupt $ do
                putStrLn $ srvName s ++ " up and running..." 
                threadDelay 1000000
           
