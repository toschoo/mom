module Main 
where

  import           Helper (getOs, address, untilInterrupt)
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
      withSporadicSub ctx
              (address Connect "tcp" "localhost" p [])
              (return . B.unpack) topic $ \s -> untilInterrupt $ do
                putStrLn "checking..."
                mbW <- checkSub s mbOne
                case mbW of
                  Nothing  -> do putStrLn "Nothing found, I'll wait..."
                                 eiw <- waitSub s (one "")
                                 case eiw of
                                   Left  e -> putStrLn $ "Error: " ++ show e
                                   Right w -> putStrLn w 
                                 threadDelay 150000
                  Just eiw -> case eiw of
                                Left  e   -> putStrLn $ "Error: " ++ show e
                                Right mbw -> case mbw of
                                               Nothing -> putStrLn ""
                                               Just w  -> putStrLn w
                 
