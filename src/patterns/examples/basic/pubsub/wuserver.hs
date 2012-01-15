module Main
where

  import Helper
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B
  import           System.Random (randomRIO)
  import           Control.Concurrent

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    (_, p, _) <- getOs
    withContext 1 $ \ctx -> do
      withPeriodicPub ctx "Weather Report" noparam 100
           (address Bind "tcp" "localhost" p [])
           (return . B.pack)
           onErr_ (\_ -> fetch1 fetch) $ \pub -> untilInterrupt $ do
             threadDelay 100000
             putStrLn $ "Waiting on " ++ srvName pub ++ "..."

  fetch :: FetchHelper () String
  fetch _ _ = do
      -- zipcode <- randomRIO (10000, 99999) :: IO Int
      let zipcode = (10001::Int)
      temperature <- randomRIO (-10, 30) :: IO Int
      humidity    <- randomRIO (10, 60) :: IO Int
      return $ Just (unwords [show zipcode, show temperature, show humidity])
