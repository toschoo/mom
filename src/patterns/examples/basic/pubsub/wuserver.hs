module Main
where

  import           Helper(getOs, address, untilInterrupt, onErr_)
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B
  import           System.Random (randomRIO)
  import           Control.Concurrent

  main :: IO ()
  main = do
    (_, p, _) <- getOs
    withContext 1 $ \ctx ->
      withPeriodicPub ctx "Weather Report" noparam 10000
           (address Bind "tcp" "localhost" p [])
           (return . B.pack) onErr_
           (fetch1 fetch) $ \pub -> untilInterrupt $ do
             threadDelay 1000000
             putStrLn $ "Waiting on " ++ srvName pub ++ "..."

  fetch :: FetchHelper' () String
  fetch _ _ _ = do
      -- zipcode <- randomRIO (10000, 99999) :: IO Int 
      let zipcode = 10001::Int -- just publish one topic
      temperature <- randomRIO (-10, 30) :: IO Int
      humidity    <- randomRIO (10, 60) :: IO Int
      return $ unwords [show zipcode, show temperature, show humidity]
