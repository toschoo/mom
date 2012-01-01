module Main
where

  import           Network.Mom.Patterns

  import qualified Data.ByteString.Char8 as B
  import           System.Random (randomRIO)

  import           Control.Monad (forever)
  import           Control.Concurrent (threadDelay)

  main :: IO ()
  main = withContext 1 $ \ctx -> 
           withPub ctx 
             (Address "tcp://*:5556" [HighWM 100]) 
             (return . B.pack) $ \p -> forever $ do
               issue p (once weather "")
               threadDelay 100000
             

  weather :: String -> IO (Maybe String)
  weather _ = do
      -- zipcode <- randomRIO (10000, 99999) :: IO Int
      let zipcode = (10001::Int)
      temperature <- randomRIO (-10, 30) :: IO Int
      humidity    <- randomRIO (10, 60) :: IO Int
      return $ Just (unwords [show zipcode, show temperature, show humidity])
