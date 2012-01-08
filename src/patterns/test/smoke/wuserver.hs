module Main
where

  import Helper
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B
  import           System.Random (randomRIO)

  main :: IO ()
  main = do
    (l, p, _) <- getOs
    withContext 1 $ \ctx -> do
      publish ctx "Weather Report " 100
         (address l "tcp" "localhost" p [HighWM 100])
         (return . B.pack)
         onErr_ (fetch1 fetch)

  fetch :: FetchHelper () String
  fetch _ _ = do
      -- zipcode <- randomRIO (10000, 99999) :: IO Int
      let zipcode = (10001::Int)
      temperature <- randomRIO (-10, 30) :: IO Int
      humidity    <- randomRIO (10, 60) :: IO Int
      return $ Just (unwords [show zipcode, show temperature, show humidity])
