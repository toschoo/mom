module Main
where

  import           Network.Mom.Patterns

  import qualified Data.ByteString.Char8 as B
  import           System.Random (randomRIO)

  main :: IO ()
  main = withContext 1 $ \ctx -> do
           publish ctx "Weather Report " 100
                 (Address "tcp://*:5556" [HighWM 100]) 
                 (return . B.pack)
                 (\e n _ _ -> putStrLn $ "Error in Publisher " ++
                                         n ++ ": " ++ show e)
                 (\_ -> return ()) (once fetch) (\_ _ -> return ())

  fetch :: FetchHelper () String
  fetch _ _ = do
      -- zipcode <- randomRIO (10000, 99999) :: IO Int
      let zipcode = (10001::Int)
      temperature <- randomRIO (-10, 30) :: IO Int
      humidity    <- randomRIO (10, 60) :: IO Int
      return $ Just (unwords [show zipcode, show temperature, show humidity])
