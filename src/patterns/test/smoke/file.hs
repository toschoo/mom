module Main
where

  import           Network.Mom.Patterns

  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = withContext 1 $ \ctx -> do
    serveNoResource ctx 5
          (Address "tcp://*:5555" []) 
          (Just $ Address "inproc://workers" []) 
          (return . B.unpack) return 
          (\_ _ _ _ -> do putStrLn "Error"
                          return Nothing)
          (fileFetcher "test/out/test.txt") 

