module Main
where

  import Network.Mom.Patterns
  import System.ZMQ
  import qualified Data.ByteString as B

  main :: IO ()
  main = withContext 1 $ \ctx -> do
    serve ctx 5
          (Address "tcp://*:5555" [])
          (Just $ Address "inproc://workers" [])
          (return . B.unpack) (return . B.pack)
          (\_ _ _ -> do putStrLn "Error"
                        return Nothing)
          (\_ -> return . reverse)
    
    
