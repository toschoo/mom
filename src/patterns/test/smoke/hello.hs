module Main
where

  import           Network.Mom.Patterns

  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = withContext 1 $ \ctx -> do
    serve ctx 5
          (Address "tcp://*:5555" []) 
          (Just $ Address "inproc://workers" []) 
          (return . B.unpack) (return . B.pack)
          (\_ _ _ _ -> do putStrLn "Error"
                          return Nothing)
          (\_ i -> return i) (once hello) (\_ _ _ -> return ())

  hello :: Context -> String -> IO (Maybe String)
  hello _ = return . Just . reverse

