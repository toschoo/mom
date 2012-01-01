module Main
where

  import           Network.Mom.Patterns

  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = withContext 1 $ \ctx -> do
    serve ctx "Hello World Server" 5
          (Address "tcp://*:5555" []) 
          (return . B.unpack) (return . B.pack)
          (\e n _ _ _ -> do putStrLn $ "Error in " ++
                                       n ++ ": " ++ show e
                            return Nothing)
          (one []) (\_ i -> return i) (fetch1 hello) (\_ _ _ -> return ())

  hello :: Context -> String -> IO (Maybe String)
  hello _ = return . Just . reverse

