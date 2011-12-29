module Main
where

  import           Network.Mom.Patterns

  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = withContext 1 $ \ctx -> do
    serve ctx "Player Service" 5
          (Address "tcp://*:5555" []) 
          (return . B.unpack) return 
          (\e n _ _ _ -> do putStrLn $ "Error in " ++
                                       n ++ ": " ++ show e
                            return Nothing)
          fileOpen fileFetcher fileClose

