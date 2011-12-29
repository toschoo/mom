module Main
where

  import           Network.Mom.Patterns

  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = withContext 1 $ \ctx -> do
    serve ctx "Error Server" 5
          (Address "tcp://*:5555" []) 
          (return . B.unpack) (return . B.pack)
          (\e n _ _ _ -> do putStrLn $ "Error in " ++
                                       n ++ ": " ++ show e
                            return Nothing)
                          -- return $ Just $ B.pack "ERROR!")
          (\_ i -> return i) err (\_ _ _ -> return ())

