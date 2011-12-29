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
          (\_ _ _ _ -> do putStrLn "An Error occured"
                          return Nothing)
                          -- return $ Just $ B.pack "ERROR!")
          (\_ i -> return i) err2 (\_ _ _ -> return ())

