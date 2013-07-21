module Main 
where

  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
  import           Network.Mom.Patterns.Basic.Client
  import           Network.Mom.Patterns.Types

  main :: IO ()
  main = withContext 1 $ \ctx -> 
           withClient ctx "test" "tcp://localhost:5555" Connect $ \c -> do
             mbX <- request c (-1) src snk
             case mbX of
               Nothing -> putStrLn "No Result"
               Just x  -> putStrLn $ "Result: " ++ x
    where src = C.yield (B.pack "hello world")
          snk = do mbX <- C.await 
                   case mbX of
                     Nothing -> return Nothing
                     Just x  -> return $ Just $ B.unpack x
                       
                      

           
