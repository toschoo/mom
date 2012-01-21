module Main
where

  ------------------------------------------------------------------------
  -- reverses the request string
  ------------------------------------------------------------------------

  import           Helper (getOs, address, onErr, untilInterrupt)
  import           Control.Concurrent (threadDelay)
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = do
    (l, p, _) <- getOs 
    withContext 1 $ \ctx -> do
    withServer ctx "Hello World Server" noparam 5
          (address l "tcp" "localhost" p []) l
          (return . B.unpack) (return . B.pack)
          onErr (\_ -> one []) 
          (fetch1 hello) $ \s -> untilInterrupt $ do
            putStrLn $ srvName s ++ " makes semordnilap"
            threadDelay 1000000

  hello :: FetchHelper String String -- Context -> String -> IO (Maybe String)
  hello _ _ = return . Just . reverse

