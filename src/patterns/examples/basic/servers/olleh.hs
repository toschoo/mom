module Main
where

  ------------------------------------------------------------------------
  -- makes semordnilap
  ------------------------------------------------------------------------

  import           Helper (getOs, address, onErr, untilInterrupt)
  import           Control.Concurrent (threadDelay)
  import           Network.Mom.Patterns

  main :: IO ()
  main = do
    (l, p, _) <- getOs 
    withContext 1 $ \ctx ->
      withServer ctx "Olleh Server" noparam 5
          (address l "tcp" "localhost" p []) l
          inString outString onErr (\_ -> one []) 
          (fetch1 olleh) $ \s -> untilInterrupt $ do
            putStrLn $ srvName s ++ " makes semordnilap"
            threadDelay 1000000

  olleh :: FetchHelper' String String
  olleh _ _ = return . reverse

