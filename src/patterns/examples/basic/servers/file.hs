module Main
where
  ------------------------------------------------------------------------
  -- Sends the contents of a file back to the client;
  -- Client request must be the name of an existing file
  ------------------------------------------------------------------------

  import           Helper (getOs, address, untilInterrupt, onErr)
  import           Network.Mom.Patterns
  import           Control.Concurrent (threadDelay)
  import qualified Data.ByteString.Char8  as  B
  import qualified Data.Enumerator.Binary as EB

  main :: IO ()
  main = do
    (l, p, _) <- getOs
    withContext 1 $ \ctx -> 
      withServer ctx "Player Service" noparam 5
          (address l "tcp" "localhost" p []) l
          (return . B.unpack) return 
          onErr (\_ -> one []) (\_ _ f -> EB.enumFile f) $ \s -> 
            untilInterrupt $ do
              putStrLn $ srvName s ++ " waiting for interrupt..." 
              threadDelay 1000000
