module Main
where

  import           Helper (getOs, address, onErr, 
                           dbFetcher, untilInterrupt)
  import           Network.Mom.Patterns
  import           Database.HDBC.ODBC
  import           Database.HDBC
  import qualified Data.ByteString.Char8 as B
  import           Control.Concurrent

  main :: IO ()
  main = do
    (l, p, _) <- getOs
    withContext 1 $ \ctx -> do
      c <- connectODBC "DSN=jose"
      s <- prepare c "select Id, substr(Name, 1, 30) Name from Player" 
      withServer ctx "Player" noparam 5
          (address l "tcp" "localhost" p []) l
          iconv outString onErr (\_  -> one []) 
          (dbFetcher s) $ \srv -> untilInterrupt $ do
              putStrLn $ "server " ++ srvName srv ++ " up and running..."
              threadDelay 1000000
              
  iconv :: InBound [SqlValue]
  iconv = return . convRow . B.unpack 
    where convRow :: String -> [SqlValue]
          convRow _ = [] -- no input parameter, in fact
