module Main
where

  ------------------------------------------------------------------------
  -- Reads data from a database table (ignoring client input)
  -- and sends the data back, one message per row
  ------------------------------------------------------------------------

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
      c <- connectODBC "DSN=jose" -- jose is a chess database 
                                  -- here, we use ODBC to connect 
                                  -- to a mysql server
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
          convRow _ = []

  ------------------------------------------------------------------------
  -- ODBC example configuration (/etc/odbc.ini)
  ------------------------------------------------------------------------
  -- [jose]
  -- Driver       = /usr/lib/i386-linux-gnu/odbc/libmyodbc.so
  -- Description  = MyODBC 3.51 Driver DSN
  -- SERVER       = localhost
  -- PORT         =
  -- USER         = jose
  -- Password     = jose
  -- Database     = jose
  -- OPTION       = 3
  -- SOCKET       =
  ------------------------------------------------------------------------

