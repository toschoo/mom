module Main
where

  import           Helper

  import           Network.Mom.Patterns
  import           Database.HDBC.ODBC
  import           Database.HDBC
  import qualified Data.ByteString.Char8 as B
  import           Control.Concurrent
  import           Control.Monad      (when, forever)
  import           System.Posix.Signals

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    (l, p, _) <- getOs
    withContext 1 $ \ctx -> do
      c <- connectODBC "DSN=jose"
      s <- prepare c "select Id, substr(Name, 1, 30) Name from Player" 
      withServer ctx "Player" noparam 5
          (address l "tcp" "localhost" p []) l
          iconv oconv
          onErr (\_  -> one []) (\_ -> dbFetcher s) $ \srv -> do
            stopper <- newMVar False
            _ <- installHandler sigINT (Catch $ handler stopper) Nothing
            go srv stopper
      threadDelay 100000
    where go srv m = do
            putStrLn $ "server " ++ srvName srv ++ " up and running..."
            threadDelay 1000000
            stp <- readMVar m
            when (not stp) $ go srv m
              

  oconv :: OutBound String
  oconv = return . B.pack 

  iconv :: InBound [SqlValue]
  iconv = return . convRow . B.unpack 
    where convRow :: String -> [SqlValue]
          convRow _ = []

  handler :: MVar Bool -> IO ()
  handler m = modifyMVar_ m (\_ -> return True)
