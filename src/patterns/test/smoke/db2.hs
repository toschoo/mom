module Main
where

  import           Helper 
  import           Network.Mom.Patterns

  import           Database.HDBC.ODBC
  import           Database.HDBC
  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = do
    (l, p, _) <- getOs
    withContext 1 $ \ctx -> do
      c <- connectODBC "DSN=jose"
      s <- prepare c "select Id, substr(Name, 1, 30) Name from Player" 
      serve ctx "Player" 5
          (address l "tcp" "localhost" p []) l
          iconv oconv
          onErr (one []) (dbFetcher s)

  oconv :: OutBound String
  oconv = return . B.pack 

  iconv :: InBound [SqlValue]
  iconv = return . convRow . B.unpack 
    where convRow :: String -> [SqlValue]
          convRow _ = []
    
