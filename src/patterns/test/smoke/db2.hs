module Main
where

  import           Network.Mom.Patterns

  import           Database.HDBC.ODBC
  import           Database.HDBC

  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = withContext 1 $ \ctx -> do
    c <- connectODBC "DSN=jose"
    s <- prepare c "select Id, substr(Name, 1, 30) Name from Player" 
    serveNoResource ctx 5
          (Address "tcp://*:5555" []) 
          (Just $ Address "inproc://workers" []) 
          (return . B.unpack) oconv
          (\_ _ _ _ -> do putStrLn "Error"
                          return Nothing)
          (dbFetcher s [])

  oconv :: OutBound [SqlValue]
  oconv = return . B.pack . convRow
    where convRow :: [SqlValue] -> String
          convRow [sqlId, sqlName] =
            show idf ++ ": " ++ name
            where idf  = (fromSql sqlId)::Int
                  name = case fromSql sqlName of
                           Nothing -> "NN"
                           Just r  -> r
          convRow _ = undefined

    
