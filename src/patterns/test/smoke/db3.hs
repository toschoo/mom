module Main
where

  import           Network.Mom.Patterns

  import           Database.HDBC.ODBC
  import           Database.HDBC

  import qualified Data.ByteString.Char8 as B

  import           Control.Concurrent (threadDelay)
  import           Control.Monad      (forever)

  noparam :: String
  noparam = ""

  main :: IO ()
  main = withContext 1 $ \ctx -> do
    c <- connectODBC "DSN=jose"
    s <- prepare c "select Id, substr(Name, 1, 30) Name from Player" 
    withServer ctx "Player" noparam 5
          (Address "tcp://*:5555" []) 
          (Just $ Address "inproc://workers" []) 
          iconv oconv
          (\e n _ _ _ -> do putStrLn $ "Error in Server " ++
                                       n ++ ": " ++ show e
                            return Nothing)
          (\_ -> dbExec s) (\_ -> dbFetcher) (\_ -> dbClose) $ 
          \srv -> forever $ do
            putStrLn $ "server " ++ srvName srv ++ " up and running..."
            threadDelay 1000000
          

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

  iconv :: InBound [SqlValue]
  iconv = return . convRow . B.unpack 
    where convRow :: String -> [SqlValue]
          convRow _ = []


    
