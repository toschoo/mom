module Main
where

  import           Network.Mom.Patterns

  import           Database.HDBC.ODBC
  import           Database.HDBC

  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = withContext 1 $ \ctx -> do
    c <- connectODBC "DSN=jose"
    serve ctx "Player Service" 5
          (Address "tcp://localhost:5556" []) Connect
          (return . B.unpack) (return . B.pack)
          (\e n _ _ _ -> do putStrLn $ "Error in Server " ++
                                       n ++ ": " ++ show e
                            return Nothing)
          (one []) (openS c) (fetcher fetch) closeS 

  openS :: IConnection c => c -> OpenSourceIO String Statement
  openS c _ _ = do
    s  <- prepare c "select Id, substr(Name, 1, 30) Name from Player" 
    _  <- execute s []
    return s

  closeS :: CloseSourceIO String Statement
  closeS _ _ _ = return ()

  fetch :: FetchHelper Statement String
  fetch _ s = do
    mbR <- fetchRow s
    case mbR of
      Nothing -> return Nothing
      Just r  -> return (Just $ convRow r)
    where convRow :: [SqlValue] -> String
          convRow [sqlId, sqlName] =
            show idf ++ ": " ++ name
            where idf  = (fromSql sqlId)::Int
                  name = case fromSql sqlName of
                           Nothing -> "NN"
                           Just r  -> r
          convRow _ = undefined

    
