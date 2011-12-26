module Main
where

  import           Network.Mom.Patterns

  import           Database.HDBC.ODBC
  import           Database.HDBC

  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = withContext 1 $ \ctx -> do
    c <- connectODBC "DSN=jose"
    serve ctx 5
          (Address "tcp://*:5555" []) 
          (Just $ Address "inproc://workers" []) 
          (return . B.unpack) (return . B.pack)
          (\_ _ _ _ -> do putStrLn "Error"
                          return Nothing)
          (openS c) (fetcher fetch) closeS 

  openS :: IConnection c => c -> OpenSource String Statement
  openS c _ _ = do
    s  <- prepare c "select Id, substr(Name, 1, 30) Name from Player" 
    _  <- execute s []
    return s

  closeS :: CloseSource String Statement
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

    
