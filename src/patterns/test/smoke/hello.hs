module Main
where

  import Helper
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = do
    (l, p, _) <- getOs 
    withContext 1 $ \ctx -> do
    serve ctx "Hello World Server" 5
          (address l "tcp" "localhost" p []) l
          (return . B.unpack) (return . B.pack)
          onErr (one []) (fetch1 hello)

  hello :: FetchHelper String String -- Context -> String -> IO (Maybe String)
  hello _ = return . Just . reverse

