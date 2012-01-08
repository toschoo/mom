module Main
where

  import Helper
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = do
    (l, p, _) <- getOs
    withContext 1 $ \ctx -> do
      serve ctx "Error Server" 5
          (address l "tcp" "localhost" p []) l
          (\_ -> return ()) (return . B.pack)
          onErr (one ()) err

