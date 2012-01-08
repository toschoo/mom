module Main
where

  import Helper
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8  as  B
  import qualified Data.Enumerator.Binary as EB

  main :: IO ()
  main = do
    (l, p, _) <- getOs
    withContext 1 $ \ctx -> 
      serve ctx "Player Service" 5
          (address l "tcp" "localhost" p []) l
          (return . B.unpack) return 
          onErr (one []) (\_ f -> EB.enumFile f)

