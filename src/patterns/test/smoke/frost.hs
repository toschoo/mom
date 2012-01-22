module Main
where

  import Helper
  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = do
    (l, p, _) <- getOs
    withContext 1 $ \ctx -> do
      serve ctx "Frost" 5
          (address l "tcp" "localhost" p []) l
          (\_ -> return ()) (return . B.pack)
          onErr (one ()) (listFetcher $ lines frost)

  frost, t, s, b :: String
  frost =  s ++ s ++ s ++ s ++ s ++ t ++ s ++ s ++ s ++ b
  t   = "Stopping by Woods on a Snowy Evening\n"
  s   = "\n"
  b   = "whose woods these are I think I know\n"
      ++ "  his house is in the village though\n"
      ++ "    he will not see me stopping here\n"
      ++ "      to watch his woods fill up with snow\n"
      ++ s
      ++ "my little horse must think it queer\n"
      ++ "  to stop without a farm house near\n"
      ++ "    between the woods and frozen lake\n"
      ++ "      the darkest evening of the year\n"
      ++ s
      ++ "he gives his harness bell a shake\n"
      ++ "  to ask if there is some mistake\n"
      ++ "    the only other sounds the sweep\n"
      ++ "      of easy wind and downy flake\n"
      ++ s
      ++ "the woods are lovely, dark and deep\n"
      ++ "  but I have promises to keep\n"
      ++ "    and miles to go before I sleep\n"
      ++ "      and miles to go before I sleep\n"
      ++ s
      ++ s
      ++ s
    