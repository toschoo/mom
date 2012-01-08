module Main
where

  import Helper

  import           Network.Mom.Patterns

  import qualified Data.ByteString.Char8 as B
  import qualified Data.Enumerator       as E
  import           Data.Enumerator (($$))
  import qualified System.IO             as IO
  import           Control.Monad.Trans (liftIO)

  main :: IO ()
  main = do
    (l, p, _) <- getOs
    withContext 1 $ \ctx -> do
      serve ctx "Player Service" 5
            (address l "tcp" "localhost" p []) l
            (\_ -> return ()) (return . B.pack)
            onErr (one ())
            (myFetcher "test/out/test.txt")

  myFetcher :: FilePath -> Fetch_ String
  myFetcher p _ _ stp = go Nothing stp
    where go mbh step = 
            case step of
              (E.Continue k) -> do
                h   <- case mbh of
                         Nothing -> liftIO $ IO.openFile p IO.ReadMode
                         Just  x -> return x
                eof <- liftIO $ IO.hIsEOF h
                if eof
                  then cleanup h k
                  else do
                    l <- liftIO (IO.hGetLine h)
                    go (Just h) $$ k (E.Chunks [l])
              _ -> E.returnI step
          cleanup h k = do liftIO $ IO.hClose h
                           E.continue k
                  
