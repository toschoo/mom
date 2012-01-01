module Main
where

  import           Network.Mom.Patterns

  import qualified Data.ByteString.Char8 as B
  import qualified Data.Enumerator       as E
  import           Data.Enumerator (($$))
  import qualified System.IO             as IO
  import           Control.Monad.Trans (liftIO)

  main :: IO ()
  main = withContext 1 $ \ctx -> do
    serve ctx "Player Service" 5
          (Address "tcp://*:5555" []) 
          (return . B.unpack) (return . B.pack)
          (\e n _ _ _ -> do putStrLn $ "Error in " ++
                                       n ++ ": " ++ show e
                            return Nothing)
          (one [])
          (\_ _   -> return ()) 
          (myFetcher "test/out/test.txt")
          (\_ _ _ -> return ())

  myFetcher :: FilePath -> Fetch () String
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
                  
