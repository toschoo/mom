module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)
  import Control.Concurrent
  import System.IO (stdout, hFlush)
  import System.Environment
  import System.Exit

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [rq, sq] -> withSocketsDo $ tstSub rq sq
      _   -> do
        putStrLn "I need a pub queue and a sub queue and nothing else."
        exitFailure

  tstSub :: QName -> QName -> IO ()
  tstSub rq sq = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
      m <- newMVar 0
      withSubMVar c "Test" "Pub1" rq 500000 
                    (sq, [], [], iconv)
                    m onerr $ forever $ waitfor m 0
    where iconv :: InBound Int
          iconv _ _ _ = return . read . B.unpack 
          onerr e m   = putStrLn $ "Error in " ++ m ++ show e
          waitfor m i = do
            i' <- readMVar m
            if i' /= i then    print i' >> waitfor m i'
                       else do putStr ". " >> hFlush stdout
                               threadDelay 100000
                               waitfor m i

