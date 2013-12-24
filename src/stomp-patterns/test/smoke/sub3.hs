module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)
  import Control.Concurrent
  import System.IO (stdout, hFlush)

  main :: IO ()
  main = withSocketsDo tstSub

  tstSub :: IO ()
  tstSub = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
      m <- newMVar 0
      withSubMVar c "Test" "Pub1" "/q/pub1" 500000 
                    ("/q/sub1", [], [], iconv)
                    m onerr $ forever $ waitfor m 0
    where iconv :: InBound Int
          iconv _ _ _ = return . read . B.unpack 
          onerr c e m = putStrLn $ show c ++ " error in " ++ m ++ show e
          waitfor m i = do
            i' <- readMVar m
            if i' /= i then    print i' >> waitfor m i'
                       else do putStr ". " >> hFlush stdout
                               threadDelay 100000
                               waitfor m i

