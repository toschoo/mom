module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)
  import Control.Concurrent 
  import System.Environment
  import System.Exit

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [rq, sq] -> withSocketsDo $ tstTask rq sq 
      _   -> do
        putStrLn "I need a registry queue and service queue and nothing else."
        exitFailure


  tstTask :: QName -> QName -> IO ()
  tstTask rq sq = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
        withTaskThread c "Test" 
                         "olleh" job
                         (sq, [], [], iconv)
                         (rq, 500000, (500, 0, 2000))
                         onerr $ 
            forever $ threadDelay 1000000
    where iconv _ _ _ = return . B.unpack
          job         = putStrLn . reverse . msgContent
          onerr e m   = putStrLn $ "Error in " ++ m ++ show e
