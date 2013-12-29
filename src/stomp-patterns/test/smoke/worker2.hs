module Main
where

  import Types
  import Registry
  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever, when)
  import Control.Concurrent 
  import Codec.MIME.Type (nullType)
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
          onerr c e m = putStrLn $ show c ++ " error in " ++ m ++ show e
