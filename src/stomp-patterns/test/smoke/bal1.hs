module Main
where

  import Types
  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import Network.Mom.Stompl.Patterns.Balancer
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)
  import Control.Concurrent
  import Codec.MIME.Type (nullType)
  import System.Environment
  import System.Exit

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [rq, sq] -> withSocketsDo $ tstPub rq sq
      _   -> do
        putStrLn "I need a register queue and service queue and nothing else."
        exitFailure


  tstPub :: QName -> QName -> IO ()
  tstPub rq sq = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withBalancer c "Test" rq {- "/q/reg/1" -} (0,5000) onerr
                            sq {- "/q/olleh/1" -} $ 
        forever $ threadDelay 100000
    where onerr c e m = putStrLn $ show c ++ " in " ++ m ++ ": " ++ show e
