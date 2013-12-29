module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import Network.Mom.Stompl.Patterns.Bridge
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)
  import Control.Concurrent
  import Codec.MIME.Type (nullType)

  main :: IO ()
  main = withSocketsDo tstPub

  tstPub :: IO ()
  tstPub = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withTaskBridge c c "Test" "olleh" 
                         "/q/source/olleh/task"
                         "/q/target/olleh/task"
                         ("/q/reg/1", 500000, (500,0,1000))
                         onerr $ forever $ do threadDelay 100000
    where onerr c e m = putStrLn $ show c ++ " in " ++ m ++ ": " ++ show e
