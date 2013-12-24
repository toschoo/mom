module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)
  import Control.Concurrent
  import Codec.MIME.Type (nullType)

  main :: IO ()
  main = withSocketsDo tstPub

  tstPub :: IO ()
  tstPub = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
      m <- newMVar 0
      withPubThread c "Test-Pub" "Pub1" "/q/pub1" nullType [] (pub m)
                    ("/q/privatepub1", [], [], oconv) 500000
                    onerr $ forever $ threadDelay 1000000
    where oconv       = return . B.pack . show
          onerr c e m = putStrLn $ show c ++ " error in " ++ m ++ show e
          pub :: MVar Int -> IO Int
          pub m = modifyMVar m $ \i -> return (i+1,i)
