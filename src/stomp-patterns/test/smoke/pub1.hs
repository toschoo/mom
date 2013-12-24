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
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withPub c "Test1" "Pub1" "/q/pub1" onerr
              ("/q/privatepub1", [], [], oconv) (go 1) 
    where oconv       = return . B.pack . show
          onerr c e m = putStrLn $ show c ++ " error in " ++ m ++ show e
          go :: Int -> PubA Int -> IO ()
          go i p      = do publish p nullType [] i 
                           print i
                           -- threadDelay 500000
                           go (i+1) p

