module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)
  import Control.Concurrent

  main :: IO ()
  main = withSocketsDo tstSub

  tstSub :: IO ()
  tstSub = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withSubThread c "Test" "Pub1" "/q/pub1" 500000 
                      ("/q/sub1", [], [], iconv)
                      outmsg onerr $ forever $ threadDelay 1000000
    where iconv :: InBound Int
          iconv _ _ _ = return . read . B.unpack 
          onerr c e m = putStrLn $ show c ++ " error in " ++ m ++ show e
          outmsg    m = print (msgContent m)

