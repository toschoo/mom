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
  main = withSocketsDo tstReply 

  tstReply :: IO ()
  tstReply = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
      withServerThread c "Test" 
                         "olleh" nullType [] createReply
                         ("/q/server4", [], [], iconv)
                         ("unknown",    [], [], oconv)
                         ("/q/reg/1", 500000, (500, 100, 2000))
                         onerr $ forever $ threadDelay 100000 
    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack
          createReply = return . reverse . msgContent
          onerr e m   = putStrLn $ "Error in " ++ m ++ show e
