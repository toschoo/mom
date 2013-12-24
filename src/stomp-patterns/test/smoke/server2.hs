module Main
where

  import Registry
  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever, when)
  import Control.Concurrent 
  import Codec.MIME.Type (nullType)

  main :: IO ()
  main = withSocketsDo tstReply 

  tstReply :: IO ()
  tstReply = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
      withRegistry c "Test" "/q/registry" (0,1000)
                   onerr $ \reg ->
        withServerThread c "Test" 
                           "olleh" nullType [] createReply
                           ("/q/request", [], [], iconv)
                           ("unknown",    [], [], oconv)
                           ("/q/registry", 500000, (500, 100, 2000))
                           onerr $ 
            withReader c "Server1" "/q/olleh" [] [] iconv $ \r -> 
              withWriter c "Dummy" "unknown"  [] [] oconv $ \w -> forever $ do
                m <- readQ r
                j <- getJobName m
                t <- mapR reg j (sendM w m)
                when (not t) $ putStrLn "No provider" >> showRegistry reg
    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack
          createReply = return . reverse . msgContent
          onerr c e m = putStrLn $ show c ++ " error in " ++ m ++ show e
          sendM w m p = writeAdHoc w (prvQ p) nullType (msgHdrs    m)
                                                       (msgContent m)
