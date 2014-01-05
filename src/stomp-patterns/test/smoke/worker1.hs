module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever, when)
  import Codec.MIME.Type (nullType)

  main :: IO ()
  main = withSocketsDo tstTask

  tstTask :: IO ()
  tstTask = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
      withRegistry c "Test" "/q/registry" (0,1000)
                   onerr $ \reg ->
        withTaskThread c "Test" 
                         "olleh" job
                         ("/q/task", [], [], iconv)
                         ("/q/registry", 500000, (500, 100, 2000))
                         onerr $ 
            forever $ 
              withReader c "Task1" "/q/olleh" [] [] iconv $ \r -> 
                withWriter c "Dummy" "unknown" [] [] oconv $ \w -> forever $ do
                  m <- readQ r
                  j <- getJobName m
                  t <- mapR reg j (sendM w m)
                  when (not t) $ do putStrLn "No provider found!"
                                    showRegistry reg
    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack
          job         = putStrLn . reverse . msgContent
          onerr e m   = putStrLn $ "Error in " ++ m ++ show e
          sendM w m p = do putStrLn $ "Provider: " ++ prvQ p
                           writeAdHoc w (prvQ p) nullType (msgHdrs    m) 
                                                          (msgContent m)
