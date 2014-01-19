module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever, when)
  import Codec.MIME.Type (nullType)
  import System.Environment
  import System.Exit

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [rq, sq] -> withSocketsDo $ tstReply rq sq
      _   -> do
        putStrLn "I need a register queue and service queue and nothing else."
        exitFailure

  tstReply :: QName -> QName -> IO ()
  tstReply rq sq = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
      withRegistry c "Test" "/q/registry" (0,1000)
                   onerr $ \reg ->
        withServerThread c "Test" 
                           "olleh" nullType [] createReply
                           (sq,           [], [], iconv)
                           ("unknown",    [], [], oconv)
                           (rq, 500000, (500, 100, 2000))
                           onerr $ 
            withReader c "Server2" "/q/olleh" [] [] iconv $ \r -> 
              withWriter c "Dummy" "unknown"  [] [] oconv $ \w -> forever $ do
                m <- readQ r
                j <- getJobName m
                t <- mapR reg j (sendM w m)
                when (not t) $ putStrLn "No provider" >> showRegistry reg
    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack
          createReply = return . reverse . msgContent
          onerr e m = putStrLn $ "Error in " ++ m ++ show e
          sendM w m p = writeAdHoc w (prvQ p) nullType (msgHdrs    m)
                                                       (msgContent m)
