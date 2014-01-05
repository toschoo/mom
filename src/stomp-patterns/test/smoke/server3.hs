module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
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
      [rq, sq] -> withSocketsDo $ tstReply rq sq
      _   -> do
        putStrLn "I need a register queue and service queue and nothing else."
        exitFailure


  tstReply :: QName -> QName -> IO ()
  tstReply rq sq = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
      withServerThread c "Test" 
                         "olleh" nullType [] createReply
                         (sq,           [], [], iconv)
                         ("unknown",    [], [], oconv)
                         (rq, 500000, (500, 100, 2000))
                         onerr $ forever $ threadDelay 100000 
    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack
          createReply = return . reverse . msgContent
          onerr e m = putStrLn $ "Error in " ++ m ++ show e
