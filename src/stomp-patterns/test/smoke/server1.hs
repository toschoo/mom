module Main
where

  import Types
  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)
  import Codec.MIME.Type (nullType)
  import System.Environment
  import System.Exit

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q] -> withSocketsDo $ tstReply q
      _   -> do
        putStrLn "I need a queue and nothing else."
        exitFailure


  tstReply :: QName -> IO ()
  tstReply q = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withServer c "Test" (q, [], [], iconv)
                          ("unknown",    [], [], oconv) $ \s -> forever $ 
        reply s (-1) nullType [] createReply
    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack
          createReply = return . reverse . msgContent

