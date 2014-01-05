module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import System.Environment
  import System.Exit
  import Network.Socket
  import Codec.MIME.Type (nullType)

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q,m] -> withSocketsDo $ tstPush q m
      _   -> do
        putStrLn "I need a queue and a message and nothing else."
        exitFailure

  tstPush :: QName -> String -> IO ()
  tstPush q m = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withPusher c "Test" "olleh" 
                   (q, [], [], oconv) $ \p -> 
        push p nullType [] m
    where oconv       = return . B.pack

