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
      [m] -> withSocketsDo $ tstPush m
      _   -> do
        putStrLn "I need a message and nothing else."
        exitFailure

  tstPush :: String -> IO ()
  tstPush m = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withPusher c "Test" "olleh" 
                   ("/q/olleh", [], [], oconv) $ \p -> 
        push p nullType [] m
    where oconv       = return . B.pack

