module Main
where

  import Network.Mom.Stompl.Client.Queue
  import qualified Data.ByteString.Char8 as B
  import System.Environment
  import System.Exit
  import Network.Socket
  import Codec.MIME.Type (nullType)

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q, m] -> withSocketsDo $ conAndSend q m
      _      -> do
        putStrLn "I need a queue name and a message and nothing else."
        exitFailure

  conAndSend :: String -> String -> IO ()
  conAndSend qn m = do
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
      let conv = return . B.pack
      q <- newWriter c "Test-Q" qn [] [] conv
      writeQ q nullType [("message-id","msg-123")] m
