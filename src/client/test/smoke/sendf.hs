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
      [q, f] -> withSocketsDo $ conAndSend q f
      _      -> do
        putStrLn "I need a queue name and a message and nothing else."
        exitFailure

  conAndSend :: String -> String -> IO ()
  conAndSend qn f = do
    m <- B.readFile f
    withConnection_ "127.0.0.1" 61613 1024 "guest" "guest" (0,0) $ \c -> do
      let conv = return 
      q <- newWriter c "Test-Q" qn [] [] conv
      writeQ q nullType [] m
