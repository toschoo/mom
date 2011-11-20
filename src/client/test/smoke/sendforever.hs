module Main
where

  import Network.Mom.Stompl.Client.Queue
  import qualified Data.ByteString.Char8 as B
  import System.Environment
  import System.Exit
  import Network.Socket
  import Codec.MIME.Type    (nullType)
  import Control.Concurrent (threadDelay)
  import Control.Monad      (forever)

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
    withConnection_ "127.0.0.1" 61613 [] $ \c -> do
      let conv = return . B.pack
      q <- newWriter c "Test-Q" qn [] [] conv
      forever $ do
        writeQ q nullType [] m
        threadDelay 500000
