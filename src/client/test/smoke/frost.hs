module Main
where

  import Network.Mom.Stompl.Client.Queue
  import qualified Data.ByteString.Char8 as B
  import System.Environment
  import System.Exit
  import Network.Socket
  import Codec.MIME.Type    (nullType)
  import Control.Monad      (forever)
  import Control.Concurrent (threadDelay)

  delay :: Int
  delay = 500000

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q] -> withSocketsDo $ conAndSend q 
      _   -> do
        putStrLn "I need a queue name."
        exitFailure

  conAndSend :: String -> IO ()
  conAndSend qn = do
    withConnection_ "127.0.0.1" 61613 1024 "guest" "guest" (0,0) $ \c -> do
      let conv = OutBound (return . B.pack)
      q <- newQueue c "Test-Q" qn [OSend] [] conv
      forever $ frost q

  frost :: Queue String -> IO ()
  frost q = do
    threadDelay delay
    writeQ q nullType [] ""
    threadDelay delay
    writeQ q nullType [] ""
    threadDelay delay
    writeQ q nullType [] ""
    threadDelay delay
    writeQ q nullType [] "Stopping by Woods on a Snowy Evening"
    threadDelay delay
    writeQ q nullType [] ""
    threadDelay delay
    writeQ q nullType [] "whose woods these are I think I know"
    threadDelay delay
    writeQ q nullType [] "  his house is in the village though"
    threadDelay delay
    writeQ q nullType [] "    he will not see me stopping here"
    threadDelay delay
    writeQ q nullType [] "      to watch his woods fill up with snow"
    threadDelay delay
    writeQ q nullType [] ""
    threadDelay delay
    writeQ q nullType [] "my little horse must think it queer"
    threadDelay delay
    writeQ q nullType [] "  to stop without a farm house near"
    threadDelay delay
    writeQ q nullType [] "    between the woods and frozen lake"
    threadDelay delay
    writeQ q nullType [] "      the darkest evening of the year"
    threadDelay delay
    writeQ q nullType [] ""
    threadDelay delay
    writeQ q nullType [] "he gives his harness bell a shake"
    threadDelay delay
    writeQ q nullType [] "  to ask if there is some mistake"
    threadDelay delay
    writeQ q nullType [] "    the only other sounds the sweep"
    threadDelay delay
    writeQ q nullType [] "      of easy wind and downy flake"
    threadDelay delay
    writeQ q nullType [] ""
    threadDelay delay
    writeQ q nullType [] "the woods are lovely, dark and deep"
    threadDelay delay
    writeQ q nullType [] "  but I have promises to keep"
    threadDelay delay
    writeQ q nullType [] "    and miles to go before I sleep"
    threadDelay delay
    writeQ q nullType [] "      and miles to go before I sleep"
    
