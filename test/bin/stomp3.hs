module Main
where

  import System.Environment (getArgs)
  import Network.Stompl.Parser
  import Network.Stomp.Frame
  import Data.Attoparsec
  import qualified Data.ByteString.UTF8 as U
  import qualified Data.ByteString      as B

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      []  -> error "I need a file name"
      [f] -> do 
        s <- B.readFile f
        parseTest stompParser s
      _ -> error "I don't know what to do with all the arguments!"   


