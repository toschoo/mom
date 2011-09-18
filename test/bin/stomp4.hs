module Main
where

  import System.Environment (getArgs)
  import Network.Stompl.Parser
  import Network.Stomp.Frame
  import Data.Attoparsec
  import qualified Data.ByteString.UTF8  as U
  import qualified Data.ByteString       as B
  import qualified Data.ByteString.Char8 as BC

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      []  -> error "I need a file name"
      [f] -> do 
        s <- B.readFile f
        putStrLn $ show $ parseit s
      _ -> error "I don't know what to do with all the arguments!"   

  parseit :: B.ByteString -> Frame
  parseit b = 
    case parse stompParser b of
      Fail s c e -> error e
      Done s r   -> r
      Partial r  -> case feed (Partial r) (BC.pack "\x00") of
                     Fail s c e  -> error e
                     Done s r    -> r
                     Partial r   -> error "I give up"  


