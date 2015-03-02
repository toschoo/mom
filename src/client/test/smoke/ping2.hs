module Main
where

  import qualified Ping2 as Ping
  import qualified Ping2 as Pong

  import System.Environment
  import Network.Socket (withSocketsDo)
  import Data.Char (isDigit)
 
  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [p,q] -> if all isDigit p 
                 then withSocketsDo $ 
                      Ping.withPingPong (read p) (Pong.ping q)
                 else putStrLn $ "Port is not numeric: " ++ p
      _   -> putStrLn "I need a port and a queue name!"
  
