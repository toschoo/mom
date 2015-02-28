module Main
where

  import qualified Ping2 as Ping
  import qualified Ping2 as Pong

  import System.Environment
  import Network.Socket (withSocketsDo)
 
  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q] -> withSocketsDo $ Ping.withPingPong (Pong.ping q)
      _   -> putStrLn "I need a queue name!"
  
