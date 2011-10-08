import Network.Stomp
import qualified Data.ByteString.Lazy.Char8 as B
import System.Environment
import System.Exit

main = do
  os <- getArgs
  case os of
    [q, m] -> sendQ q m
    _      -> do
      putStrLn "I need a queue name and a message and nothing else."
      exitFailure

sendQ :: String -> String -> IO ()
sendQ q m = do
  -- connect to a stomp broker
  con <- connect "stomp://guest:guest@127.0.0.1:61613" vers headers

  -- send the messages to the queue
  send con q [("content-type", "text/plain")] (B.pack m)

  disconnect con []
  where 
    vers = [(1,0),(1,1)]
    headers = []
