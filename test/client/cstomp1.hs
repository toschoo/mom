import Network.Stomp
import qualified Data.ByteString.Lazy.Char8 as B

main = do
  -- connect to a stomp broker
  con <- connect "stomp://guest:guest@127.0.0.1:61613" vers headers
  putStrLn $ "Accepted versions: " ++ show (versions con)
  
  sendUserMsg con "/queue/test"

  -- unsubscribe and disconnect
  disconnect con []
  where 
    vers = [(1,0),(1,1)]
    headers = []

sendUserMsg :: Connection -> String -> IO ()
sendUserMsg c q = do
  l <- getLine
  if l == "quit" || l == "q"
    then return ()
    else do
      send c q [] $ B.pack l
      sendUserMsg c q
