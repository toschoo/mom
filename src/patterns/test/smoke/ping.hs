module Main 
where

  import           Network.Mom.Patterns
  import qualified Data.Enumerator       as E
  import qualified Data.ByteString.Char8 as B
  import           Control.Concurrent
  import           Control.Monad

  main :: IO ()
  main = withContext 1 $ \ctx -> do
      ping ctx True

  data PingPong = Ping | Pong
    deriving (Show, Eq, Read)

  revert :: PingPong -> PingPong
  revert Ping = Pong
  revert Pong = Ping
    
  ping :: Context -> Bool -> IO ()
  ping ctx start = withPeer ctx start
                     (Address "inproc://ping" [])
                     (return . read . B.unpack)
                     (return . B.pack . show) $ \p -> do
      when start $ forkIO (ping ctx False) >>= \_ -> return ()
      if start then go p  (Right Ping)
               else receive p it >>= go p
    where go p eix = do
            threadDelay 100000
            case eix of
              Left  e   -> putStrLn $ "Error: " ++ show e
              Right png -> do when start $ putStrLn (show png) 
                              sendPing p $ if start then revert png else png
                              receive p it >>= go p
                              

  it :: E.Iteratee PingPong IO PingPong
  it = one Ping

  enum :: PingPong -> Fetch () PingPong
  enum p = fetch1 (\_ _ -> return $ Just p)

  sendPing :: Peer PingPong -> PingPong -> IO ()
  sendPing p png = send p (enum png (peerContext p) ())
