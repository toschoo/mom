module Main 
where

  ------------------------------------------------------------------------
  -- Playing ping pong
  ------------------------------------------------------------------------

  import           Network.Mom.Patterns
  import qualified Data.Enumerator       as E
  import qualified Data.ByteString.Char8 as B
  import           Control.Concurrent
  import           Control.Monad
  import           Control.Exception (finally)
  import           System.Posix.Signals

  main :: IO ()
  main = do
    m <- newMVar True -- set False on user interrupt
    -- install handler ------------------------------------
    _ <- installHandler sigINT (Catch $ handler m) Nothing
    -- start ping pong ------------------------------------
    withContext 1 $ \ctx -> ping ctx m True
    -- signal handler ------------------------------------
    where handler m = modifyMVar_ m (\_ -> return False)

  data PingPong = Ping | Pong
    deriving (Show, Eq, Read)

  swap :: PingPong -> PingPong
  swap Ping = Pong
  swap Pong = Ping
    
  ping :: Context -> MVar Bool -> Bool -> IO ()
  ping ctx m start = 
    let l = if start then Bind else Connect
     in withPeer ctx 
          (Address "inproc://ping" []) l
          (return . read . B.unpack)
          (return . B.pack . show) $ \p -> do
            x <- newEmptyMVar      -- MVar to wait for second thread
            when start $ starter x -- start second thread 
            if start then go p (Right Ping)     -- start with Ping
                     else receive p it >>= go p -- or wait for Ping
            when start $ takeMVar x -- wait for second thread 
    where go p eix = do
            threadDelay 100000
            case eix of
              Left  e   -> putStrLn $ "Error: " ++ show e
              Right png -> do when start $ print png
                              sendPing p $ if start then swap png else png
                              continue <- readMVar m -- user interrupt?
                              when continue $ receive p it >>= go p
          starter stopped = 
            forkIO (ping ctx m False 
                    `finally` putMVar stopped ()) >>= \_ -> return ()
                              
  it :: E.Iteratee PingPong IO PingPong
  it = one Ping

  sendPing :: Peer PingPong -> PingPong -> IO ()
  sendPing p png = send p (just png)

