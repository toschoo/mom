module Main 
where

  import           Network.Mom.Device -- Patterns.Device
  import           Network.Mom.Patterns -- Patterns.Device
  import           System.Environment
  import qualified Data.Enumerator       as E
  import qualified Data.Enumerator.List  as EL
  import qualified Data.ByteString.Char8 as B
  import           Control.Monad.Trans
  import           Control.Concurrent
  import           Control.Monad
  import           Control.Exception

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    let sub = Address "tcp://localhost:5556" []
    let pub = Address "tcp://*:5557"         []
    withContext 1 $ \ctx -> 
      withForwarder ctx "Weather Forwarder" noparam "" (sub,pub) onErr wait
  
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000
     
  onErr :: OnError_ () B.ByteString
  onErr e nm _ _ = putStrLn $ "Error in Forwarder " ++ nm ++ ": " ++ show e
