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
    let dealer = (Address "tcp://*:5555" [])
    let router = (Address "tcp://*:5556" [])
    withContext 1 $ \ctx -> 
      withQueue ctx "Simple Queue" noparam (dealer,router) onErr wait
  
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000

  onErr :: SomeException -> String -> Maybe () -> Maybe B.ByteString -> IO ()
  onErr e nm _ _ = putStrLn $ "Error in Subscription " ++ nm ++ ": " ++ show e
     
