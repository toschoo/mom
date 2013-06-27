module Main 
where

  import           Control.Monad.Trans
  import           Control.Monad (forever)
  import           Control.Concurrent
  import           Control.Applicative ((<$>))
  import qualified Data.Conduit          as C
  import qualified Data.ByteString       as B
  
  import           Network.Mom.Patterns.Basic.Server
  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams

  main :: IO ()
  main = withContext 1 $ \ctx -> 
             withServer ctx "olleh" "tcp://*:5555" Bind
                        (\_     -> return ())
                        showErr
                        bounce $ \_ -> forever $ threadDelay 100000
    where bounce _ = passThrough

  showErr :: OnError_
  showErr _ e m = putStrLn $ m ++ ": " ++  show e
