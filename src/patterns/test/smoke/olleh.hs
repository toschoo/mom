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
           queue ctx $ \s -> 
             -- withServer ctx "olleh" "tcp://*:5555" Bind
             withServer ctx "olleh" "inproc://srv" Connect
                        (\_     -> return ())
                        (\_ _ _ -> return ()) 
                        olleh $ \_ -> forever $ threadDelay 100000
    where olleh s = C.awaitForever (C.yield . B.reverse)

  queue :: Context -> (Controller -> IO ()) -> IO ()
  queue ctx = withQueue ctx "queue" 
                        ("tcp://*:5555", Bind)
                        ("inproc://srv", Bind)
                        (\_ _ _ -> return ()) 
                        
                       
                      

           
