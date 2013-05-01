module Main 
where

  import           Control.Monad.Trans
  import           Control.Monad (forever)
  import           Control.Concurrent
  import           Control.Exception   (SomeException)
  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
  
  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
  import           Network.Mom.Patterns.Basic.Pusher
  import           Network.Mom.Patterns.Basic.Puller

  pus, pul :: String
  pus = "inproc://__pus" -- "tcp://*:5556"
  pul = "inproc://__pul" -- "tcp://*:5556"
  
  main :: IO ()
  main = withContext 1 $ \ctx -> 
           withPipeline ctx "pipe"
                        (pus, Bind)
                        (pul, Bind) (onErr "Pipeline") $ \_ -> do 
             goPull ctx $ \_ -> do
               _ <- forkIO $ goPush ctx
               forever $ threadDelay 1000000

  goPush :: Context -> IO ()
  goPush ctx = withPipe ctx pul Connect $ \p -> go p 0
    where go :: Pipe -> Int -> IO ()
          go p i = do
            threadDelay 1000000
            push p $ src i
            go p $ i + 1
          src i = C.yield $ B.pack $ show i

  goPull :: Context -> (Controller -> IO ()) -> IO ()
  goPull ctx = withPuller ctx "puller" pus Connect
                          (\_ -> return ()) (onErr "Puller") snk 
    where snk = do mbX <- C.await
                   case mbX of
                     Nothing -> return ()
                     Just x  -> liftIO $ putStrLn $ "Processing job# " 
                                                    ++ show x

  onErr :: String -> Criticality -> SomeException -> String -> IO ()
  onErr a c e m = putStrLn $ show c ++ " in " ++ a ++ ": " ++
                             show e ++ " - " ++ m
