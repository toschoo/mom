module Main 
where

  import           Control.Monad.Trans
  import           Control.Monad (forever)
  import           Control.Concurrent
  import           Control.Applicative ((<$>))
  import           Prelude hiding (catch)
  import           Control.Exception   (catch, SomeException)
  import           Data.Conduit (($$), ($=), (=$=))
  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
  
  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams
  import           Network.Mom.Patterns.Basic.Publisher
  import           Network.Mom.Patterns.Basic.Subscriber

  pub :: String
  pub = "inproc://__pub" -- "tcp://*:5556"
  
  main :: IO ()
  main = withContext 1 $ \ctx -> do
             _ <- forkIO $ goPub ctx
             m <- newMVar [] -- (0::Int)
             -- withSubVarR ctx pub Connect ["even"] m snk (
             withSubVar_ ctx pub Connect ["even"] m snk (
               forever $ do threadDelay 1000000
                            withMVar m (print))
    where snk :: MVar [Int] -> SinkR (Maybe ())
          snk m = do mbX <- C.await
                     case mbX of
                       Nothing -> return Nothing
                       Just x  -> let i = read $ B.unpack x
                                   in do liftIO (modifyMVar_ m $ \is -> 
                                                   return (i:is)) -- return $ Just (read $ B.unpack x)
                                         snk m

  goPub :: Context -> IO ()
  goPub ctx = withPub ctx pub Bind $ \p -> go p 0
    where go :: Pub -> Int -> IO ()
          go p i = do
            threadDelay 1000000
            if i `mod` 2 == 0 
              then issue p ["even"] $ src i
              else issue p ["odd"]  $ src i
            go p $ i + 1
          src i = C.yield $ B.pack $ show i


  onErr :: Criticality -> SomeException -> String -> IO ()
  onErr c e m = putStrLn $ show c ++ " in Forwarder: " ++
                           show e ++ " - " ++ m
