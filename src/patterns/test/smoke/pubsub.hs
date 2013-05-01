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
  
  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
  import           Network.Mom.Patterns.Basic.Publisher
  import           Network.Mom.Patterns.Basic.Subscriber

  pub, sub :: String
  pub = "inproc://__pub" -- "tcp://*:5556"
  sub = "inproc://__sub" -- "tcp://*:5556"
  
  main :: IO ()
  main = withContext 1 $ \ctx ->
           withForwarder ctx "forward" [""] -- ["odd", "even"]
                         (pub, Bind)
                         (sub, Bind) onErr $ \_ -> do
             _ <- forkIO $ goPub ctx
             _ <- forkIO $ goSub ctx
             forever $ threadDelay 1000000

  goPub :: Context -> IO ()
  goPub ctx = withPub ctx sub Connect $ \p -> go p 0
    where go :: Pub -> Int -> IO ()
          go p i = do
            threadDelay 1000000
            if i `mod` 2 == 0 
              then issue p ["even"] $ src i
              else issue p ["odd"]  $ src i
            go p $ i + 1
          src i = C.yield $ B.pack $ show i

  goSub :: Context -> IO ()
  goSub ctx = withSub ctx pub Connect $ \s -> do
                subscribe s ["odd", "even"]
                forever $ do
                  mbX <- checkSub s (-1) snk
                  case mbX of
                    Nothing -> return ()
                    Just x  -> print x
    where snk = do mbX <- C.await
                   case mbX of
                     Nothing -> return Nothing
                     Just x  -> return $ Just x

  onErr :: Criticality -> SomeException -> String -> IO ()
  onErr c e m = putStrLn $ show c ++ " in Forwarder: " ++
                           show e ++ " - " ++ m
