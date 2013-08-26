module Main 
where

  import           Control.Monad (forever)
  import           Control.Concurrent
  import           Prelude hiding (catch)
  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
  
  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Basic.Publisher
  import           Network.Mom.Patterns.Basic.Subscriber

  pub :: String
  pub = "inproc://__pub" 
  
  main :: IO ()
  main = withContext 1 $ \ctx ->
           withPub ctx pub Bind $ \p -> 
             forkIO (goSub ctx) >>= \_ -> go p True
    where go p True  = do issue p ["demoT"]  $ src "hello world"
                          threadDelay 100000 >> go p False
          go p False = do issue p ["otherT"] $ src "hello world"
                          threadDelay 100000 >> go p True
          src i = C.yield $ B.pack i

  goSub :: Context -> IO ()
  goSub ctx = withSub ctx pub Connect $ \s -> do
                subscribe s ["demoT"]
                forever $ do
                  mbX <- checkSub s (-1) snk
                  case mbX of
                    Nothing -> return ()
                    Just x  -> print x
    where snk = do mbX <- C.await
                   case mbX of
                     Nothing -> return Nothing
                     Just x  -> return $ Just x
