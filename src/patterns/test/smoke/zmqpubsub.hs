module Main
where

  import qualified System.ZMQ as Z
  import           Control.Concurrent
  import           Control.Monad (when, forever)
  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = Z.withContext 1 $ \ctx -> 
           Z.withSocket ctx Z.Pub $ \p -> do
             Z.bind p "inproc://_pub" 
             _ <- forkIO $ sub ctx
             go p 0
    where go p i = do
            threadDelay 1000000
            if i `mod` 2 == 0
              then do
                Z.send p (B.pack "number, even") [Z.SndMore]
                Z.send p (B.pack $ show i) []
              else do
                Z.send p (B.pack "number, odd") [Z.SndMore]
                Z.send p (B.pack $ show i) []
            go p (i+1)

  sub :: Z.Context -> IO ()
  sub ctx = Z.withSocket ctx Z.Sub $ \s -> do
              Z.connect s "inproc://_pub"
              Z.subscribe s "even" 
              Z.subscribe s "number" 
              go s
    where go s = forever $ do
                   _ <- Z.receive s []
                   m <- Z.moreToReceive s
                   when m $ do
                     x <- Z.receive s []  
                     print x
          
             
