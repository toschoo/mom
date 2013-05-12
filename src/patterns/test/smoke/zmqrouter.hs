module Main
where

  import qualified System.ZMQ as Z
  import           Control.Concurrent
  import           Control.Monad (when, forever)
  import qualified Data.ByteString.Char8 as B

  ap1, ap2 :: String
  ap1 = "inproc://accesspoint1"
  ap2 = "inproc://accesspoint2"

  main :: IO ()
  main = Z.withContext 1 $ \ctx -> 
           Z.withSocket ctx Z.XReq $ \p -> do
             Z.bind p ap1
             _ <- forkIO $ route2 ctx
             forever $ go p 
    where go p = do
            threadDelay 1000000
            Z.send p B.empty [Z.SndMore]
            Z.send p (B.pack $ "hello world") []

  route2 :: Z.Context -> IO ()
  route2 ctx = Z.withSocket ctx Z.XRep $ \r -> do
                 Z.connect r ap1
                 Z.withSocket ctx Z.XReq $ \s -> do
                   Z.bind s ap2
                   _ <- forkIO $ route1 ctx
                   -- i <- Z.receive s []
                   -- _ <- Z.receive s []
                   let i = B.empty
                   forever $ relay i r s

  relay :: B.ByteString -> Z.Socket a -> Z.Socket b -> IO ()
  relay i r s = do
    x <- Z.receive r []
    -- Z.send s i [Z.SndMore]
    Z.send s B.empty [Z.SndMore]
    go x
    where go x = do m <- Z.moreToReceive r
                    if m then do Z.send s x [Z.SndMore] 
                                 Z.receive r [] >>= go
                         else Z.send s x []

  handshake :: Z.Socket a -> IO ()
  handshake s = Z.send s B.empty []

  route1 :: Z.Context -> IO ()
  route1 ctx = Z.withSocket ctx Z.XRep $ \s -> do
               Z.connect s ap2
               -- handshake s
               forever $ printFrames s

  printFrames :: Z.Socket a -> IO ()
  printFrames s = do
    x <- Z.receive s []
    print x
    m <- Z.moreToReceive s
    if m then printFrames s
         else putStrLn "===================================="
             
