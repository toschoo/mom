module Main
where

  import qualified System.ZMQ as Z
  import           Control.Concurrent
  import           Control.Monad (when, forever)
  import qualified Data.ByteString.Char8 as B

  ac :: String
  ac = "inproc://_ac"

  main :: IO ()
  main = Z.withContext 1 $ \ctx -> 
           Z.withSocket ctx Z.XRep $ \s -> do
             Z.bind s ac
             _ <- forkIO $ client ctx
             go s 
    where go s = do
            i  <- Z.receive s []
            rs <- recvAll s []
            print rs
            putStrLn "EOT"
            -- sendAll s [i, B.pack "ok"]
            go s

  client :: Z.Context -> IO ()
  client ctx = Z.withSocket ctx Z.XReq $ \c -> 
               Z.connect c ac >> go c
    where go c = forever $ do
                   sendAll c $ map (B.pack . show) [1..5]
                   -- _ <- recvAll c []
                   -- return ()

  sendAll :: Z.Socket a -> [B.ByteString] -> IO ()
  sendAll _ [] = return ()
  sendAll c (x:xs) | null xs   = Z.send c x []
                   | otherwise = Z.send c x [Z.SndMore] >> sendAll c xs

  recvAll :: Z.Socket a -> [B.ByteString] -> IO [B.ByteString]
  recvAll c rs = do
    r <- Z.receive c []
    m <- Z.moreToReceive c
    if m then recvAll c (r:rs)
         else return $ reverse (r:rs)
             
