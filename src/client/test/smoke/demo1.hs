module Main
where

import           Network.Mom.Stompl.Client.Queue
import           Codec.MIME.Type (nullType)
import qualified Data.ByteString.Char8 as B
import           Control.Concurrent (threadDelay, forkIO)
import           Control.Monad (forever)

delay :: Int
delay = 100

main :: IO ()
main = withConnection "localhost" 61613 [] [] $ \c -> do
         iq <- newReader c "demoI" "/q/demo" [] [] 
                         (\_ _ _ -> return . B.unpack)
         oQ <- newWriter c "demoO" "/q/demo" [] []
                         (return . B.pack)
         _ <- forkIO $ consume iq
         publish oQ
         
consume :: Reader String -> IO ()
consume r = forever $ do
  m <- readQ r
  putStrLn $ "Message: " ++ msgContent m

publish :: Writer String -> IO ()
publish w = go 1
  where go 0 = do writeQ     w    nullType [] msg -- this one is     for us
                  threadDelay delay >> go 1
        go 1 = do writeAdHoc w q2 nullType [] msg -- this one is not for us
                  threadDelay delay >> go 0
        msg = "hello world"
        q2  = "/q/otherQ"

