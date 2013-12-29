module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)

  main :: IO ()
  main = withSocketsDo tstSub

  tstSub :: IO ()
  tstSub = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withSub c "Test" "Pub1" "/q/target/pub1" 500000 
              ("/q/sub1", [], [], iconv) $ \s -> forever $ do
        m <- checkIssue s
        print $ msgContent m
    where iconv :: InBound Int
          iconv _ _ _ = return . read . B.unpack 

