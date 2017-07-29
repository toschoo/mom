module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)
  import Codec.MIME.Type (nullType)

  main :: IO ()
  main = withSocketsDo tstReply 

  tstReply :: IO ()
  tstReply = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withServer c "Test" ("/q/request", [], [], iconv)
                          ("unknown",    [], [], oconv) $ \s -> forever $ 
        reply s (-1) nullType [] createReply
    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack
          createReply = return . reverse . msgContent

