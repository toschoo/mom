module Main
where

  import Network.Mom.Stompl.Client.Queue
  import qualified Data.ByteString.Char8 as B
  import System.Timeout
  import Network.Socket
  import Control.Monad (forever)
  import Codec.MIME.Type (nullType)

  reply :: (Reader String, Writer String) -> Int -> IO ()
  reply (r,w) tmo = do
    mbM <- timeout tmo $ readQ r
    case mbM of
      Nothing -> return ()
      Just m  -> do
        let x = reverse $ msgContent m
        case lookup "channel" $ msgHdrs m of
          Nothing -> putStrLn "no channel"
          Just c  ->
            writeAdHoc w c nullType [] x

  main :: IO ()
  main = withSocketsDo tstReply 

  tstReply :: IO ()
  tstReply = 
    withConnection_ "127.0.0.1" 61613 [] $ \c -> 
      withPair c "Test" "/q/request" "unknown" [] [] [] []
                        iconv oconv $ \ch -> forever $ reply ch (-1)
    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack

