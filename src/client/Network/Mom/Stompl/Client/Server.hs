module Network.Mom.Stompl.Client.Server
where

  import           Network.Mom.Stompl.Client.Queue 
  import qualified Network.Mom.Stompl.Frame as F
  import qualified Data.ByteString.Char8 as B
  import           System.Timeout
  import           Network.Socket
  import qualified Codec.MIME.Type as M

  data Server i o = Server {
                      srvChn :: String,
                      srvIn  :: Reader i,
                      srvOut :: Writer o}
  
  withServer :: Con -> String ->
                         String   -> 
                         [Qopt] -> [Qopt] ->
                         [F.Header]       -> 
                         InBound  i       ->
                         OutBound o       ->
                         (Server i o -> IO r) -> IO r
  withServer c n rn rqs wqs hs iconv oconv act =
    withPair c n rn "unknown" rqs wqs hs [] iconv oconv $ \(r,w) -> 
      act $ Server rn r w

  reply :: Server i o -> Int -> M.Type -> [F.Header] -> (Message i -> IO o) -> IO ()
  reply s tmo t hs transform = do
    mbM <- timeout tmo $ readQ (srvIn s)
    case mbM of
      Nothing -> return ()
      Just m  -> do
        x <- transform m
        case lookup "channel" $ msgHdrs m of
          Nothing -> putStrLn "no channel" -- error handling
          Just c  -> writeAdHoc (srvOut s) c t hs x
