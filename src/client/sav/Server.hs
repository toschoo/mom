module Network.Mom.Stompl.Client.Server
where

  import           Network.Mom.Stompl.Client.Queue 
  import qualified Network.Mom.Stompl.Frame as F
  import           System.Timeout
  import qualified Codec.MIME.Type as M
  import           Control.Exception (throwIO)

  data Server i o = Server {
                      srvIn  :: Reader i,
                      srvOut :: Writer o}
  
  withServer :: Con -> String ->
                       ReaderDesc i ->
                       WriterDesc o ->
                       (Server i o  -> IO r) -> IO r
  withServer c n rd wd act =
    withPair c n rd wd $ \(r,w) -> act $ Server r w

  reply :: Server i o -> Int -> M.Type -> [F.Header] -> 
           (Message i -> IO o) -> IO ()
  reply s tmo t hs transform = do
    mbM <- timeout tmo $ readQ (srvIn s)
    case mbM of
      Nothing -> return ()
      Just m  -> do
        x <- transform m
        case lookup "client" $ msgHdrs m of
          Nothing -> throwIO $ ProtocolException "No response channel defined!"
          Just c  -> writeAdHoc (srvOut s) c t hs x
