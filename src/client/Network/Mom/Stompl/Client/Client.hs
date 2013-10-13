module Network.Mom.Stompl.Client.Client
where

  import qualified Network.Mom.Stompl.Client.Queue as Q
  import qualified Network.Mom.Stompl.Frame as F
  import qualified Data.ByteString.Char8 as B
  import           System.Timeout
  import           Network.Socket
  import qualified Codec.MIME.Type as M

  data Client i o = Client {
                      clChn :: String,
                      clIn  :: Q.Reader i,
                      clOut :: Q.Writer o}
  
  withClient :: Q.Con -> String ->
                       String -> String ->
                       [Q.Qopt] -> [Q.Qopt] ->
                       [F.Header]       -> 
                       Q.InBound  i       ->
                       Q.OutBound o       ->
                       (Client i o -> IO r) -> IO r
  withClient c n rn wn rqs wqs hs iconv oconv act =
    Q.withPair c n rn wn rqs wqs hs [] iconv oconv $ \(r,w) -> 
      act $ Client rn r w

  request :: Client i o -> 
             Int -> M.Type -> [F.Header] -> o -> IO (Maybe (Q.Message i))
  request c tmo t hs r = 
    Q.writeQ (clOut c) t (("client", clChn c) : hs) r >> 
      timeout tmo (Q.readQ (clIn c))

  checkRequest :: Client i o -> Int -> IO (Maybe (Q.Message i))
  checkRequest c tmo = timeout tmo $ Q.readQ (clIn c)
