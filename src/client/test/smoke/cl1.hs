module Main
where

  import Network.Mom.Stompl.Frame (Header)
  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Client.Streams
  import Network.Mom.Stompl.Client.Basic
 
  import System.Environment
  import Network.Socket (withSocketsDo)
  import Control.Monad.Trans (liftIO)
  import Control.Concurrent (threadDelay)
  import Control.Applicative ((<$>))
  import qualified Data.ByteString.UTF8  as U
  import Data.Char (toUpper)
  import Data.Conduit (($=), ($$)) 
  import qualified Data.Conduit as C
  import Codec.MIME.Type (Type, nullType)

  qn :: String
  qn = "/q/test" 
 
  main :: IO ()
  main = withSocketsDo $ 
           withConnection "localhost" 61613 [] [] $ \cn -> do
             withClient cn "TestClient" "/q/client/test"
                                        "/q/server/test"
                                        (\_ _ _ -> return . U.toString)
                                        (return . U.fromString) $ \c -> do
               let ctrl = nullCtrl ()
               r <- getContent <$> request c (-1) ctrl 
                                     (streamList ctrl ["hello"]) consume
               putStrLn r
    where getContent = concat . map msgContent

  outSink :: Consumer () (Message String) ()
  outSink = C.awaitForever $ \(_,m) -> liftIO (putStrLn $ msgContent m)
