module Main
where

  import Network.Mom.Stompl.Frame (Header)
  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Client.Streams
 
  import System.Environment
  import Network.Socket (withSocketsDo)
  import Control.Monad.Trans (liftIO)
  import Control.Concurrent (threadDelay)
  import qualified Data.ByteString.UTF8  as U
  import Data.Char (toUpper)
  import Data.Conduit (($=), ($$)) 
  import qualified Data.Conduit as C
  import Codec.MIME.Type (Type, nullType)

  qn :: String
  qn = "/q/test" 
 
  main :: IO ()
  main = withSocketsDo $ ping qn
  
  data Ping = Ping | Pong
    deriving (Show)
 
  strToPing :: String -> IO Ping
  strToPing s = case map toUpper s of
                  "PING" -> return Ping
                  "PONG" -> return Pong
                  _      -> convertError $ "Not a Ping: '" ++ s ++ "'"
 
  ping :: String -> IO ()
  ping qn = do 
    withConnection_ "localhost" 61613 [] $ \c -> do
      let iconv _ _ _ = strToPing . U.toString
      let oconv = return . U.fromString . show
      inQ  <- newReader c "Q-Ping" qn [] [] iconv
      outQ <- newWriter c "Q-Pong" qn [] [] oconv
      writeQ outQ nullType [] Ping
      C.runResourceT (sourceQ inQ (nullCtrl ()) $= 
                      process p2p               $$ sinkQ outQ)

  p2p :: Control a -> Message Ping -> IO (Control a, Ping)
  p2p c m = let i = msgContent m
                o = case i of
                      Ping -> Pong
                      Pong -> Ping
             in do threadDelay 500000
                   print  i
                   return (c, o)
    
