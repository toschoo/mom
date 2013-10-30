module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Client.Conduit

  import qualified Data.Conduit as C
  import Data.Conduit (($=), ($$))
 
  import System.Environment
  import Network.Socket (withSocketsDo)
  import Control.Monad.Trans (liftIO)
  import Control.Concurrent (threadDelay)
  import qualified Data.ByteString.UTF8  as U
  import Data.Char (toUpper)
  import Codec.MIME.Type (nullType)
 
  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q] -> withSocketsDo $ ping q
      _   -> putStrLn "I need a queue name!"
  
  data Ping = Ping | Pong
    deriving (Show)
 
  strToPing :: String -> IO Ping
  strToPing s = case map toUpper s of
                  "PING" -> return Ping
                  "PONG" -> return Pong
                  _      -> convertError $ "Not a Ping: '" ++ s ++ "'"

  pingPong :: Ping -> Ping
  pingPong p = case p of
                 Ping -> Pong
                 _    -> Ping

  pingPongC :: C.MonadResource m => C.Conduit (Message Ping) m Ping 
  pingPongC = C.awaitForever $ \x -> C.yield (pingPong $ msgContent x) 

  waitC :: C.MonadResource m => C.Conduit a m a
  waitC = C.awaitForever $ \x -> liftIO (threadDelay 500000) >> C.yield x

  outC :: (Show a, C.MonadResource m) => C.Conduit a m a
  outC = C.awaitForever $ \x -> liftIO (print x) >> C.yield x
 
  ping :: String -> IO ()
  ping qn = do 
    withConnection "localhost" 61613 [] [] $ \c -> do
      let iconv _ _ _ = strToPing . U.toString
      let oconv = return . U.fromString . show
      inQ  <- newReader c "Q-Ping" qn [] [] iconv
      outQ <- newWriter c "Q-Pong" qn [] [] oconv
      writeQ outQ nullType [] Pong
      C.runResourceT $ qSource inQ (-1) $= 
                       pingPongC        $= 
                       outC             $=
                       waitC            $$ qSink outQ nullType []
 
