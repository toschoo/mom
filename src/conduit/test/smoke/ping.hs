module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Client.Conduit

  import qualified Data.Conduit as C
  import Data.Conduit ((.|))
 
  import System.Environment
  import Network.Socket (withSocketsDo)
  import Control.Monad.Trans (liftIO)
  import Control.Monad.IO.Class (MonadIO)
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

  pingPongC :: MonadIO m => C.ConduitT (Message Ping) Ping m ()
  pingPongC = C.awaitForever $ \x -> C.yield (pingPong $ msgContent x) 

  waitC :: MonadIO m => C.ConduitT a a m ()
  waitC = C.awaitForever $ \x -> liftIO (threadDelay 500000) >> C.yield x

  outC :: (Show a, MonadIO m) => C.ConduitT a a m ()
  outC = C.awaitForever $ \x -> liftIO (print x) >> C.yield x
 
  ping :: String -> IO ()
  ping qn = do 
    withConnection "localhost" 61613 [] [] $ \c -> do
      let iconv _ _ _ = strToPing . U.toString
      let oconv = return . U.fromString . show
      inQ  <- newReader c "Q-Ping" qn [] [] iconv
      outQ <- newWriter c "Q-Pong" qn [] [] oconv
      writeQ outQ nullType [] Pong
      C.runConduitRes (qSource inQ (-1) .| 
                       pingPongC        .| 
                       outC             .|
                       waitC            .| qSink outQ nullType [])
 
