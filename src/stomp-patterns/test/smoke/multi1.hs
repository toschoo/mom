module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Client.Conduit

  import qualified Data.Conduit as C
  import Data.Conduit (($=), ($$))
  import Data.Conduit.List (sourceList)
 
  import System.Environment
  import Network.Socket (withSocketsDo)
  import Control.Monad.Trans (liftIO)
  import qualified Data.ByteString.UTF8  as U
  import Codec.MIME.Type (nullType)
 
  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q] -> withSocketsDo $ handshake q
      _   -> putStrLn "I need a queue name!"
  
  data Msg = Msg String 

  instance Show Msg where
    show (Msg s) = s
  
  str2Msg :: C.MonadResource m => C.Conduit String m Msg
  str2Msg = C.awaitForever $ \x -> C.yield $ Msg x

  outC :: (Show a, C.MonadResource m) => C.Conduit (Message a) m a
  outC = C.awaitForever $ \i -> let o = msgContent i
                                 in liftIO (print o) >> C.yield o

  dropC :: C.MonadResource m => C.Consumer i m ()
  dropC = C.awaitForever return
 
  handshake :: String -> IO ()
  handshake qn = do 
    withConnection "localhost" 61613 [] [] $ \c -> do
      let iconv _ _ _ = return . Msg . U.toString
      let oconv = return . U.fromString . show
      inQ  <- newReader c "Q-Ping" qn [] [] iconv
      outQ <- newWriter c "Q-Pong" qn [] [] oconv
      C.runResourceT $ sourceList req $=
                       str2Msg        $$
                       qMultiSink outQ nullType [] 
      C.runResourceT $ qMultiSource inQ (-1) $=
                       outC $$ dropC
    where req = ["whose woods these are I think I know",
                 "  his house is in the village though",
                 "    he cannot see me stopping here",
                 "      to watch his woods fill up with snow"]
 
