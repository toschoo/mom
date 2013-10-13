module Main
where

  import Network.Mom.Stompl.Frame (Header)
  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Client.Exception
  import Network.Mom.Stompl.Client.Streams
 
  import System.Environment
  import Network.Socket (withSocketsDo)
  import Control.Monad.Trans (liftIO)
  import Control.Concurrent (forkIO, threadDelay)
  import Control.Exception  (throwIO)
  import qualified Data.ByteString.UTF8  as U
  import Data.Char (toUpper)
  import Data.Conduit (($=), (=$), (=$=), ($$)) 
  import qualified Data.Conduit as C
  import Codec.MIME.Type (Type, nullType)

  q :: String
  q = "/q/test"
 
  main :: IO ()
  main = withSocketsDo $ 
           withConnection_ "localhost" 61613 [] $ \c -> do
             _ <- forkIO (consume c q)
             produce c q
          
  produce :: Con -> String -> IO ()
  produce c qn = do 
      let oconv = return . U.fromString . show
      outQ <- newWriter c "Q-Pong" qn [] [] oconv
      C.runResourceT (streamList (nullCtrl ()) 
                                 ([1..]::[Int]) $$ 
                      controlTx c [] commitN    =$= 
                      raiseOn 100 =$ sinkQ outQ)

  consume :: Con -> String -> IO ()
  consume c qn = do
    let iconv _ _ _ = return . read . U.toString
    inQ  <- newReader c "Q-Ping" qn [] [] iconv
    C.runResourceT (sourceQ inQ (nullCtrl ()) $$ outSink)

  raiseOn :: Int -> Conduit () Int Int
  raiseOn n = C.awaitForever $ \(c,i) -> 
                if n == i
                  then liftIO (throwIO $ AppException "raiseOn")
                  else C.yield (c,i)

  outSink :: Consumer () (Message Int) ()
  outSink = C.awaitForever $ \(_,m) -> liftIO (print $ msgContent m)

  commitN :: [i] -> IO Bool
  commitN ps = if length ps >= 5 then threadDelay 1000000 >>
                                      return True
                                 else return False

