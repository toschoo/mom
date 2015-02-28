{-# Language BangPatterns #-}
module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Frame
  import Network.Mom.Stompl.Parser
  import qualified Data.ByteString.Char8 as B
  import           Data.Char (isDigit)
  import           Data.String (fromString)
  import qualified Data.Conduit as C
  import qualified Data.Conduit.List as CL
  import           Data.Conduit (($$),(=$),(=$=))
  import           Data.Conduit.Network
  import           Data.Conduit.Network.TLS
  import           Control.Monad (forever)
  import           Control.Monad.Trans (liftIO)
  import           Control.Monad.Trans.Resource (ResourceT, runResourceT)
  import           Control.Concurrent
  import           System.Environment
  import           Network.Socket
  import           Network.Connection
  import           Codec.MIME.Type (nullType)
  import           Filesystem.Path.CurrentOS

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [p,q] -> if all isDigit p
                 then withSocketsDo $ conAndListen (read p) q 
                 else error ("Port is not numeric: " ++ p)
      _      -> error ("I need a port and a queue name " ++
                       "and nothing else.")

  type RIO = ResourceT IO

  conAndListen :: Int -> String -> IO ()
  conAndListen p qn = let hp   = fromString "*"
                          cfg  = tlsConfig hp p (decodeString "test/keys/broker.crt") 
                                                (decodeString "test/keys/broker.ks")
                       in runTCPServerTLS cfg session
    where session ad = do
            ch <- newChan
            _  <- forkIO (sender ad ch)
            appSource ad $$ pipeSink ch

  sender :: AppData -> Chan B.ByteString -> IO ()
  sender ad ch = pipeSource ch $$ parse =$= handleFrame ad =$ appSink ad

  check :: C.ConduitM B.ByteString B.ByteString IO ()
  check = C.awaitForever (\i -> if B.null i then liftIO $ putStrLn "NULL!"
                                            else C.yield i)
 
  pipeSink :: Chan B.ByteString -> C.Sink B.ByteString IO ()
  pipeSink ch = C.awaitForever (\i -> liftIO (writeChan ch i))

  pipeSource :: Chan B.ByteString -> C.Source IO B.ByteString 
  pipeSource ch = forever (liftIO (readChan ch) >>= C.yield)

  outSnk :: (Show i) => C.Consumer i IO ()
  outSnk = C.awaitForever (\i -> liftIO (print i))

  parse :: C.ConduitM B.ByteString Frame IO ()
  parse = C.awaitForever (\i -> case stompAtOnce i of
                                  Left  e -> liftIO $ putStrLn ("error " ++ e)
                                  Right f -> C.yield f)

  handleFrame :: AppData -> C.ConduitM Frame B.ByteString IO () 
  handleFrame ad = C.awaitForever $ \f -> 
    case typeOf f of
      Connect{} -> case conToCond "" "ses1" (0,0) [(1,3)] f of
                    Nothing -> liftIO (putStrLn $ "Error: " ++ show f)
                    Just c  -> C.yield (putFrame c)
      _         -> liftIO (putStrLn $ "Not Connect: " ++ show f)

  sendf :: AppData -> Frame -> IO ()
  sendf ad f = C.yield (putFrame f) $$ appSink ad

                          
    {-
    withConnection "127.0.0.1" p [] [] $ \c -> do
      let conv = return 
      q <- newWriter c "Test-Q" qn [] [] conv
      writeQ q nullType [] m
    -}
