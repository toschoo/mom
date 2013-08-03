module Main 
where

  import           Control.Monad.Trans
  import           Control.Monad (forever)
  import           Control.Concurrent
  import           Control.Applicative ((<$>))
  import           Prelude hiding (catch)
  import           Control.Exception   (catch, SomeException, throwIO)
  import           Data.Conduit (($$), ($=), (=$=))
  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
  
  import           Network.Mom.Patterns.Types
  import           Network.Mom.Patterns.Streams
  import           Network.Mom.Patterns.Broker.Broker
  import           Network.Mom.Patterns.Broker.Client
  import           Network.Mom.Patterns.Broker.Server

  cl, srv :: String
  cl  = "inproc://__clients" -- "tcp://*:5556"
  srv = "inproc://__servers" -- "tcp://*:5555"
  
  main :: IO ()
  main = withContext 1 $ \ctx -> catch
           (withBroker ctx "test" 1000 cl srv onErr $ \c -> do
              startServer ctx srv
              threadDelay 10000
              startClient ctx cl
              forever (threadDelay 1000000))
           (\e -> putStrLn $ "Broker: " ++ show (e::SomeException))

  startClient :: Context -> String -> IO ()
  startClient ctx a = forkIO go >>= \_ -> return ()
    where go = catch 
            (withClient ctx "olleh" a Connect $ \c -> forever $ do
               mbOK <- checkService c (1000)
               case mbOK of
                 Nothing -> throwIO $ AppExc $ 
                               "Nothing received from mmi.service"
                 Just x | not x     -> throwIO $ AppExc "olleh not available"
                        | otherwise -> do
                   mbR  <- request c (-1) (C.yield $ B.pack "hello world")
                                          (C.await >>= \mbX -> do
                                             case mbX of
                                               Nothing -> return Nothing
                                               Just m  -> return $ 
                                                   Just $ B.unpack m)
                   case mbR of
                     Nothing -> putStrLn "Nothing received..."
                     Just r  -> putStrLn $ "Result: " ++ r
                   threadDelay 1000000)
            (\e -> putStrLn $ "Client: " ++ show (e::SomeException))

  startServer :: Context -> String -> IO ()
  startServer ctx a = forkIO go >>= \_ -> return ()
    where go = catch
            (withServer ctx "olleh" 1000 a onErr
                 (C.awaitForever (C.yield . B.reverse)) $ \_ ->
                   forever (threadDelay 1000000))
            (\e -> putStrLn $ "Server: " ++ show (e::SomeException))

  onErr :: OnError_
  onErr c e m = do
   putStrLn $ show c ++ " in Broker: " ++ 
              show (e::SomeException)  ++ " - " ++ m

  onTmo :: StreamAction
  onTmo _ = return ()
