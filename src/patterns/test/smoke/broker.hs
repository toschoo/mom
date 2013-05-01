module Main 
where

  import           Control.Monad.Trans
  import           Control.Monad (forever)
  import           Control.Concurrent
  import           Control.Applicative ((<$>))
  import           Prelude hiding (catch)
  import           Control.Exception   (catch, SomeException)
  import           Data.Conduit (($$), ($=), (=$=))
  import qualified Data.Conduit          as C
  import qualified Data.ByteString.Char8 as B
  
  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
  import           Network.Mom.Patterns.Broker.Broker
  import           Network.Mom.Patterns.Broker.Client
  import           Network.Mom.Patterns.Broker.Server
  import           System.Exit

  cl, srv :: String
  cl  = "inproc://__clients" -- "tcp://*:5556"
  srv = "inproc://__servers" -- "tcp://*:5555"
  
  main :: IO ()
  main = withContext 1 $ \ctx -> catch
           (withBroker ctx "test" cl srv onErr $ \c -> do
              startServer ctx srv
              threadDelay 10000
              startClient ctx cl
              forever (threadDelay 1000000))
           (\e -> putStrLn $ "Broker: " ++ show (e::SomeException))

  startClient :: Context -> String -> IO ()
  startClient ctx a = forkIO go >>= \_ -> return ()
    where go = catch 
            (withClient ctx "olleh" a Connect $ \c -> forever $ do
               mbR <- request c (-1) (C.yield $ B.pack "hello world")
                                     (C.await >>= \mbX -> do
                                        case mbX of
                                          Nothing -> return Nothing
                                          Just x  -> return $ Just $ B.unpack x)
               case mbR of
                 Nothing -> putStrLn "Nothing received..."
                 Just r  -> putStrLn $ "Result: " ++ r
               threadDelay 1000000)
            (\e -> putStrLn $ "Client: " ++ show (e::SomeException))

  startServer :: Context -> String -> IO ()
  startServer ctx a = forkIO go >>= \_ -> return ()
    where go = catch
            (withServer ctx "olleh" a Connect onTmo onErr
                 (\_ -> C.awaitForever (C.yield . B.reverse)) $ \c ->
                   forever (threadDelay 1000000))
            (\e -> putStrLn $ "Server: " ++ show (e::SomeException))

  onErr :: OnError_
  onErr c e m = do
   putStrLn $ show c ++ " in Broker: " ++ 
              show (e::SomeException)  ++ " - " ++ m

  onTmo :: StreamAction
  onTmo _ = return ()
