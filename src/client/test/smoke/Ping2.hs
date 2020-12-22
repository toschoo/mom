module Ping2 (withPingPong, ping)
where

  import Network.Mom.Stompl.Client.Queue
 
  import Control.Monad (when)
  import Control.Concurrent (threadDelay)
  import qualified Data.ByteString.UTF8  as U
  import Data.Char (toUpper)
  import Codec.MIME.Type (nullType)

    
  data Ping = Ping | Pong
    deriving (Show)


  strToPing :: String -> IO Ping
  strToPing s = case map toUpper s of
                  "PING" -> return Ping
                  "PONG" -> return Pong
                  _      -> convertError $ "Not a Ping: '" ++ s ++ "'"

  withPingPong :: Int -> (Con -> IO ()) -> IO ()
  withPingPong p = withConnection "localhost" p [] [] 

  ping :: String -> Con -> IO ()
  ping qn c = do 
    let iconv _ _ _ = strToPing . U.toString
    let oconv = return . U.fromString . show
    withReader   c "Q-IN"  qn [] [] iconv $ \inQ  ->
      withWriter c "Q-OUT" qn [] [] oconv $ \outQ -> do
        writeQ outQ nullType [] Pong
        listen inQ outQ 100
        threadDelay 1000000

  listen  :: Reader Ping -> Writer Ping -> Int -> IO ()
  listen _  _  0 = return () 
  listen iQ oQ n = do -- forever $ do
    eiM <- try $ readQ iQ 
    case eiM of
      Left  e -> do
        putStrLn $ "Error: " ++ show e 
      Right m -> do
        let p = case msgContent m of
                  Ping -> Pong
                  Pong -> Ping
        putStrLn $ show p
        when (n > 1) $ do
          writeQ oQ nullType [] p
          threadDelay 100000
          listen iQ oQ (n-1)


 
