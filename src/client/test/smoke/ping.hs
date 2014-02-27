module Main
where

  import Network.Mom.Stompl.Client.Queue
 
  import System.Environment
  import Network.Socket (withSocketsDo)
  import Control.Monad (forever)
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
 
  ping :: String -> IO ()
  ping qn = do 
    withConnection "localhost" 61613 [] [] $ \c -> do
      let iconv _ _ _ = strToPing . U.toString
      let oconv = return . U.fromString . show
      withReader  c "Q-IN"  qn [] [] iconv $ \inQ ->
       withWriter c "Q-OUT" qn [] [] oconv $ \outQ -> do
         writeQ outQ nullType [] Pong
         listen inQ outQ
 
  listen  :: Reader Ping -> Writer Ping -> IO ()
  listen iQ oQ = forever $ do
    eiM <- try $ readQ iQ 
    case eiM of
      Left  e -> do
        putStrLn $ "Error: " ++ show e 
      Right m -> do
        let p = case msgContent m of
                  Ping -> Pong
                  Pong -> Ping
        putStrLn $ show p
        writeQ oQ nullType [] p
        threadDelay 100000

