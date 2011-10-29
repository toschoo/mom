module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Client.Exception
 
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
      _   -> return () 
        -- error handling...
  
  data Ping = Ping | Pong
    deriving (Show)
 
  strToPing :: String -> IO Ping
  strToPing s = case map toUpper s of
                  "PING" -> return Ping
                  "PONG" -> return Pong
                  _      -> convertError $ "Not a Ping: '" ++ s ++ "'"
 
  ping :: String -> IO ()
  ping qn = do 
    withConnection_ "127.0.0.1" 61613 1024 "guest" "guest" (0,0) $ \c -> do
      let iconv = InBound  (\_ _ _ -> strToPing . U.toString)
      let oconv = OutBound (return . U.fromString . show)
      inQ  <- newQueue c "Q-Ping" qn [OReceive] [] iconv
      outQ <- newQueue c "Q-Pong" qn [OSend]    [] oconv
      writeQ outQ nullType [] Pong
      listen inQ outQ
 
  listen  :: Queue Ping -> Queue Ping -> IO ()
  listen iQ oQ = forever $ do
    eiM <- try $ readQ iQ 
    case eiM of
      Left  e -> do
        putStrLn $ "Error: " ++ (show (e::StomplException))
      Right m -> do
        let p = case msgContent m of
                  Ping -> Pong
                  Pong -> Ping
        putStrLn $ show p
        writeQ oQ nullType [] p
        threadDelay 100000

