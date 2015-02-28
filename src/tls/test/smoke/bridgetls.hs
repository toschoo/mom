module Main
where

  import Network.TLS.Bridge.Bridge

  import           Control.Concurrent (threadDelay)
  import           Control.Monad (forever)
  import           Data.Char (isDigit)
  import           Data.String (fromString)
  import           Data.Conduit.Network
  import           Data.Conduit.Network.TLS
  import           Network.Socket
  import qualified Filesystem.Path.CurrentOS as F
  import           System.Environment
  import           System.IO (hFlush, stdout)

  hp :: HostPreference
  hp = fromString "*"

  keyStore, certs :: F.FilePath
  keyStore = F.decodeString "test/keys/broker.ks"
  certs    = F.decodeString "test/keys/broker.crt"

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [c,s] -> if (all isDigit c) && (all isDigit s)
                 then withSocketsDo $ bridge (read c) (read s)
                 else error ("At least one of the ports is not numeric: " ++ 
                             show c ++ ", " ++ show s)
      _      -> error ("I need a bridge port and a server port " ++
                       "and nothing else.")

  bridge :: Int -> Int -> IO ()
  bridge c s = tls2Server s (tlsConfig hp c certs keyStore) $ 
                 forever $ meep False
    where meep t = do threadDelay 100000
                      let o = if t then "\bo" else "\bO"
                      putStr o >> hFlush stdout
                      meep (not t)

