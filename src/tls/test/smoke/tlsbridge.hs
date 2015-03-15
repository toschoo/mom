{-# Language BangPatterns #-}
module Main
where

  import Network.TLS.Bridge.Bridge

  import qualified Data.ByteString.Char8 as B
  import           Data.Char (isDigit)
  import           Data.String (fromString)
  import           Data.Conduit.Network
  import           Data.Conduit.Network.TLS
  import           Network.Socket
  import           Network.Connection
  import           Control.Concurrent (threadDelay)
  import           System.Environment
  import           System.IO (hFlush, stdout)

  hp :: HostPreference
  hp = fromString "*"

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
  bridge c s = client2TLS out (serverSettings c hp) cfg $ meep False
                 
    where tlss = TLSSettingsSimple {
                    settingDisableCertificateValidation = True,
                    settingDisableSession               = False,
                    settingUseServerName                = False}
          cfg  = (tlsClientConfig s $ B.pack "localhost"){ 
                  tlsClientTLSSettings=tlss}
          meep t = do threadDelay 100000 
                      let o = if t then "\bo" else "\bO"
                      putStr o >> hFlush stdout
                      meep (not t)

