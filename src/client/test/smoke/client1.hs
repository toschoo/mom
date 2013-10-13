module Main
where

  import Network.Mom.Stompl.Client.Queue
  import qualified Data.ByteString.Char8 as B
  import System.Environment
  import System.Exit
  import System.Timeout
  import Network.Socket
  import Codec.MIME.Type (nullType)

  request :: (Reader i, Writer o) -> Int -> o -> IO (Maybe (Message i))
  request (r,w) tmo m = 
    writeQ w nullType [("channel", "/q/mychannel")] m >> 
      timeout tmo (readQ r)

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [m] -> withSocketsDo $ tstRequest m
      _   -> do
        putStrLn "I need a message and nothing else."
        exitFailure

  tstRequest :: String -> IO ()
  tstRequest m = 
    withConnection_ "127.0.0.1" 61613 [] $ \c -> 
      withPair c "Test" "/q/mychannel" "/q/request" [] [] [] []
                        iconv oconv $ \ch -> do
                          mbX <- request ch (-1) m
                          case mbX of
                            Nothing -> putStrLn "Nothing received"
                            Just x  -> putStrLn $ msgContent x
    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack

