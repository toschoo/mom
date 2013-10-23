module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Client.Client
  import qualified Data.ByteString.Char8 as B
  import System.Environment
  import System.Exit
  import Network.Socket
  import Codec.MIME.Type (nullType)

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
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withClient c "Test" 
                 ("/q/mychannel", [], [], iconv)
                 ("/q/request",   [], [], oconv) $ \cl -> do
        mbX <- request cl (-1) nullType [] m
        case mbX of
          Nothing -> putStrLn "Nothing received"
          Just x  -> putStrLn $ msgContent x
    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack

