module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import System.Environment
  import System.Exit
  import Network.Socket
  import Codec.MIME.Type (nullType)

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q,m] -> withSocketsDo $ tstRequest q m
      _     -> do
        putStrLn "I need a queue and a message and nothing else."
        exitFailure

  tstRequest :: QName -> String -> IO ()
  tstRequest q m = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withClient c "Test" "olleh"
                 ("/q/mychannel", [], [], iconv)
                 (q,              [], [], oconv) $ \cl -> do
        mbX <- request cl (-1) nullType [] m
        case mbX of
          Nothing -> putStrLn "Nothing received"
          Just x  -> putStrLn $ msgContent x
    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack

