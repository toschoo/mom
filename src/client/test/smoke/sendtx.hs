--------------------------------------------------------------------------
-- with broker listening to 61613
--------------------------------------------------------------------------
module Main
where

  import Network.Mom.Stompl.Client.Queue

  import System.Exit
  import System.Environment

  import Network.Socket (withSocketsDo)

  import qualified Data.ByteString.Char8 as B

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q] -> withSocketsDo $ makeTransaction q
      _   -> do
        putStrLn "I need a queue name (and only a queue name)."
        exitFailure

  makeTransaction :: String -> IO ()
  makeTransaction qn = withSocketsDo $ do -- connectAndGo
    withConnection_ "127.0.0.1" 61613 1024 "guest" "guest" (0,0) $ \c -> do
      let conv = OutBound (return . B.pack)
      q <- newQueue c "Q-Hof" qn [OSend] [] conv
      withTransaction_ c [] $ \_ -> do
        writeQ q "text/plain" [] "Tx Message 1" 
        writeQ q "text/plain" [] "Tx Message 2"
        writeQ q "text/plain" [] "Tx Message 3"
        putStrLn "Hit key to end transaction!"
        _ <- getChar
        return ()

