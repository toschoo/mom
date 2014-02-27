--------------------------------------------------------------------------
-- with broker listening to 61613
--------------------------------------------------------------------------
module Main
where

  import Network.Mom.Stompl.Client.Queue

  import System.Exit
  import System.Environment

  import Network.Socket (withSocketsDo)
  import Control.Monad (forever)
  import Control.Exception (finally)

  import qualified Data.ByteString.UTF8  as U

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q] -> withSocketsDo $ conAndListen q
      _   -> do
        putStrLn "I need a queue name (and only a queue name)."
        exitFailure

  conAndListen :: String -> IO ()
  conAndListen qn = withSocketsDo $ do -- connectAndGo
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
      let conv _ _ _ = return . U.toString
      q <- newReader c "Q-Hof" qn [] [] conv
      listen2 q `finally` destroyReader q

  listen2 :: Reader String -> IO ()
  listen2 q = forever $ do
    eiM <- try $ readQ q 
    case eiM of
      Left  e -> do
        putStrLn $ "Error: " ++ (show (e::StomplException))
      Right m -> do
        putStrLn (msgContent m)

