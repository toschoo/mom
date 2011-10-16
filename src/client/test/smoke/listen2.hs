--------------------------------------------------------------------------
-- with broker listening to 61613
--------------------------------------------------------------------------
module Main
where

  import Protocol -- Message interface!
  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Exception

  import System.Exit
  import System.Environment

  import Network.Socket (withSocketsDo)
  import Control.Monad (forever)
  import Control.Exception (try)

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
    withConnection_ "127.0.0.1" 61613 1024 "guest" "guest" (0,0) $ \c -> do
      let conv = InBound (return . U.toString)
      q <- newQueue c "Q-Hof" qn [OReceive] [] conv
      listen2 q

  listen2 :: Queue String -> IO ()
  listen2 q = forever $ do
    eiM <- try $ readQ q 
    case eiM of
      Left  e -> do
        putStrLn $ "Error: " ++ (show (e::StomplException))
      Right m -> do
        putStrLn $ "Message: " ++ (show $ msgRaw m)

