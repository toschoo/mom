--------------------------------------------------------------------------
-- with broker listening to 61613
--------------------------------------------------------------------------
module Main
where

  import Network.Mom.Stompl.Client.Queue

  import System.Environment

  import Network.Socket (withSocketsDo)
  import Control.Monad (forever)
  import Control.Exception (finally)

  import qualified Data.ByteString.UTF8  as U
  import           Data.Char (isDigit)

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [p,q] -> if all isDigit p 
                 then withSocketsDo $ conAndListen (read p) q
                 else error ("Port is not numeric: " ++ p)
      _     -> error ("I need a port and a queue name " ++
                      "(and only a port and a queue name).")

  conAndListen :: Int -> String -> IO ()
  conAndListen p qn = withSocketsDo $ 
    withConnection "127.0.0.1" p [] [] $ \c -> do
      let conv _ _ _ = return . U.toString
      q <- newReader c "Q-Hof" qn [] [] conv
      listen2 q `finally` destroyReader q

  listen2 :: Reader String -> IO ()
  listen2 q = forever $ do
    eiM <- try $ readQ q 
    case eiM of
      Left  e -> putStrLn $ "Error: " ++ show (e::StomplException)
      Right m -> putStrLn (msgContent m)

