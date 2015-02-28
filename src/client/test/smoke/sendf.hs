module Main
where

  import Network.Mom.Stompl.Client.Queue
  import qualified Data.ByteString.Char8 as B
  import Data.Char (isDigit)
  import System.Environment
  import Network.Socket
  import Codec.MIME.Type (nullType)

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [p,q,f] -> if all isDigit p
                   then withSocketsDo $ conAndSend (read p) q f
                   else error ("Port is not numeric: " ++ p)
      _      -> error ("I need a port, a queue name and a message " ++
                       "and nothing else.")

  conAndSend :: Int -> String -> String -> IO ()
  conAndSend p qn f = do
    m <- B.readFile f
    withConnection "127.0.0.1" p [] [] $ \c -> do
      let conv = return 
      q <- newWriter c "Test-Q" qn [] [] conv
      writeQ q nullType [] m
