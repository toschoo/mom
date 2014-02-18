module Main
where 
  
  import System.Environment
  import System.Exit
  import Network.Mom.Stompl.Parser
  import Network.Mom.Stompl.Frame
  import qualified Data.ByteString as B
  -- import           Data.Attoparsec
  -- import           Data.Word

  main :: IO ()
  main = do
    as <- getArgs
    case as of
      [f] -> runTest f
      _   -> putStrLn "I just need a filename!" >> exitFailure

  runTest :: String -> IO ()
  runTest f = do m <- B.readFile f
                 case stompAtOnce m of
                   Left  e -> putStrLn $ "Parse failed: " ++ e
                   Right x -> print $ putFrame x
    
