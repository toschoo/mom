module Main
where

  import System.Environment (getArgs)
  import Text.Stomp.Parser
  import Network.Stomp.Frame

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      []  -> error "I need a file name"
      [f] -> do 
        c <- parseFile f 
        case c of
          Left  e  -> error $ "Error: " ++ (show e)
          Right m -> putStrLn $ show m 
      _ -> error "I don't know what to do with all the arguments!"   


