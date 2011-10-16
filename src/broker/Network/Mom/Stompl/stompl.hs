module Main 
where

  import Server
  import System.Environment (getArgs)

  main :: IO ()
  main = do
    args <- getArgs
    case args of
      [f] -> startServer f
      _   -> putStrLn "I need one argument"

