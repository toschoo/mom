module Main 
where

  import System.Cmd (rawSystem)
  import System.Exit

  main :: IO ()
  main = do
    e <- rawSystem "make" ["all", "-f", "test/src/Makefile"]
    case e of
      ExitSuccess -> exitSuccess
      _           -> exitFailure
