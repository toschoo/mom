module Main where

  import Network.Mom.Stompl.CfgParser

  main :: IO ()
  main = do
    eiCfg <- getConfig "test/cfg/stompl.cfg"
    case eiCfg of
      Left  s -> putStrLn s
      Right c -> putStrLn $ show c
    
