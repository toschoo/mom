module Main
where

  import Data.Configurator as C
  import Data.Configurator.Types
  import Control.Exception.Base
  import Control.Concurrent
  import Data.Text (Text, pack)

  config :: AutoConfig
  config = AutoConfig 5 onExc

  onExc :: SomeException -> IO ()
  onExc e = putStrLn $ show e

  onChange :: ChangeHandler
  onChange n v = do
    let val = case v of
                Nothing         -> "Nothing"
                Just (List vs ) -> "List"
                Just t          -> show t
    putStrLn $ "Change of " ++ (show n) ++ ": " ++ val

  strLookup :: Config -> String -> IO ()
  strLookup cfg s = do
    mbN <- C.lookup cfg $ pack s
    case mbN of
      Nothing -> putStrLn $ s ++ " not found"
      Just n  -> putStrLn $ s ++ ": " ++ n

  doLookups :: IO ()
  doLookups = do
    threadDelay 5000000
    doLookups 

  main :: IO ()
  main = do
    (cfg, tid) <- autoReload config [Required "test/cfg/stompl.cfg"]
    subscribe cfg (prefix $ pack "Broker") onChange
    doLookups 
