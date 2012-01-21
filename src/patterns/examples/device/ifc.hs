module Main
where

  import           Command
  import           Helper
  import           Network.Mom.Patterns
  import           System.Environment

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [p, "stop"]         -> sendCmd (read p) Stop
      [p, "rem", i]       -> sendCmd (read p) $ RemPort i
      [p, "tmo", t]       -> sendCmd (read p) $ Tmo (read t)
      [p, "add", i, l, x] -> case parseLink l of
                               Just lt -> sendCmd (read p) $ 
                                                  AddPort i lt (read x)
                               _       -> usage 
      _          -> usage

  sendCmd :: Int -> Command -> IO ()
  sendCmd p cmd = withContext 1 $ \ctx -> 
                    withClient ctx (address Connect "tcp" "localhost" p []) 
                                   fromCmd toResult $ \c -> do
                      r <- request c (just cmd) (one OK)
                      case r of
                        Right OK      -> return ()
                        Right (Err e) -> error e
                        Left       e  -> error $ show e

  usage :: IO ()
  usage = error $ "ifc <port> <cmd>\n" ++
                  "    cmd: stop\n" ++
                  "    cmd: add identifier linktype port\n" ++
                  "    cmd: rem identifier\n"


