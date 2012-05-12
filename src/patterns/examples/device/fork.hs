module Main 
where

  ------------------------------------------------------------------------
  -- Device that sends data streams coming from a connected publisher
  -- to all connected subscribers;
  -- subscribers can be connected and removed through the "ifc" program.
  -- The program cannot be stopped by the INT signal.
  -- Instead, it is stopped by using the "stop" command with ifc.
  ------------------------------------------------------------------------

  import           Command
  import           Helper (getPorts, address, onErr_, onErr)
  import           Network.Mom.Patterns
  import           Control.Concurrent
  import           System.Posix.Signals

  main :: IO ()
  main = do
    ((l1, p1), (l2, p2), r) <- getPorts
    (l3, p3) <- case r of
                  [l,p] -> case getThird l p of
                             Nothing -> usage
                             Just lp -> return lp
                  _     -> print r >> usage
    let sub = pollEntry "Subscriber" XSub
                        (address l1 "tcp" "localhost" p1 []) l1 [""]
    let pub = pollEntry "Publisher"  XPub
                        (address l2 "tcp" "localhost" p2 []) l2 []
    -- don't stop process on sigINT
    _ <- installHandler sigINT Ignore Nothing
    withContext 1 $ \ctx -> 
      withDevice ctx "Forker" noparam (-1)
                     [sub, pub] idIn idOut onErr_ 
                     (\_ -> putStrLn "Timeout!") 
                     (\_ -> putThrough) $ \d -> do
          cc <- newChan
          rc <- newChan
          withServer ctx "Commander" noparam 1
                         (address l3 "tcp" "localhost" p3 []) l3
                         toCmd fromResult onErr
                         (\_ -> one Empty) 
                         (fetch1 (commander cc rc)) $ go cc rc d
       where go cc rc d s = do
              cmd <- readChan cc
              putStrLn $ srvName s ++ " received: " ++ show cmd
              case cmd of
                Empty         -> writeChan rc OK >> go cc rc d s
                Stop          -> do
                  writeChan rc OK
                  threadDelay 10000
                Tmo     t     -> do
                  changeTimeout d t
                  writeChan rc OK
                  go cc rc d s
                AddPort i l p -> do
                  let e = pollEntry i XPub 
                            (address l "tcp" "localhost" p []) l []
                  addDevice d e
                  writeChan rc OK
                  go cc rc d s
                RemPort i     -> do
                  remDevice d i
                  writeChan rc OK 
                  go cc rc d s

  getThird :: String -> String -> Maybe (LinkType, Int)
  getThird l p = 
     case parseLink l of
       Just t -> Just (t, read p)
       _      -> Nothing
                 
  usage :: IO a
  usage = error $ "fork 'bind' | 'connect' <incoming port>\n" ++
                  "     'bind' | 'connect' <outgoing port>\n" ++
                  "     'bind' | 'connect' <command  port>\n" 

  commander :: Chan Command -> Chan Result -> FetchHelper' Command Result
  commander cc rc _ _ cmd = writeChan cc cmd >> readChan rc
