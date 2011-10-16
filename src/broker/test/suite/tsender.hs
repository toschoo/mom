module Main
where

  import Network.Mom.Stompl.Types
  import Network.Mom.Stompl.Frame
  import Network.Mom.Stompl.Config
  import Network.Mom.Stompl.Sender

  import Control.Concurrent
  import Control.Monad.State

  import System.Exit
  import qualified Network.Socket as S
  import           Network.BSD (getProtocolNumber) 

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U

  import Data.List (delete)

  main :: IO ()
  main = tSender

  cid :: Int
  cid = 1

  tSender :: IO ()
  tSender = do
    s   <- mkSocket "127.0.0.1" 5432
    v   <- mkConfig "test/cfg/stompl.cfg" dummyChange dummyChange
    cfg <- readMVar v
    ok <- evalStateT (applyTests s) $ mkBook (getSender cfg) [] [] []
    case ok of
      False -> do
        putStrLn "Bad. Some Tests failed"
        exitFailure
      True -> do
        putStrLn "Ok. All Tests passed"
        exitSuccess

  mkSocket :: String -> S.PortNumber -> IO S.Socket
  mkSocket h p = do
    proto <- getProtocolNumber "tcp"
    sock  <- S.socket S.AF_INET S.Stream proto
    addr  <- S.inet_addr h
    S.bindSocket sock (S.SockAddrInet p addr)
    return sock

  applyTests :: S.Socket -> Sender Bool
  applyTests s = do
    (return True)                              ?>
      -- Test connect/disconnect 
      putTitle "--- Simple connect/disconnect" ?>
      testConnect s                            ?> 
      testDisconnect                           ?>

      putLn                                    ?>

      -- Test subscribe/unsubscribe
      -- with id
      putTitle "--- subscribe/unsubscribe with Id" ?>
      testConnect s                                ?>
      testSub   "sub-1" 0 Auto                     ?>
      testUnSub "sub-1" 0                          ?>

      putLn                                        ?>

      -- Test subscribe with id / unsubscribe without id
      putTitle "--- subscribe with id / unsubscribe without" ?>
      testSub   "sub-1" 0 Auto ?>
      (do 
         ok <- testUnSub "" 0 
         return $ not ok)      ?>
      testUnSub "sub-1" 0      ?>

      putLn                    ?>

      -- Test subscribe/unsubscribe without id 
      putTitle "--- subscribe/unsubscribe without id" ?>
      testSub   "" 0 Auto      ?>
      testUnSub "" 0           ?>

      putLn                    ?>

      -- Test multiple subscribe / unsubscribe without id
      putTitle "--- Multiple subscribe/unsubscribe without id" ?>
      testSub   "" 0 Auto      ?>
      testSub   "" 1 Auto      ?>
      testSub   "" 2 Auto      ?>
      testUnSub "" 2           ?>
      testUnSub "" 1           ?>
      testUnSub "" 0           ?>

      putLn                    ?>

      -- Test Send --
      putTitle "--- Send "     ?>
      testSub   "sub-1" 0 Auto ?>
      testSend                 ?>
      testUnSub "sub-1" 0      ?>

      putLn                    ?>

      -- Test Send with Ack --
      putTitle "--- Send in client mode " ?>
      testDisconnect                      ?>
      testConnect s                       ?> 
      testSub   "sub-1" 0 Client          ?>
      testSend                            ?>
      testAck                             ?>
      testUnSub "sub-1" 0                 ?>

      putLn                    

  putLn :: Sender Bool
  putLn = do 
    liftIO $ putStrLn ""
    return True

  putTitle :: String -> Sender Bool
  putTitle s = do
    liftIO $ putStrLn s
    return True
       
  testConnect :: S.Socket -> Sender Bool
  testConnect s = do
    handleRequest $ RegMsg cid s
    b <- get
    case bookCons b of
      [] -> do
        liftIO $ putStrLn $ "Cannot add Connection." 
        return False
      [c] -> do
        liftIO $ putStrLn $ "New Connection: " ++ (show c) ++ "."
        return True

  testDisconnect :: Sender Bool
  testDisconnect = do
    handleRequest $ UnRegMsg cid
    b <- get
    case bookCons b of
      [] -> do
        liftIO $ putStrLn $ "Connection removed." 
        return True
      _  -> do
        liftIO $ putStrLn $ "Cannot remove connection." 
        return False

  testSub :: String -> Int -> AckMode -> Sender Bool
  testSub sid l a = 
    case mkSubscribe sid a of
      Left e -> do
        liftIO $ putStrLn $ "Cannot create Subscription Frame."
        return False
      Right f -> do
        handleRequest $ FrameMsg cid f
        b <- get
        case bookSubs b of
          [] -> do
            liftIO $ putStrLn $ "Cannot subscribe."
            return False
          [s] -> do
            liftIO $ putStrLn $ "New Subscription: " ++ (show s) ++ "."
            if l == 0 then return True else return False
          s   -> do
            liftIO $ putStrLn $ "Many Subscriptions: " ++ (show s) ++ "."
            if length s == l + 1 then return True else return False

  testUnSub :: String -> Int -> Sender Bool
  testUnSub sid l =
    case mkUnSubscribe sid of
      Left e -> do
        liftIO $ putStrLn $ "Cannot create UnSubscribe Frame."
        return False
      Right f -> do
        handleRequest $ FrameMsg cid f
        b <- get
        case bookSubs b of
          [] -> do
            liftIO $ putStrLn $ "Subscription removed."
            if l == 0 then return True else return False
          s  -> do
            if length s == l
              then do 
                liftIO $ putStrLn $ "Subscription removed."
                return True
              else do
                liftIO $ putStrLn $ "Cannot remove Subscription."
                return False

  testSend :: Sender Bool
  testSend = do
    case mkSend of
      Left e -> do
        liftIO $ putStrLn $ "Cannot create Send Frame."
        return False
      Right f -> do
        handleRequest $ FrameMsg cid f
        mbM <- getLastMsg
        case mbM of
          Nothing -> return False
          Just m -> do
            liftIO $ putStrLn $ "Message found: " ++ (show m)
            return True

  testAck :: Sender Bool
  testAck = do
    mbM <- getLastMsg 
    case mbM of
      Nothing -> return False
      Just m  -> do
        let m'  = m {strState = Sent}
        updMsg m'
        liftIO $ putStrLn $ "Message: " ++ (show m')
        case mkAck $ strId m of 
          Left e  -> do
            liftIO $ putStrLn $ "Cannot create Ack Frame: " ++ e ++ "."
            return False
          Right f -> do
            handleRequest $ FrameMsg cid f
            mbM2 <- getLastMsg
            case mbM2 of
              Nothing -> do
                liftIO $ putStrLn $ "Acknowledged message was removed."
                return True
              Just n  -> do
                liftIO $ putStrLn $ "Message was not removed. " ++
                                    "State is: " ++ (show $ strState n)
                return False
        
  updMsg :: MsgStore -> Sender ()
  updMsg m = do
    b <- get
    case getCon cid $ bookCons b of
      Nothing -> do
        liftIO $ putStrLn $ "Connection " ++ (show cid) ++ " not found."
        return ()
      Just c -> do
        let ms' = case conPending c of
                    [] -> [m]
                    ms -> m : tail ms
        let c'  = c {conPending = ms'}
        put b {bookCons = c' : (delete c $ bookCons b)}

  getLastMsg :: Sender (Maybe MsgStore)
  getLastMsg = do
    b <- get
    let cc = bookCons b
    case getCon cid cc of
      Nothing -> do
        liftIO $ putStrLn $ "Connection " ++ (show cid) ++ " not found."
        return Nothing
      Just c  -> do
        case conPending c of
          [] -> do
            liftIO $ putStrLn $ "No Message found."
            return Nothing
          ms -> return $ Just $ head ms

  applyB :: Sender Bool -> Sender Bool -> Sender Bool
  applyB f g = f >>= \ok -> 
                 if ok then g else return False

  infixr 9 ?>
  (?>) = applyB

  mkConnect :: Either String Frame
  mkConnect = 
    mkConFrame [mkLogHdr  "guest",
                mkPassHdr "guest"] 

  mkSubscribe :: String -> AckMode -> Either String Frame
  mkSubscribe sid a =
    let ih = if null sid then [] else [mkIdHdr sid] 
    in  mkSubFrame (ih ++ [mkDestHdr "/queue/a",
                           mkAckHdr  (show a)])

  mkUnSubscribe :: String -> Either String Frame
  mkUnSubscribe sid =
    let hs = if null sid then [mkDestHdr "/queue/a"] 
                         else [mkIdHdr   sid]
    in  mkUSubFrame hs

  mkSend :: Either String Frame
  mkSend = 
    let b = B.pack "hello world"
    in  mkSndFrame [mkDestHdr "/queue/a"]
                   (B.length b) b

  mkAck :: String -> Either String Frame
  mkAck mid = 
    mkAckFrame [mkMIdHdr mid]
               
  dummyChange :: ChangeAction
  dummyChange _ = return ()

  
