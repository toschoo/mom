module Main
where

  import Test

  import qualified Protocol as P
  import qualified Socket   as Sock
  import qualified Factory  as Fac
 
  import           Network.Mom.Stompl.Client.Exception 
  import qualified Network.Mom.Stompl.Frame as F

  import           Codec.MIME.Type (nullType)

  import qualified Network.Socket as S

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U

  import           System.Exit
  
  maxRcv :: Int
  maxRcv = 1024

  host :: String
  host = "127.0.0.1"

  port :: Int
  port = 22222

  vers :: [F.Version]
  vers = [(1,0), (1,1), (1,2)]

  beat :: F.Heart
  beat = (0,0)

  msg1, q1, text1 :: String
  msg1  = "msg-1"
  q1    = "/q/test-1"
  text1 = "what is just to say..."

  tx1, rc1, sub1 :: Int
  tx1   = 1
  rc1   = 1
  sub1  = 1

  main :: IO ()
  main = do
    r <- runTests mkTests
    case r of
      Pass   -> exitSuccess
      Fail _ -> exitFailure

  runTests :: TestGroup IO -> IO TestResult
  runTests g = do
    (r, s) <- execGroup g
    putStrLn s
    return r

  mkTests :: TestGroup IO
  mkTests = 
    let t10  = mkTest "Socket   Connect" testConFrame
        t20  = mkTest "Protocol Connect" $ testConnect F.Connect
        t25  = mkTest "Protocol Stomp  " $ testConnect F.Stomp
        t30  = mkTest "Begin           " $ testWith (\c -> P.begin  c (show tx1) 
                                                                      (show Fac.NoRec)) 
                                                    (F.mkBegin  (show tx1) 
                                                                (show Fac.NoRec) []) 
        t40  = mkTest "Begin with Rc   " $ testWith (\c -> P.begin  c (show tx1) (show rc1)) 
                                                    (F.mkBegin  (show tx1) (show rc1) []) 
        t50  = mkTest "Commit          " $ testWith (\c -> P.commit c (show tx1) 
                                                                      (show Fac.NoRec)) 
                                                    (F.mkCommit (show tx1) 
                                                                (show Fac.NoRec) [])
        t60  = mkTest "Commit with Rc  " $ testWith (\c -> P.commit c (show tx1) (show rc1)) 
                                                    (F.mkCommit (show tx1) (show rc1) [])
        t70  = mkTest "Abort           " $ testWith (\c -> P.abort  c (show tx1) 
                                                                      (show Fac.NoRec)) 
                                                    (F.mkAbort  (show tx1) 
                                                                (show Fac.NoRec) [])
        t80  = mkTest "Abort with Rc   " $ testWith (\c -> P.abort  c (show tx1) (show rc1)) 
                                                    (F.mkAbort  (show tx1) (show rc1) [])
        t100 = mkTest "Ack             " $ testWith (testAck True  
                                                             Fac.NoTx 
                                                             Fac.NoSub
                                                             "ack-1" 
                                                             text1 
                                                             Fac.NoRec)
                                                    (F.mkAck "ack-1" "" "" "" [])
        t110 = mkTest "Nack            " $ testWith (testAck False 
                                                             Fac.NoTx 
                                                             Fac.NoSub 
                                                             "ack-1" 
                                                             text1 
                                                             Fac.NoRec) 
                                                    (F.mkNack "ack-1" "" "" "" [])
        t120 = mkTest "Ack with Tx     " $ testWith (testAck True  
                                                             (Fac.Tx tx1) 
                                                             Fac.NoSub 
                                                             "ack-1" 
                                                             text1 
                                                             Fac.NoRec) 
                                                    (F.mkAck  "ack-1" "" (show tx1) "" [])
        t130 = mkTest "Nack with Tx    " $ testWith (testAck False 
                                                             (Fac.Tx tx1) 
                                                             Fac.NoSub 
                                                             "ack-1" 
                                                             text1 
                                                             Fac.NoRec) 
                                                    (F.mkNack "ack-1" "" (show tx1) "" [])
        t140 = mkTest "Ack with Rc     " $ testWith (testAck True  
                                                             (Fac.Tx tx1) 
                                                             Fac.NoSub 
                                                             "ack-1" 
                                                             text1 
                                                             (Fac.Rec rc1)) 
                                                    (F.mkAck  "ack-1" "" (show tx1) (show rc1) [])
        t150 = mkTest "Nack with Rc    " $ testWith (testAck False 
                                                             (Fac.Tx tx1) 
                                                             Fac.NoSub 
                                                             "ack-1" 
                                                             text1 
                                                             (Fac.Rec rc1)) 
                                                    (F.mkNack "ack-1" "" (show tx1) (show rc1) [])
        t160 = mkTest "Ack with Sub    " $ testWith (testAck True  
                                                             (Fac.Tx tx1) 
                                                             (Fac.Sub sub1)
                                                             "ack-1" 
                                                             text1 
                                                             (Fac.Rec rc1)) 
                                                    (F.mkAck  "ack-1" (show sub1) (show tx1) (show rc1) [])
        t170 = mkTest "Nack with Sub   " $ testWith (testAck False 
                                                             (Fac.Tx tx1) 
                                                             (Fac.Sub sub1)
                                                             "ack-1" 
                                                             text1 
                                                             (Fac.Rec rc1)) 
                                                    (F.mkNack "ack-1" (show sub1) (show tx1) (show rc1) [])
        t180 = mkTest "Send            " $ testWith (testSend 
                                                       Fac.NoTx Fac.NoRec text1 [])
                                                    (mkSend
                                                       Fac.NoTx Fac.NoRec text1 [])
        t190 = mkTest "Send with Hdrs  " $ testWith (testSend 
                                                       Fac.NoTx Fac.NoRec text1 
                                                       [("another","some text"),
                                                        ("special","more text")])
                                                    (mkSend
                                                       Fac.NoTx Fac.NoRec text1 
                                                       [("another","some text"),
                                                        ("special","more text")])
        t200 = mkTest "Send with Tx    " $ testWith (testSend 
                                                       (Fac.Tx tx1)
                                                       Fac.NoRec text1 [])
                                                    (mkSend
                                                       (Fac.Tx tx1)
                                                       Fac.NoRec text1 [])
        t210 = mkTest "Send with Rc    " $ testWith (testSend 
                                                       (Fac.Tx tx1)
                                                       (Fac.Rec rc1) text1 [])
                                                    (mkSend
                                                       (Fac.Tx tx1)
                                                       (Fac.Rec rc1) text1 [])
        t220 = mkTest "Subscribe       " $ testWith (\c -> P.subscribe c
                                                           (P.mkSub Fac.NoSub q1
                                                                    F.Auto)  
                                                           (show Fac.NoRec) [])
                                                    (F.mkSubscribe q1 F.Auto "" 
                                                             (show Fac.NoSub)
                                                             (show Fac.NoRec) [])
        t230 = mkTest "Sub with Sub    " $ testWith (\c -> P.subscribe c
                                                           (P.mkSub (Fac.Sub sub1) q1
                                                                    F.Auto)  
                                                           (show Fac.NoRec) [])
                                                    (F.mkSubscribe q1 F.Auto "" 
                                                             (show $ Fac.Sub sub1)
                                                             (show Fac.NoRec) [])
        t240 = mkTest "Sub with Rc     " $ testWith (\c -> P.subscribe c
                                                           (P.mkSub (Fac.Sub sub1) q1
                                                                    F.Auto)  
                                                           (show $ Fac.Rec rc1) [])
                                                    (F.mkSubscribe q1 F.Auto "" 
                                                             (show $ Fac.Sub sub1)
                                                             (show $ Fac.Rec rc1) [])
        t250 = mkTest "Sub with Client " $ testWith (\c -> P.subscribe c
                                                           (P.mkSub (Fac.Sub sub1) q1
                                                                    F.Client)  
                                                           (show $ Fac.Rec rc1) [])
                                                    (F.mkSubscribe q1 F.Client "" 
                                                             (show $ Fac.Sub sub1)
                                                             (show $ Fac.Rec rc1) [])
        t260 = mkTest "Sub with ClientI" $ testWith (\c -> P.subscribe c
                                                           (P.mkSub (Fac.Sub sub1) q1
                                                                    F.ClientIndi)  
                                                           (show $ Fac.Rec rc1) [])
                                                    (F.mkSubscribe q1 F.ClientIndi "" 
                                                             (show $ Fac.Sub sub1)
                                                             (show $ Fac.Rec rc1) [])
        t270 = mkTest "Sub with Sel.   " $ testWith (\c -> P.subscribe c
                                                           (P.mkSub (Fac.Sub sub1) q1
                                                                    F.Client)
                                                           (show $ Fac.Rec rc1) 
                                                           [("selector","@x='y'")])
                                                    (F.mkSubscribe q1 F.Client "@x='y'" 
                                                             (show $ Fac.Sub sub1)
                                                             (show $ Fac.Rec rc1) [])
        t280 = mkTest "Unsub           " $ testWith (\c -> P.unsubscribe c
                                                           (P.mkSub (Fac.Sub sub1) q1
                                                                    F.Client)
                                                           (show $ Fac.Rec rc1) [])
                                                    (F.mkUnsubscribe q1  
                                                             (show $ Fac.Sub sub1)
                                                             (show $ Fac.Rec rc1) [])
        t290 = mkTest "Unsub no Sub    " $ testWith (\c -> do let sub = P.mkSub Fac.NoSub q1
                                                                                F.Client
                                                              P.unsubscribe c sub
                                                                  (show $ Fac.Rec rc1) [])
                                                    (F.mkUnsubscribe q1  
                                                             (show   Fac.NoSub)
                                                             (show $ Fac.Rec rc1) [])
        t300 = mkTest "HeartBeat       " $ testWith P.sendBeat 
                                                    F.mkBeat
    in  mkGroup "Protocol Tests" (Stop (Fail "")) [
            t10,  t20,  t25,  t30,  t40,  t50,  t60,  t70,  t80, t100,
           t110, t120, t130,       t140, t150, t160, t170, t180, t190, 
           t200, t210, t220, t230, t240, t250, t260, t270, t280, t290, t300] 

  testConFrame :: IO TestResult
  testConFrame = 
    case F.mkConFrame [F.mkLogHdr  "guest",
                       F.mkPassHdr "guest",
                       F.mkAcVerHdr $ F.versToVal vers, 
                       F.mkBeatHdr  $ F.beatToVal beat] of
      Left  e -> return $ Fail e
      Right cf -> do
        eiC <- connect 
        case eiC of 
          Left e -> return $ Fail e
          Right (s, rc, wr) -> do
            Sock.send wr s cf
            eiF <- Sock.receive rc s maxRcv
            S.sClose s
            case eiF of
              Left  e -> return $ Fail e
              Right f -> case F.typeOf f of
                              F.Connected -> return Pass
                              _           -> return $ Fail $ 
                                               "Unexpected Frame: " ++ show f

  testConnect :: F.FrameType -> IO TestResult
  testConnect t = do
    c <- P.connect host port maxRcv "guest" "guest" "" t vers beat []
    let r = if P.connected c 
              then Pass
              else Fail $ "Not connected: " ++ P.getErr c
    -- _ <- P.disc c
    S.sClose (P.getSock c)
    return r

  testAck :: Bool -> Fac.Tx -> Fac.Sub -> String -> String -> 
             Fac.Rec -> P.Connection -> IO ()
  testAck ok tx sub ak m rc c = do
    let b   = U.fromString m
    let msg = P.mkMessage (P.MsgId msg1) sub q1 ak
                          nullType (B.length b) 
                          tx b m
    if ok then P.ack  c msg (show rc)
          else P.nack c msg (show rc)

  testSend :: Fac.Tx -> Fac.Rec -> String -> [F.Header] -> P.Connection -> IO ()
  testSend tx rc m hs c = do
    let b   = U.fromString m
    let msg = P.mkMessage P.NoMsg Fac.NoSub q1 "ack-1" 
                nullType (B.length b) tx b m
    P.send c msg (show rc) hs

  mkSend :: Fac.Tx -> Fac.Rec -> String -> [F.Header] -> F.Frame
  mkSend tx rc m hs =
    let b = U.fromString m
        l = B.length b
    in F.mkSend q1 
       (show tx) (show rc) nullType l hs b
    
  testWith :: (P.Connection -> IO ()) -> F.Frame -> IO TestResult
  testWith act tertium = do
    c <- P.connect host port maxRcv "guest" "guest" "" F.Connect vers beat []
    if not (P.connected c)
      then return $ Fail $ P.getErr c
      else do
        eiR <- try $ act c
        case eiR of
          Left e  -> do
            _ <- P.disc c
            return $ Fail $ show e
          Right _ -> do
            eiF <- Sock.receive (P.getRc c) (P.getSock c) (P.conMax c)
            -- c'   <- P.disconnect c "" -- this causes 'resource busy'
                                         -- (laziness issue)
            _ <- P.disc c
            case eiF of
              Left  e -> return $ Fail e
              Right f -> 
                if f == tertium then return Pass
                  else return $ Fail $ "Frame does not equal pattern, " ++
                          "expected: "   ++ show tertium ++
                          ", received: " ++ show f

  connect :: IO (Either String (S.Socket, Sock.Receiver, Sock.Writer))
  connect = 
    case F.mkConFrame [F.mkLogHdr  "guest",
                       F.mkPassHdr "guest",
                       F.mkAcVerHdr $ F.versToVal vers, 
                       F.mkBeatHdr  $ F.beatToVal beat] of
      Left  e -> return $ Left e
      Right c -> do
        s  <- Sock.connect host port
        rc <- Sock.initReceiver
        wr <- Sock.initWriter
        Sock.send wr s c
        eiF <- Sock.receive rc s maxRcv
        case eiF of
          Left e  -> do
            S.sClose s
            return $ Left e
          Right f -> 
            case F.typeOf f of
              F.Connected -> return $ Right (s, rc, wr)
              _           -> return $ Left $ "Unexpected Frame: " ++ show f

