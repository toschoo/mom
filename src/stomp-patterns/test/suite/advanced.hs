module Main
where

  import           System.Exit
  import           System.Timeout
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import           Common

  import           Registry  
  import           Types 

  import           Network.Mom.Stompl.Patterns.Basic -- <--- SUT
  import           Network.Mom.Stompl.Patterns.Balancer
  import           Network.Mom.Stompl.Patterns.Bridge
  import           Network.Mom.Stompl.Patterns.Desk
  import           Network.Mom.Stompl.Client.Queue

  import           Data.List ((\\), nub)
  import           Data.Maybe (catMaybes, fromMaybe)
  import           Data.Char (isAlpha)
  import           Data.Time.Clock
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO)
  import           Control.Monad (void)
  import           Codec.MIME.Type (nullType)

  ------------------------------------------------------------------------
  -- DESK
  ------------------------------------------------------------------------
  -- simple request ------------------------------------------------------
  prpRequest :: NonEmptyList Char -> Property
  prpRequest (NonEmpty ns) = let is = "/q/" ++ qname ns in monadicIO $ do
    s <- run $ testDesk (0,1000) onerr "/q/desk1/service" $ \c ->
      withServerThread c "Srv1" "Service1" nullType [] (return . msgContent)
                   (is, [], [],  stringIn)
                   ("unknown",   [], [], stringOut)
                   ("/q/desk1/reg", 500000, (0,0,0)) onerr $ 
        withClient c "Cl1" "Desk1" 
                   ("/q/client1",       [], [], ignorebody) 
                   ("/q/desk1/service", [], [],     nobody) $ \cl -> do
          (sc,ps) <- requestProvider cl 500000 "Service1" 1
          case sc of
            OK -> if length ps == 1 then return (head ps)
                                    else return ""
            _  -> print sc >> return ""
    assert (s == is)

  -- request n providers -------------------------------------------------
  prpReqN1 :: NonEmptyList Char -> Property
  prpReqN1 (NonEmpty ns) = let is = "/q/" ++ qname ns in monadicIO $ do
    s <- run $ testDesk (0,1000) onerr "/q/desk1/service" $ \c ->
      withServerThread c "Srv1" "Service1" nullType [] (return . msgContent)
                   (is, [], [],  stringIn)
                   ("unknown",   [], [], stringOut)
                   ("/q/desk1/reg", 500000, (0,0,0)) onerr $ 
        withClient c "Cl1" "Desk1" 
                   ("/q/client1",       [], [], ignorebody) 
                   ("/q/desk1/service", [], [],     nobody) $ \cl -> do
          (sc,ps) <- requestProvider cl 500000 "Service1" 5
          case sc of
            OK -> if length ps == 1 then return (head ps)
                                    else return ""
            _  -> print sc >> return ""
    assert (s == is)

  -- request with n servers -----------------------------------------------
  prpNReqs :: Property
  prpNReqs = monadicIO $ do
    n <- pick $ choose (5,10)
    t <- run $ testDesk (0,1000) onerr "/q/desk1/service" $ \c ->
      withServers n c "/q/desk1/reg" $
        withClient  c "Cl1" "Desk1" 
                    ("/q/client1",       [], [], ignorebody) 
                    ("/q/desk1/service", [], [],     nobody) $ \cl -> do
          let x = min 3 $ n `div` 2
          (sc1,ps1) <- requestProvider cl 500000 "Service1" x
          case sc1 of
            OK -> do (sc2,ps2) <- requestProvider cl 50000 "Service1" x
                     case sc2 of
                       OK -> if length ps1 == x   && length ps2 == x &&
                                ps1 \\ ps2 == ps1 && ps2 \\ ps1 == ps2
                               then return True else return False
                       _  -> print sc2 >> return False
            _  -> print sc1 >> return False
    assert t 

  -- request with notfound -----------------------------------------------
  prpReq404:: NonEmptyList Char -> Property
  prpReq404  (NonEmpty is) = monadicIO $ do
    s <- run $ testDesk (0,1000) onerr "/q/desk1/service" $ \c ->
        withClient c "Cl1" "Desk1" 
                   ("/q/client1",       [], [], ignorebody) 
                   ("/q/desk1/service", [], [],     nobody) $ \cl -> do
          (sc,ps) <- requestProvider cl 500000 "Service1" 1
          case sc of
            NotFound -> if null ps then return is
                                   else return ""
            _        -> print   sc >>   return ""
    assert (s == is)

  -- request with notfound, but with providers of other services ---------
  prpReq4042 :: NonEmptyList Char -> Property
  prpReq4042 (NonEmpty ns) = let is = qname ns in monadicIO $ do
    s <- run $ testDesk (0,1000) onerr "/q/desk1/service" $ \c ->
      withServerThread c "Srv1" "Service1" nullType [] (return . msgContent)
                   ("/q/" ++ is, [], [],  stringIn)
                   ("unknown",   [], [], stringOut)
                   ("/q/desk1/reg", 500000, (0,0,0)) onerr $ 
        withClient c "Cl1" "Desk1" 
                   ("/q/client1",       [], [], ignorebody) 
                   ("/q/desk1/service", [], [],     nobody) $ \cl -> do
          (sc,ps) <- requestProvider cl 500000 "Service2" 1
          case sc of
            NotFound -> if null ps then return is
                                   else return ""
            _  -> print sc >> return ""
    assert (s == is)

  -- error

  ------------------------------------------------------------------------
  -- Proxy
  ------------------------------------------------------------------------
  -- proxy ok with heartbeats --------------------------------------------
  prpProxyOK  :: NonEmptyList Char -> Property
  prpProxyOK  (NonEmpty is) = monadicIO $ do
    s <- run $ testDesk (0,1000) onerr "/q/desk1/service" $ \c -> -- /q/desk1/reg
      withPub c "Pub1" "Topic1" "/q/pub/reg" onerr
               ("unknown", [], [], stringOut) $ \p ->
       withClient c "PubC" "Desk" 
                   ("/q/client/in",     [], [], ignorebody)
                   ("/q/desk1/service", [], [],     nobody) $ \d -> 
         withPubProxy c "Proxy" "Topic1"
                                "/q/pub/reg"
                               ("/q/proxy/in", [], [], ignorebody)
                               ("/q/desk1/reg", 500000, (500,100,1000))
                               onerr $ action c d p [] (100::Int)
    assert (not (null s) && head s == is)
    where action _ _ _ rs 0 = return rs
          action c d p rs n = do
            (sc,qs) <- requestProvider d 500000 "Topic1" 1
            case sc of
              OK -> do
                r <- withSub c "Sub1" "Topic1" (head qs) 500000
                              ("/q/sub1/in", [], [], stringIn) $ \s -> do
                  publish p nullType [] is
                  mbM <- checkIssue s 2000000
                  case mbM of
                    Nothing -> putStrLn "no issue" >> return "no"
                    Just m  -> return (msgContent m)
                if null r then return [] else action c d p (r:rs) (n-1)
              _ -> putStrLn "No provider!" >> return []

  -- proxy fails without heartbeats --------------------------------------
  prpProxyNO  :: NonEmptyList Char -> Property
  prpProxyNO  (NonEmpty is) = monadicIO $ do
    s <- run $ testDesk (100,1000) onerr "/q/desk1/service" $ \c -> 
      withPub c "Pub1" "Topic1" "/q/pub/reg" onerr
               ("unknown", [], [], stringOut) $ \p ->
       withClient c "PubC" "Desk" 
                   ("/q/sub/in",        [], [], ignorebody)
                   ("/q/desk1/service", [], [],     nobody) $ \d -> do
         (sc, h) <- register c "Topic1" Topic 
                               "/q/desk1/reg" 
                               "/q/pub/reg" 500000 100
         case sc of
           OK -> if h == 100 then action c d p [] (100::Int)
                             else putStrLn "wrong heartbeat!" >> return []
           _  -> putStrLn "not registered" >> return []
    assert (not (null s) && head s == is)
    where action _ _ _ _  0 = putStrLn "End..." >> return []
          action c d p rs n = do
            (sc,qs) <- requestProvider d 500000 "Topic1" 1
            case sc of
              OK -> do
                rs' <- withSub c "Sub1" "Topic1" (head qs) 500000
                                ("/q/sub1/in", [], [], stringIn) $ \s -> do
                  publish p nullType [] is
                  mbM <- checkIssue s 1000000
                  case mbM of
                    Nothing -> putStrLn "no issue!" >> return []
                    Just m  -> return $ msgContent m:rs
                action c d p rs' (n-1)
              _ -> return rs

  -- error ---------------------------------------------------------------
  prpProxyExc :: Property
  prpProxyExc = monadicIO $ do
    s <- run $ testDesk (0,1000) onerr "/q/desk1/service" $ \c -> -- /q/desk1/reg
      withPub c "Pub1" "Topic1" "/q/pub/reg" onerr
               ("unknown", [], [], stringOut) $ \_ -> do
        m <- newMVar 0
        withPubProxy c "Proxy" "Topic1"
                       "/q/pub/reg"
                      ("/q/proxy/in", [], [], ignorebody)
                      ("/q/desk1/reg", 500000, (100,50,1000))
                      (subtleErr m) $ action (10::Int)
    assert (s == 0)
    where action :: Int -> IO Int
          action 0 = return 0
          action n = threadDelay 1000000 >> action (n-1)
            
  ------------------------------------------------------------------------
  -- Router
  ------------------------------------------------------------------------
  -- simple router -------------------------------------------------------
  prpRouter1 :: NonEmptyList Char -> Property
  prpRouter1 (NonEmpty is) = monadicIO $ do
    s <- run $ testWithCon $ \c -> 
      withPub c "Pub1" "Topic1" "/q/pub/reg" onerr
               ("unknown", [], [], stringOut) $ \p -> 
        withRouter c "Router1" "Topic1" 
                     "/q/pub/reg"
                     "/q/router/internal"
                     "/q/pub/router/out" 500000 onerr $ 
          withSub c "Sub1" "Topic1" "/q/pub/router/out" 500000
                   ("/q/sub1/in", [], [], stringIn) $ \s -> do
            publish p nullType [] is
            mbM <- checkIssue s 1000000
            case mbM of
              Nothing -> return ""
              Just m  -> return $ msgContent m 
    assert (s == is)

  -- router with n subscribers --------------------------------------------
  prpRouterN :: Int -> NonEmptyList Char -> Property
  prpRouterN n (NonEmpty is) | n <= 1 || n > 100 = prpRouterN 10 (NonEmpty is)
                             | otherwise         = monadicIO $ do
    s <- run $ testWithCon $ \c -> 
      withPub c "Pub1" "Topic1" "/q/pub/reg" onerr
               ("unknown", [], [], stringOut) $ \p -> 
        withRouter c "Router1" "Topic1" 
                     "/q/pub/reg"
                     "/q/router/internal"
                     "/q/pub/router/out" 500000 onerr $ 
        
        -- t1 <- getCurrentTime
        withSubs n c "/q/pub/router/out" $ \ss -> do 
          publish p nullType [] is
          m <- newMVar []
          mapM_ (collectR m) ss
          r <- nub <$> waitR m (0::Int)
          -- t2 <- getCurrentTime
          -- putStrLn $ show n ++ " cycles = " ++ show (t2 `diffUTCTime`  t1)
          if length r == 1 then return $ head r
                           else return ""
    assert (s == is)
    where collectR m s = 
            void (forkIO $ do 
                    mbR <- checkIssue s (-1) -- 100000
                    case mbR of
                      Nothing -> throwIO $ TimeoutX "on checkIssue"
                      Just r  -> modifyMVar_ m $ \rs -> 
                                   return (msgContent r:rs))
          waitR m i | i > 1000  = throwIO $ TimeoutX "on waiting"
                    | otherwise = do
            x <- readMVar m
            if length x == n
              then return x
              else threadDelay 100 >> waitR m (i+1)

  -- pass headers through -------------------------------------------------
  prpRtrHdrs :: Property
  prpRtrHdrs = monadicIO $ do
    is <- pick hdrVal
    s  <- run $ testWithCon $ \c -> 
      withPub c "Pub1" "Topic1" "/q/pub/reg" onerr
               ("unknown", [], [], nobody) $ \p ->
        withRouter c "Router1" "Topic1" 
                     "/q/pub/reg"
                     "/q/router/internal"
                     "/q/pub/router/out" 500000 onerr $ 
          withSub c "Sub1" "Topic1" "/q/pub/router/out" 500000
                   ("/q/sub1/in", [], [], ignorebody) $ \s -> do
            publish p nullType [("test-hdr", is)] ()
            mbM <- checkIssue s 1000000
            case mbM of
              Nothing -> return ""
              Just m  -> return $ fromMaybe "" $ lookup "test-hdr" $ msgHdrs m 
    assert (s == is)

  ------------------------------------------------------------------------
  -- Balancer
  -- only special tests (pass headers, error),
  -- since we have already used the balancer extensively in tests
  ------------------------------------------------------------------------
  -- pass headers through -------------------------------------------------
  prpBalHdrs :: Property
  prpBalHdrs = monadicIO $ do
    is <- pick hdrVal
    s  <- run  $ testWithCon $ \c -> 
      withBalancer c "Bal-1" "/q/registry1" (0,5000) 
                             "/q/service1/server" onerr $ 
        withServerThread c "Server1" "Service1" nullType [] job
                            ("/q/service1/internal", [], [], ignorebody)
                            ("unknown",              [], [], stringOut) 
                            ("/q/registry1", 500000, (500,0,1000)) onerr $
          withClient c "SourceCl" "Service1" 
                      ("/q/src/client/in"  , [], [], stringIn)
                      ("/q/service1/server", [], [], nobody) $ \cl -> do
              mbM <- request cl 500000 nullType [("test-hdr", is)] ()
              case mbM of
                Nothing -> return ""
                Just m  -> return $ msgContent m
    assert (s == is)
    where job m = return $ fromMaybe "" $ lookup "test-hdr" $ msgHdrs m 

  -- error

  ------------------------------------------------------------------------
  -- FORWARDER
  ------------------------------------------------------------------------
  -- simple forwarder ----------------------------------------------------
  prpFwd1 :: NonEmptyList Char -> Property
  prpFwd1 (NonEmpty is) = monadicIO $ do
    s <- run $ testWith2Con $ \src dst -> 
      withPub src "Pub1" "Topic1" "/q/pub2bridge" onerr
                 ("unknown", [], [], stringOut) $ \p ->
        withForwarder src dst "Bridge1" "Topic1" 
                              "/q/pub2bridge"
                              "/q/bridge/in"
                              "/q/bridge2sub"
                              500000 onerr $ 
          withSub dst "Sub1" "Topic1" "/q/bridge2sub" 500000
                     ("/q/sub1/in", [], [], stringIn) $ \s -> do
            publish p nullType [] is
            mbM <- checkIssue s 1000000
            case mbM of
              Nothing -> return ""
              Just m  -> return $ msgContent m
    assert (s == is)

  -- forwarder with n subscribers -----------------------------------------
  prpFwdN :: NonEmptyList Char -> Property
  prpFwdN (NonEmpty is) = monadicIO $ do
    n <- pick $ choose (5,10)
    s <- run $ testWith2Con $ \src dst -> 
      withPub src "Pub1" "Topic1" "/q/pub2bridge" onerr
                 ("unknown", [], [], stringOut) $ \p ->
        withForwarder src dst "Bridge1" "Topic1" 
                              "/q/pub2bridge"
                              "/q/bridge/in"
                              "/q/bridge2sub"
                              500000 onerr $ 
          withSubs n dst "/q/bridge2sub" $ \ss -> do
            publish p nullType [] is
            m <- newMVar []
            mapM_ (getIssue m) ss
            rs <- checkM m n (100::Int)
            if length rs == 1 then return $ head rs
                              else return "" 
    assert (s == is)
    where getIssue rm s = void $ forkIO $ do
            mbM <- checkIssue s 1000000
            modifyMVar_ rm (return . (mbM:))
          checkM rm n i | i == 0    = return []
                        | otherwise = do 
                             x <- readMVar rm
                             if length x == n 
                               then return $ nub $ 
                                       map msgContent $ catMaybes x
                               else threadDelay 1000
                                    >> checkM rm n (i-1)

  -- forwarder passes headers through ------------------------------------
  prpFwdHdrs :: Property
  prpFwdHdrs = monadicIO $ do
    is <- pick hdrVal
    s  <- run  $ testWith2Con $ \src dst -> 
      withPub src "Pub1" "Topic1" "/q/pub2bridge" onerr
                 ("unknown", [], [], stringOut) $ \p ->
        withForwarder src dst "Bridge1" "Topic1" 
                              "/q/pub2bridge"
                              "/q/bridge/in"
                              "/q/bridge2sub"
                              500000 onerr $ 
          withSub dst "Sub1" "Topic1" "/q/bridge2sub" 500000
                     ("/q/sub1/in", [], [], stringIn) $ \s -> do
            publish p nullType [("test-hdr", is)] ""
            mbM <- checkIssue s 1000000
            case mbM of
              Nothing -> return ""
              Just m  -> case lookup "test-hdr" $ msgHdrs m of
                           Nothing -> return ""
                           Just h  -> return h
    assert (s == is)

  ------------------------------------------------------------------------
  -- ServiceBridge
  ------------------------------------------------------------------------
  -- Service Bridge straight through --------------------------------------
  prpSrvB1 :: NonEmptyList Char -> Property
  prpSrvB1 (NonEmpty is) = monadicIO $ do
    s <- run $ testWith2Con $ \src dst -> 
      withServerThread dst "TargetSrv" "Service1" nullType [] 
                          (return . reverse . msgContent) 
                          ("/q/dst/srv/in", [], [],  stringIn)
                          ("unknown"      , [], [], stringOut) 
                          ("", 0, (0,0,0)) onerr $ 
        withServiceBridge src dst "Bridge1" "Service1"
                              "/q/src/service1/server" 
                              "/q/dst/service1/client"
                              "/q/dst/srv/in"
                             ("", 500000, (0,0,0)) onerr $ 
          withClient src "SourceCl" "Service1" 
                        ("/q/src/client/in"      , [], [],  stringIn)
                        ("/q/src/service1/server", [], [], stringOut) $ \cl -> do
            mbM <- request cl 500000 nullType [] is
            case mbM of
              Nothing -> return ""
              Just m  -> return $ msgContent m
    assert (s == reverse is)

  -- Service Bridge with balancer on source -----------------------------
  prpSrvBCB1 :: NonEmptyList Char -> Property
  prpSrvBCB1 (NonEmpty is) = monadicIO $ do
    s <- run $ testWith2Con $ \src dst -> 
      withBalancer src "Bal-1" "/q/src/registry1" (0,5000) 
                               "/q/src/service1/server" onerr $ 
        withServerThread dst "TargetSrv" "Service1" nullType [] 
                            (return . reverse . msgContent) 
                            ("/q/dst/srv/in", [], [],  stringIn)
                            ("unknown"      , [], [], stringOut) 
                            ("", 0, (0,0,0)) onerr $ 
          withServiceBridge src dst "Bridge1" "Service1"
                                "/q/src/service1/internal" 
                                "/q/dst/service1/client"
                                "/q/dst/srv/in"
                               ("/q/src/registry1", 500000, 
                                                      (500,0,1000)) onerr $
            withClient src "SourceCl" "Service1" 
                          ("/q/src/client/in"      , [], [],  stringIn)
                          ("/q/src/service1/server", [], [], stringOut) $ \cl -> do
              mbM <- request cl 500000 nullType [] is
              case mbM of
                Nothing -> return ""
                Just m  -> return $ msgContent m
    assert (s == reverse is)

  -- Service Bridge with balancer on target --------------------------------
  prpSrvBDB1 :: NonEmptyList Char -> Property
  prpSrvBDB1 (NonEmpty is) = monadicIO $ do
    s <- run $ testWith2Con $ \src dst -> 
      withBalancer dst "Bal-1" "/q/dst/registry1" (0,5000) 
                               "/q/dst/service1/server" onerr $ 
        withServerThread dst "TargetSrv" "Service1" nullType [] 
                            (return . reverse . msgContent) 
                            ("/q/dst/srv/in", [], [],  stringIn)
                            ("unknown"      , [], [], stringOut) 
                            ("/q/dst/registry1", 500000, (500,0,1000)) onerr $
          withServiceBridge src dst "Bridge1" "Service1"
                                "/q/src/service1/server" 
                                "/q/dst/service1/client"
                                "/q/dst/service1/server"
                               ("", 500000, (0,0,0)) onerr $
            withClient src "SourceCl" "Service1" 
                          ("/q/src/client/in"      , [], [],  stringIn)
                          ("/q/src/service1/server", [], [], stringOut) $ \cl -> do
              mbM <- request cl 500000 nullType [] is
              case mbM of
                Nothing -> return ""
                Just m  -> return $ msgContent m
    assert (s == reverse is)

  ------------------------------------------------------------------------
  -- TaskBridge
  ------------------------------------------------------------------------
  -- Task Bridge straight through ----------------------------------------
  prpTskB1 :: NonEmptyList Char -> Property
  prpTskB1 (NonEmpty is) = monadicIO $ do
    s <- run $ testWith2Con $ \src dst -> do
      m <- newEmptyMVar
      withTaskThread dst "TargetTask" "Task1" 
                          (putMVar m . reverse . msgContent) 
                          ("/q/dst/tsk/in", [], [],  stringIn)
                          ("", 0, (0,0,0)) onerr $ 
        withTaskBridge src dst "Bridge1" "Task1"
                               "/q/src/tsk/in" 
                               "/q/dst/tsk/in"
                              ("", 500000, (0,0,0)) onerr $ 
          withPusher src "SrcPush" "Task1" 
                        ("/q/src/tsk/in", [], [],  stringOut) $ \p -> do
            push p nullType [] is
            mbR <- timeout 500000 $ takeMVar m
            case mbR of
              Nothing -> return ""
              Just r  -> return r
    assert (s == reverse is)

  -- Task Bridge with balancer on source ---------------------------------
  prpTskBCB1 :: NonEmptyList Char -> Property
  prpTskBCB1 (NonEmpty is) = monadicIO $ do
    s <- run $ testWith2Con $ \src dst -> do
      m <- newEmptyMVar
      withBalancer src "Bal-1" "/q/src/registry1" (0,5000) 
                               "/q/src/tsk/in" onerr $ 
        withTaskThread dst "TargetTask" "Task1" 
                            (putMVar m . reverse . msgContent) 
                            ("/q/dst/tsk/in", [], [],  stringIn)
                            ("", 0, (0,0,0)) onerr $ 
          withTaskBridge src dst "Bridge1" "Task1"
                         "/q/src/tsk/internal" 
                         "/q/dst/tsk/in"
                        ("/q/src/registry1", 500000, (500,0,1000)) onerr $
            withPusher src "SrcPush" "Task1" 
                          ("/q/src/tsk/in", [], [],  stringOut) $ \p -> do
              push p nullType [] is
              mbR <- timeout 500000 $ takeMVar m
              case mbR of
                Nothing -> return ""
                Just r  -> return r
    assert (s == reverse is)

  -- Task Bridge with balancer on target ---------------------------------
  prpTskBDB1 :: NonEmptyList Char -> Property
  prpTskBDB1 (NonEmpty is) = monadicIO $ do
    s <- run $ testWith2Con $ \src dst -> do
      m <- newEmptyMVar
      withBalancer dst "Bal-1" "/q/dst/registry1" (0,5000) 
                               "/q/dst/tsk/in" onerr $ 
        withTaskThread dst "TargetTask" "Task1" 
                       (putMVar m . reverse . msgContent) 
                       ("/q/dst/tsk/internal", [], [],  stringIn)
                       ("/q/dst/registry1", 500000, (500,0,1000)) onerr $ 
          withTaskBridge src dst "Bridge1" "Task1"
                         "/q/src/tsk/in" 
                         "/q/dst/tsk/in"
                        ("", 500000, (0,0,0)) onerr $
            withPusher src "SrcPush" "Task1" 
                          ("/q/src/tsk/in", [], [],  stringOut) $ \p -> do
              push p nullType [] is
              mbR <- timeout 500000 $ takeMVar m
              case mbR of
                Nothing -> return ""
                Just r  -> return r
    assert (s == reverse is)

  -- header values -------------------------------------------------
  hdrVal :: Gen String
  hdrVal = do
    dice <- choose (1,255) :: Gen Int
    hdrVal' dice
    where hdrVal' dice = 
            if dice == 0 then return ""
              else do
                c <- hdrChar
                s <- hdrVal' (dice - 1)
                return (c:s)

  -- random char for header values -------------------------------------
  hdrChar :: Gen Char
  hdrChar = elements (['A'..'Z'] ++ ['a'..'z'] ++ ['0'..'9'] ++ 
                      "!\"$%&/()=?<>#§:\n\r\\") -- fail: " \t")

  esc :: String -> String
  esc = foldl (\l -> (++) l . conv) []
    where conv c = case c of 
                     '\n' -> "\\n"
                     '\r' -> "\\r"
                     '\\' -> "\\\\"
                     ':'  -> "\\c"
                     _    -> [c]

  setBack :: Registry -> JobName -> Int -> IO ()
  setBack r jn i = mapAllR r jn (\p -> p{prvNxt = timeAdd (prvNxt p) i})

  setTo :: Registry -> JobName -> UTCTime -> IO ()
  setTo r jn now = mapAllR r jn (\p -> p{prvNxt = now})

  qname :: String -> String
  qname ""     = ""
  qname (c:cs) = if isAlpha c then   c:qname cs
                              else '/':qname cs

  withServers :: Int -> Con -> QName -> IO r -> IO r
  withServers n c rq action = go n 
    where go 0 = action 
          go k = withServerThread c "Srv1" "Service1" nullType [] (return . msgContent)
                   ("/q/servers/" ++ show k, [], [],  stringIn)
                   ("unknown",               [], [], stringOut)
                   (rq, 500000, (0,0,0)) onerr $ go (k-1)

  withClients :: Int -> Con -> QName -> 
                 ([ClientA String String] -> IO a) -> IO a
  withClients n c sq action = withNClient n []
    where withNClient 0 cs = action cs
          withNClient k cs = 
            withClient c ("CL-" ++ show k) "Service1"
                         ("/q/client" ++ show k, [], [],  stringIn)
                         (sq,                    [], [], stringOut) $ \cl ->
              withNClient (k-1) (cl:cs)

  withPushers :: Int -> Con -> QName -> ([PusherA String] -> IO a) -> IO a
  withPushers n c tq action = withNPushers n []
    where withNPushers 0 ps = action ps
          withNPushers k ps = 
            withPusher c ("PUSH-" ++ show k) "Task1"
                         (tq, [], [], stringOut) $ \p ->
               withNPushers (k-1) (p:ps)

  withSubs :: Int -> Con -> QName -> ([SubA String] -> IO a) -> IO a
  withSubs n c rq action  = withNSubs n []
    where withNSubs 0 ss  = action ss
          withNSubs k ss  = 
            withSub c ("Sub"    ++ show k) "Topic1" rq 500000
                      ("/q/sub" ++ show k, [], [], stringIn) $ \s ->
              withNSubs (k-1) (s:ss)

  testWithBalancer :: QName -> (Con -> IO a) -> IO a
  testWithBalancer jq action =
    withConnection "localhost" 61613 [] [] $ \c -> 
      withBalancer c "Bal-1" "/q/registry1" (0,5000) jq onerr $ action c

  testWithWorker :: MVar String -> Con -> QName -> QName -> 
                                  (Con -> IO a) -> IO a
  testWithWorker m c rq sq action = 
    withTaskThread c "Task-1" "Task1" tsk 
                     (sq, [], [],  stringIn)
                     (rq, 500000, (500,0,5000))
                     onerr $ action c
    where tsk msg = putMVar m (msgContent msg) 

  testWithReg :: (Registry -> IO a) -> IO a
  testWithReg action =
    withConnection "localhost" 61613 [] [] $ \c -> 
      withRegistry c "Test-1" "/q/registry1" (0,0) onerr action

  testWithCon :: (Con -> IO a) -> IO a
  testWithCon = withConnection "localhost" 61613 [] [] 

  testWith2Con :: (Con -> Con -> IO r) -> IO r
  testWith2Con action = 
    withConnection "localhost" 61613 [] [] $ \c1 ->
    withConnection "localhost" 61615 [] [] $ \c2 -> action c1 c2

  testDesk :: (Int, Int) -> OnError -> QName -> (Con -> IO r) -> IO r
  testDesk (mn,mx) onErr dq action = 
    testWithCon $ \c ->
      withDesk c "Desk1" "/q/desk1/reg" (mn,mx) onErr dq $ action c
                   
  onerr :: OnError
  onerr e m = putStrLn $ "Error in " ++ m ++ ": " ++ show e

  nonemptyString :: NonEmptyList (NonEmptyList Char) -> [String]
  nonemptyString (NonEmpty ns) = map (\(NonEmpty c) -> c) ns

  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    putStrLn "========================================="
    putStrLn "   Stompl Patterns Library Test Suite"
    putStrLn "               Advanced"
    putStrLn "========================================="
    -- o <- newMVar 0
    r <- -- testWithCon $ \c -> do
         runTest "Desk: simple request"
                  (deepCheck prpRequest)      ?> 
         runTest "Desk: request n, get 1"
                  (deepCheck prpReqN1)        ?> 
         runTest "Desk: n requests"
                  (someCheck 10 prpNReqs)     ?> 
         runTest "Desk: request - 404"
                  (deepCheck prpReq404)       ?> 
         runTest "Desk: request - 404, with service"
                  (deepCheck prpReq4042)      ?> 
         runTest "Proxy: heartbeats"
                  (oneCheck prpProxyOK)       ?>  
         runTest "Proxy: no heartbeat"
                  (oneCheck prpProxyNO)       ?> 
         runTest "Proxy: continues on Exc"
                  (oneCheck prpProxyExc)      ?> 
         runTest "Router: publish 1 via router"
                  (someCheck 50 prpRouter1)   ?> 
         runTest "Router: publish n via router"
                  (someCheck 10 prpRouterN)   ?> 
         runTest "Router: pass headers through"
                  (someCheck 50 prpRtrHdrs)   ?> 
         runTest "Balancer: pass headers through"
                  (someCheck 50 prpBalHdrs)   ?>
         runTest "Forwd. 1 sub"
                  (someCheck 30 prpFwd1)      ?> 
         runTest "Forwd. n sub"
                  (someCheck 10 prpFwdN)      ?> 
         runTest "Forwd.: Hdrs pass through"
                  (someCheck 100 prpFwdHdrs)  ?> 
         runTest "SrvBrd: simple request"
                  (deepCheck prpSrvB1 )       ?> 
         runTest "SrvBrd: request with source registry"
                  (someCheck 100 prpSrvBCB1 ) ?> 
         runTest "SrvBrd: request with target registry"
                  (someCheck 100 prpSrvBDB1 ) ?>
         runTest "TskBrd: simple push"
                  (someCheck 100 prpTskB1)    ?>
         runTest "TskBrd: push with source registry"
                  (someCheck 100 prpTskBCB1)  ?> 
         runTest "TskBrd: push with destination registry"
                  (someCheck 100 prpTskBDB1) 
         
         
    case r of
      Success {} -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
