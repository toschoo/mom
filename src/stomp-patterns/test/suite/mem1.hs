module Main
where

  import           System.Exit
  import           System.Environment
  import           Test.QuickCheck
  import           Common

  import           Registry  
  import           Types 

  import           Network.Mom.Stompl.Patterns.Basic -- <--- SUT
  import           Network.Mom.Stompl.Patterns.Balancer
  import           Network.Mom.Stompl.Patterns.Desk
  import           Network.Mom.Stompl.Client.Queue

  import           Control.Concurrent
  import           Prelude hiding (catch)
  import           Control.Monad (when)
  import           Codec.MIME.Type (nullType)

  ------------------------------------------------------------------------
  -- DESK
  ------------------------------------------------------------------------
  deskReq :: Con -> QName -> QName -> IO ()
  deskReq c reg srv = 
      withServerThread c "Srv1" "Service1" nullType [] (return . msgContent)
                   ("/q/server/in", [], [],  stringIn)
                   ("unknown",      [], [], stringOut)
                   (reg, 500000, (0,0,0)) onerr $ 
        withClient c "Cl1" "Desk1" 
                   ("/q/client1", [], [], ignorebody) 
                   (srv,          [], [],     nobody) $ \c1 -> do
          (sc,ps) <- requestProvider c1 500000 "Service1" 1
          case sc of
            OK -> withClient c "Cl2" "Service1" 
                              ("/q/client2", [], [],  stringIn)
                              (head ps,      [], [], stringOut) $ \c2 -> do
                    mbM <- request c2 500000 nullType [] "hello world"
                    case mbM of
                      Nothing -> putStrLn "Nothing received" 
                      _       -> return ()
            _  -> putStrLn "No provider!"

  ------------------------------------------------------------------------
  -- Publisher
  ------------------------------------------------------------------------
  pubTest :: Con -> QName -> String -> IO ()
  pubTest c q m = 
    withSub c "Sub1" "Topic1" q 500000
             ("/q/sub/1", [], [], stringIn) $ \s -> do
      mbI <- checkIssue s 100000
      case mbI of
        Nothing -> error "No issue!"
        Just i  -> when (msgContent i /= m) $ 
                        error $ "Wrong message: " ++ msgContent i

  ------------------------------------------------------------------------
  -- Subscriber
  ------------------------------------------------------------------------
  subTest :: SubA String -> String -> IO ()
  subTest s m = do
      mbI <- checkIssue s 100000
      case mbI of
        Nothing -> error "No issue!"
        Just i  -> when (msgContent i /= m) $ 
                        error $ "Wrong message: " ++ msgContent i

  ------------------------------------------------------------------------
  -- Server
  ------------------------------------------------------------------------
  srvTest :: Con -> QName -> String -> IO ()
  srvTest c q m = 
    withClient c "Client1" "Service1" 
                ("/q/client/in", [], [], stringIn)
                (q,              [], [], stringOut) $ \cl -> do
      mbM <- request cl 500000 nullType [] m
      case mbM of
        Nothing -> error "No response!"
        Just r  -> when (msgContent r /= reverse m) $
                        error $ "Wrong message: " ++ msgContent r

  ------------------------------------------------------------------------
  -- Client
  ------------------------------------------------------------------------
  cliTest :: ClientA String String -> String -> IO ()
  cliTest cl m = do
      mbM <- request cl 500000 nullType [] m
      case mbM of
        Nothing -> error "No response!"
        Just r  -> when (msgContent r /= reverse m) $
                        error $ "Wrong message: " ++ msgContent r

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

  withCon :: String -> IO ()
  withCon t = do
    putStrLn "========================================="
    putStrLn "   Stompl Patterns Library Test Suite"
    putStrLn "            Memory Test "
    putStrLn "========================================="

    withConnection "localhost" 61613 [] [] $ \c ->
        let p = case t of
                  "desk" -> withDesk c "Desk1" "/q/desk1/reg" (0,1000) 
                              onerr "/q/desk1/service" $ 
                                deskReq c "/q/desk1/reg" "/q/desk1/service"
                  "pub"  -> withPubThread c "Pub1" "Topic1" "/q/pub/topic1"
                                nullType [] (return "hello world!")
                                ("unknown", [], [], stringOut)
                                50000 onerr $ 
                                  pubTest c "/q/pub/topic1" "hello world!"
                  "sub"  -> withPubThread c "Pub1" "Topic1" "/q/pub/topic1"
                                nullType [] (return "hello world!")
                                ("unknown", [], [], stringOut)
                                50000 onerr $ 
                              withSub c "Sub1" "Topic1" "/q/pub/topic1" 500000
                                       ("/q/sub/1", [], [], stringIn) $ \s -> 
                                  subTest s "hello world!"
                  "srv"  -> withServerThread c "Srv1" "Service1" nullType []
                                               (return . reverse . msgContent)
                                               ("/q/srv/in", [], [], stringIn)
                                               ("unknown",   [], [], stringOut) 
                                               ("", 0,(0,0,0))
                                               onerr $ 
                              srvTest c "/q/srv/in" "hello world!"
                  "cli"  -> withServerThread c "Srv1" "Service1" nullType []
                                               (return . reverse . msgContent)
                                               ("/q/srv/in", [], [], stringIn)
                                               ("unknown",   [], [], stringOut) 
                                               ("", 0,(0,0,0))
                                               onerr $ 
                              withClient c "Client1" "Service1" 
                                          ("/q/client/in", [], [], stringIn)
                                          ("/q/srv/in",    [], [], stringOut) $ \cl -> 
                                cliTest cl "hello world!"
                  _      -> error $ "I don't know that test: " ++ t
         in runX t 1000 p
         
  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [t] -> withCon t
      _   -> putStrLn "I need a test name and nothing else!"
             >> exitFailure
