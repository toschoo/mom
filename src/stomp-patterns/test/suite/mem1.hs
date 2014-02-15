module Main
where

  import           System.Exit
  import           System.IO (stdout, hFlush)
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

  runX :: String -> Int -> IO () -> IO ()
  runX s n f = putStr ("Running Test " ++ s ++ ":    ") 
               >> hFlush stdout >> go n
    where go 0 = bubble 0 0 >> putStrLn ""
          go i = bubble i (i+1) >> f >> go (i-1)

  bubble :: Int -> Int -> IO ()
  bubble n o = 
    let x  = show o ++ " "
        b  = map (\_ -> '\b') x
        ch = b ++ show n
     in do putStr " "
           putStr ch >> hFlush stdout
                   
  withCon :: IO ()
  withCon = do
    putStrLn "========================================="
    putStrLn "   Stompl Patterns Library Test Suite"
    putStrLn "            Memory Test "
    putStrLn "========================================="

    withConnection "localhost" 61613 [] [] $ \c ->
      withDesk c "Desk1" "/q/desk1/reg" (0,1000) onerr "/q/desk1/service" $ 
        runX "Desk Request" 1000 $ deskReq c "/q/desk1/reg" "/q/desk1/service"
         
  main :: IO ()
  main = withCon
