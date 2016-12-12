module Main 
where

  import Network.Mom.Stompl.Parser
  import Network.Mom.Stompl.Frame

  import Test.QuickCheck

  import qualified Data.ByteString      as B 
  import qualified Data.ByteString.UTF8 as U 

  import Data.Char (toUpper)
  import Data.List (foldl')

  import System.FilePath ((</>))
  import System.Exit (exitSuccess, exitFailure)
  import System.Environment (getArgs)

  import Control.Monad.Writer
  import Control.Applicative ((<$>))

  import Codec.MIME.Type (showType)
  import qualified Data.Text as T (unpack)

  main :: IO ()
  main = do 
    os <- getArgs
    case os of
      [typ, dir] -> do
        let ts = if (map toUpper) typ == "ALL"
                   then mkTestDir
                   else select (read typ) mkTestDir
        allTests dir ts
      _          -> do
        putStrLn "Give me: "
        putStrLn " => The type of message to test (or ALL)"
        putStrLn " => and the directory where I can find the test messages"
        exitFailure

  -------------------------------------------------------------------------
  -- Random Tests
  -------------------------------------------------------------------------
  instance Arbitrary Frame where
    arbitrary = do
      t <- elements [Connect, Stomp, Connected, Disconnect,
                     Subscribe, Unsubscribe, 
                     Send, Message, 
                     Begin, Commit, Abort,
                     Ack, Nack,
                     Receipt,  Error, 
                     HeartBeat] 
      mst <- concat <$> (mapM (mkHdr Must) $ getHdrs t Must)
      may <- concat <$> (mapM (mkHdr May ) $ getHdrs t May)
      mk  <- getMk t
      case mk (mst ++ may) of 
        Left  e -> error e
        Right f -> return f

  -- get a "frame maker" -------------------------------------------------
  getMk :: FrameType -> Gen ([Header] -> Either String Frame)
  getMk t = 
      case t of
        Connect -> return (mkConFrame . cleanHdrs)    -- remove special chars
        Stomp   -> return mkStmpFrame 
        Connected -> return (mkCondFrame . cleanHdrs) -- connect and connected
                                                      -- are not escaped!
        Disconnect -> return mkDisFrame
        Subscribe -> return mkSubFrame 
        Unsubscribe -> return mkUSubFrame
        Begin -> return mkBgnFrame
        Commit -> return mkCmtFrame
        Abort -> return mkAbrtFrame
        Ack -> return mkAckFrame
        Nack -> return mkNackFrame
        Receipt -> return mkRecFrame
        HeartBeat -> return (\_ -> Right mkBeat)
        Send -> do
          s <- arbitrary
          let b = U.fromString s
          let l = let l' = B.length  b 
                   in if l' == 0 then (-1) else l'
          return (\hs -> mkSndFrame hs l b)
        Message -> do
          s <- arbitrary 
          let b = U.fromString s
          let l = let l' = B.length  b 
                   in if l' == 0 then (-1) else l'
          return (\hs -> mkMsgFrame hs l b)
        Error -> do
          s <- arbitrary 
          let b = U.fromString s
          let l = let l' = B.length  b 
                   in if l' == 0 then (-1) else l'
          return (\hs -> mkErrFrame hs l b)

  -- random headers -------------------------------------------------
  mkHdr :: Must -> String -> Gen [Header]
  mkHdr m h = do
    dice <- case m of 
              Must -> return (1::Int) 
              May  -> choose (1,3)
    if dice == 1
      then case h of
             "heart-beat"     -> return [(h, "500,500")]
             "server"         -> return [(h, "name/1.1")]
             "version"        -> return [(h, "1.1")]
             "accept-version" -> return [(h, "1.0,1.1")]
             "ack"            -> 
               elements [Auto, Client, ClientIndi] >>= (\a -> return [(h, show a)])
             "content-type"   -> return [(h, "text/plain")]
             _                -> 
               hdrVal >>= (\x -> return [(h, x)])
      else return []

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
                      "!\"$%&/()=?<>#§:\n\r\t\\")

  cleanHdrs :: [Header] -> [Header]
  cleanHdrs = map cleanHdr
    where cleanHdr (h,v) = (remove h, remove v)
          remove = foldl' (\l -> (++) l . rm) []
          rm c | c `elem` ":\n\r\\" = []
               | otherwise          = [c] 

  -- mandatory or optional --------------------------------------------
  data Must = Must | May
  must :: Must -> Bool
  must Must = True
  must May  = False

  -- mandatory and optional headers -------------------------------------
  getHdrs :: FrameType -> Must -> [String]
  getHdrs t m =
    case t of
       Connect     -> 
         if must m then ["host", "accept-version"]
           else ["login", "passcode", "client-id", "heart-beat"] 
       Stomp       -> 
         if must m then ["host", "accept-version"]
           else ["login", "passcode", "client-id", "heart-beat"] 
       Connected   ->
         if must m then ["version"]
           else ["session", "server", "heart-beat"]
       Disconnect  ->
         if must m then [] else ["receipt"]
       Subscribe   ->
         if must m then ["id", "destination"]
           else ["ack", "receipt"]
       Unsubscribe ->
         if must m then ["id"] else ["destination"]
       Send        ->
         if must m then ["destination"]
           else ["content-type", 
                 "transaction", "receipt", "content-length"]
       Message     ->
         if must m then ["message-id", "subscription", "destination"]
           else ["ack", "content-type", "content-length"]
       Begin       ->
         if must m then ["transaction"] else ["receipt"]
       Commit      ->
         if must m then ["transaction"] else ["receipt"]
       Abort       ->
         if must m then ["transaction"] else ["receipt"]
       Ack         ->
         if must m then ["id"]
           else ["message-id", "subscription", "transaction", "receipt"]
       Nack        ->
         if must m then ["id", "subscription"]
           else ["message-id", "subscription", "transaction", "receipt"]
       Error       ->
         if must m then [] 
           else ["message", "receipt-id", 
                 "content-type", "content-length"]
       Receipt     -> if must m then ["receipt-id"] else []
       HeartBeat   -> []

  -- put, parse and compare equal -------------------------------------------
  prp_Parse :: Frame -> Property
  prp_Parse f = collect (typeOf f) $ 
    case stompAtOnce $ putFrame f of
      Left  _ -> False
      Right x -> if x == f then True -- x == f
                   else error $ "Not equal:\n" ++ show x ++ "\n" ++ show f

  -- repeated headers -------------------------------------------------------
  prp_RepeatedHdrs :: [Header] -> Bool
  prp_RepeatedHdrs hs = 
    let b   = U.fromString "hello"
        l   = B.length b
        hs' = ("destination", "/q/test"):hs ++ 
              [("message-id", "msg-1"), ("test", "true"), 
               ("destination", "/q/false"), ("xyz", "-")]
     in case mkMsgFrame hs' l b of
          Left  e -> error $ "Can't create frame: " ++ e
          Right f -> if getDest f == "/q/test" then True else False 

  -- Check -------------------------------------------------------------------
  deepCheck :: (Testable p) => p -> IO Result
  deepCheck = quickCheckWithResult stdArgs{maxSuccess=10000,
                                           maxDiscardRatio=10}

  -------------------------------------------------------------------------
  -- Controlled Tests
  -------------------------------------------------------------------------
  type Test     = (TestDesc, FilePath)
  type Msg      = String
  data TestDesc = TDesc {
                    dscDesc  :: String,               -- verbal description
                    dscType  :: FrameType,            -- expected frame type
                    dscTrans :: Frame -> Maybe Frame, -- transformation
                    dscRes   :: TestResult,           -- expected result
                    dscHdrs  :: [(String, String)]}   -- expected headers

  data TestResult = Fail | Pass
    deriving (Eq, Show, Read)

  myBeat :: Heart
  myBeat = (500,500)

  -- filter tests --------------------------------------------------------
  select :: FrameType -> [Test] -> [Test]
  select t = filter (hasType t)

  hasType :: FrameType -> Test -> Bool
  hasType t c = (t == (dscType . fst) c) 

  ------------------------------------------------------------------------
  -- The test battery
  ------------------------------------------------------------------------
  mkTestDir :: [Test]
  mkTestDir = 
    [(TDesc "HeartBeat"
            HeartBeat (Just . id) Pass
            [], "beat.txt"),
     (TDesc "Simple connect" 
            Connect (Just . id) Pass 
            [("login", "guest"),
             ("passcode", "guest")], "con.txt"),
     (TDesc "Connect 1.1" 
            Connect (Just . id) Pass 
            [("accept-version", "1.0,1.1"),
             ("login", "guest"),
             ("passcode", "guest"),
             ("heart-beat", "50,1000"),
             ("host", " Test-1")], "con-1.1.txt"),
     (TDesc "Connected 1.1" 
            Connected (Just . id) Pass 
            [("version", "1.1"),
             ("heart-beat", "500,500")], "cond-1.1.txt"),
     (TDesc "Connect 1.1 to Connected 1.1" 
            Connected (conToCond "test/0.1" "1" myBeat [(1,1)]) Pass 
            [("version", "1.1"),
             ("server", "test/0.1"),
             ("heart-beat", "500,1000"),
             ("session", "1")], "con-1.1.txt"),
     (TDesc "Connect 1.1 to Connected 1.1 with 0 Client Send" 
            Connected (conToCond "test/0.1" "1" myBeat [(1,1)]) Pass 
            [("version", "1.1"),
             ("server", "test/0.1"),
             ("heart-beat", "0,1000"),
             ("session", "1")], "con2-1.1.txt"),
     (TDesc "Connect 1.1 to Connected Version 1.0" 
            Connected (conToCond "test/0.1" "1" myBeat [(1,0)]) Pass 
            [("version", "1.0"),
             ("server", "test/0.1"),
             ("heart-beat", "0,1000"),
             ("session", "1")], "con2-1.1.txt"),
     (TDesc "Connect 1.1 to Connected multiple Versions" 
            Connected (conToCond "test/0.1" "1" myBeat [(1,0), (1,1)]) Pass 
            [("version", "1.1"),
             ("server", "test/0.1"),
             ("heart-beat", "0,1000"),
             ("session", "1")], "con2-1.1.txt"),
     (TDesc "Connect 1.1 to Connected 2 times multiple Versions" 
            Connected (conToCond "test/0.1" "1" myBeat [(1,0), (1,1)]) Pass 
            [("version", "1.0"),
             ("server", "test/0.1"),
             ("heart-beat", "0,1000"),
             ("session", "1")], "con4-1.1.txt"),
     (TDesc "Connect 1.1 to Connected no Version" 
            Connected (conToCond "test/0.1" "1" myBeat []) Pass 
            [("version", "1.0"),
             ("server", "test/0.1"),
             ("heart-beat", "0,1000"),
             ("session", "1")], "con2-1.1.txt"),
     (TDesc "Connect 1.1 to Connected 1.1 without heart-beat" 
            Connected (conToCond "test/0.1" "1" myBeat [(1,1)]) Pass 
            [("version", "1.1"),
             ("server", "test/0.1"),
             ("heart-beat", "0,0"),
             ("session", "1")], "con3-1.1.txt"),
     (TDesc "Simple begin" 
            Begin (Just . id) Pass 
            [("transaction", "trn-12345")], "begin.txt"),
     (TDesc "Simple commit" 
            Commit (Just . id) Pass 
            [("transaction", "trn-12345")], "commit.txt"),
     (TDesc "commit without transaction" 
            Commit (Just . id) Fail
            [("content-length", "12345")], "commit2.txt"),
     (TDesc "Simple abort" 
            Abort (Just . id) Pass 
            [("transaction", "trn-12345")], "abort.txt"),
     (TDesc "Ack" 
            Ack (Just . id) Pass 
            [("message-id", "1234"),
             ("transaction", "trn-12345")], "ack.txt"),
     (TDesc "Ack 1.1" 
            Ack (Just . id) Pass 
            [("subscription", "sub-1"),
             ("message-id", "1234"),
             ("transaction", "trn-12345")], "ack1-1.1.txt"),
     (TDesc "Ack 1.2" 
            Ack (Just . id) Pass 
            [("id", "1234"),
             ("transaction", "trn-12345")], "ack1-1.2.txt"),
     (TDesc "Nack 1.1" 
            Nack (Just . id) Pass 
            [("subscription", "sub-1"),
             ("message-id", "1234"),
             ("transaction", "trn-12345")], "nack1-1.1.txt"),
     (TDesc "Nack 1.2" 
            Nack (Just . id) Pass 
            [("id", "1234"),
             ("transaction", "trn-12345")], "nack1-1.2.txt"),
     (TDesc "Simple Subscription (1.0)" 
            Subscribe (Just . id) Pass 
            [("destination", "/queue/test"),
             ("ack", "client")], "sub1.txt"),
     (TDesc "Subscription 1.1 without id" 
            Subscribe (Just . id) Pass -- Fail
            [("destination", "/queue/test"),
             ("ack", "client")], "sub2-1.1.txt"),
     (TDesc "Subscription 1.1 auto mode" 
            Subscribe (Just . id) Pass
            [("id", "10"),
             ("destination", "/queue/test"),
             ("ack", "auto")], "sub1-1.1.txt"),
     (TDesc "Subscription 1.1 client mode" 
            Subscribe (Just . id) Pass
            [("id", "10"),
             ("destination", "/queue/test"),
             ("ack", "client")], "sub3-1.1.txt"),
     (TDesc "Subscription 1.1 client-individual mode" 
            Subscribe (Just . id) Pass
            [("id", "10"),
             ("destination", "/queue/test"),
             ("ack", "client-individual")], "sub4-1.1.txt"),
     (TDesc "send with content-length and receipt" 
            Send (Just . id) Pass 
            [("destination", "/queue/test"),
             ("content-length", "13"),
             ("content-type", "text/plain"),
             ("receipt", "msg-123")], "send1-1.1.txt"),
     (TDesc "send with duplicated destination header" 
            Send (Just . id) Pass 
            [("destination", "/queue/test"),
             ("content-length", "13"),
             ("content-type", "text/plain"),
             ("receipt", "msg-123")], "send2-1.1.txt"),
     (TDesc "send with content-length and many NULs" 
            Send (Just . id) Pass 
            [("destination", "/queue/test"),
             ("content-length", "23")], "send2.txt"),
     (TDesc "send with escaped headers" 
            Send (Just . id) Pass 
            [("destination", "/queue/test"),
             ("content-length", "23"),
             ("complex:header", "and \\ comp\rlex\nvalue")], "send2-esc.txt"), 
     (TDesc "send with dos-style line endings" 
            Send (Just . id) Pass 
            [("destination", "/queue/test"),
             ("content-length", "24"),
             ("complex:header", "and \\ complex\nvalue")], "send2-dos.txt"),
     (TDesc "send witout content-length" 
            Send (Just . id) Pass 
            [("destination", "/queue/test")], 
             "send3.txt"),
     (TDesc "Send missing NUL" 
            Send (Just . id) Fail 
            [("destination", "/queue/test")], "send4.txt"),
     (TDesc "Empty send" 
            Send (Just . id) Pass 
            [("destination", "/queue/test")], "send5.txt"),
     (TDesc "send with spaces at beginning of body" 
            Send (Just . id) Pass 
            [("destination", "/queue/test"),
             ("content-length", "16"),
             ("content-type", "text/plain"),
             ("receipt", "msg-123")], "send6-1.1.txt"),
     (TDesc "send with special headers" 
            Send (Just . id) Pass 
            [("destination", "/queue/test"),
             ("content-length", "16"),
             ("content-type", "text/plain"),
             ("receipt", "msg-123"),
             ("special1", "xyz"),
             ("special2", "123")], "send7-1.1.txt"),
     (TDesc "UTF" 
            Send (Just . id) Pass 
            [("destination", "/queue/test"),
             ("content-length", "81"),
             ("content-type", "text/plain"),
             ("receipt", "msg-123")], "send-jap.txt"),
     (TDesc "send to message" 
            Message (sndToMsg "msg-1" "sub-1" "ack-1") Pass 
            [("destination", "/queue/test"),
             ("content-length", "13"),
             ("content-type", "text/plain"),
             ("subscription", "sub-1")], "send1-1.1.txt"),
     (TDesc "Message 1.1" 
            Message (Just . id) Pass
            [("destination", "/queue/test"),
             ("message-id", "msg-§54321"),
             ("content-length", "13"),
             ("content-type", "text/plain")], "msg1-1.1.txt"),
     (TDesc "Message with special headers" 
            Message (Just . id) Pass
            [("destination", "/queue/test"),
             ("message-id", "msg-54321"),
             ("content-length", "13"),
             ("content-type", "text/plain"),
             ("special1", "\t1"),
             ("special2", "123")], "msg7-1.1.txt"),
     (TDesc "Message with some NULs in body" 
            Message (Just . id) Pass
            [("destination", "/queue/test"),
             ("message-id", "msg-54321"),
             ("content-length", "23")], "msg2.txt"),
     (TDesc "Message without content-length" 
            Message (Just . id) Pass
            [("destination", "/queue/test"),
             ("message-id", "msg-54321")], "msg3.txt"),
     (TDesc "Message missing NUL" 
            Message (Just . id) Fail
            [("destination", "/queue/test"),
             ("message-id", "msg-54321")], "msg4.txt"),
     (TDesc "Message with wrong content-length" 
            Message (Just . id) Fail
            [("destination", "/queue/test"),
             ("message-id", "msg-54321"),
             ("content-length", "22")], "msg5.txt"),
     (TDesc "Message with huge body" 
            Message (Just . id) Pass
            [("destination", "/queue/test"),
             ("message-id", "msg-54321"),
             ("content-length", "1553418")], "msg6.txt"),
     (TDesc "Error 1.1 without content-length" 
            Error (Just . id) Pass
            [("message", "Malformed package received"),
             ("content-type", "text/plain")], "err1-1.1.txt")
    ]

  -- compare frame type --------------------------------------------------
  frmOk :: FrameType -> TestDesc -> Bool
  frmOk f d = f == dscType d

  -- compare header ------------------------------------------------------
  headerOk :: String -> Frame -> TestDesc -> Bool
  headerOk k f d = 
    if acc f == value then True else False
    where acc   = getAccess k
          value = case lookup k $ dscHdrs d of
                    Nothing -> ""
                    Just v  -> v

  -- get header value ------------------------------------------------------
  getValue :: String -> Frame -> String
  getValue s f = acc f
    where acc = getAccess s

  -- get access to frame ------------------------------------------------------
  getAccess :: String -> (Frame -> String)
  getAccess k =  
    case k of
      "login"          -> getLogin
      "passcode"       -> getPasscode
      "destination"    -> getDest
      "subscription"   -> getSub
      "content-length" -> show . getLength
      "content-type"   -> T.unpack . showType . getMime
      "transaction"    -> getTrans
      "id"             -> getId
      "ack"            -> ackToVal . getAcknow
      "message-id"     -> getId
      "message"        -> getMsg
      "receipt"        -> getReceipt
      "heart-beat"     -> beatToVal . getBeat
      "accept-version" -> versToVal . getVersions
      "version"        -> verToVal  . getVersion
      "session"        -> getSession
      "server"         -> srvToStr  . getServer
      "host"           -> getHost
      "special1"       -> getSpecial "special1"
      "special2"       -> getSpecial "special2"
      " special3"      -> getSpecial " special3"
      "complex:header" -> getSpecial "complex:header"
      _                -> (\_ -> "unknown")
    where getSpecial k' f = case lookup k' $ getHeaders f of
                              Nothing -> ""
                              Just v  -> v

  -- Test writer ------------------------------------------------------
  type Tester a = Writer String a

  -- test parse and transform ------------------------------------------------
  testParse :: String -> TestDesc -> B.ByteString -> Tester (Either Bool Frame)
  testParse _ d m = do
    let good = "Parse successful."
    let bad  = "Parse failed"
    case stompAtOnce m of
      Left  e -> case dscRes d of
                   Fail -> do
                     tell $ bad ++ " " ++ {- "(" ++ (U.toString m) ++ "): " ++ -} e ++ "\n"
                     return $ Left True
                   Pass -> do
                     tell $ bad ++ " " ++ {- "(" ++ (U.toString m) ++ "): " ++ -} e ++ "\n"
                     return $ Left False
      Right f -> 
        case trans f of
          Nothing -> do
            tell $ "Transformation failed.\n"
            return $ Left False
          Just f' -> 
            case dscRes d of
              Fail -> do
                tell $ good ++ "\n"
                return $ Left False
              Pass -> do
                tell $ good ++ "\n"
                return $ Right f'

    where trans = dscTrans d

  -- test frame type verbosely ------------------------------------------------
  testFrame :: Frame -> TestDesc -> Tester Bool
  testFrame f d = do
    let t = typeOf f
    if frmOk t d
      then do
        tell $ "Frame Type " ++ (show t) ++ " correct.\n"
        return True
      else do
        tell $ "Wrong Frame Type: " ++ (show t) ++ ".\n"
        return False

  -- test header verbosely ------------------------------------------------
  testHeader :: Frame -> TestDesc -> String -> Tester Bool
  testHeader f d h = do
    if headerOk h f d 
      then do
        tell $ "Header '" ++ h ++ "' is correct.\n"
        return True
      else do
        tell $ "Header '" ++ h ++ "' is not correct: '" ++ (getValue h f) ++ " - " ++ show (dscHdrs d) ++ "'\n"
        return False

  -- test headers verbosely ------------------------------------------------
  testHeaders :: Frame -> TestDesc -> Tester Bool
  testHeaders f d = do
    oks <- mapM (testHeader f d) $ map fst $ dscHdrs d
    return $ and oks

  -- complete test ---------------------------------------------------------
  applyTests :: String -> TestDesc -> B.ByteString -> Tester Bool
  applyTests n d m = do
    let good = "Test '" ++ n ++ "' passed.\n"
    let bad  = "Test '" ++ n ++ "' failed.\n"
    mbF <- testParse n d m
    case mbF of
      Left True  -> do
        tell good
        return True
      Left False -> do
        tell bad
        return False
      Right f    -> do
        ok <- testFrame f d ~> testHeaders f d
        if ok then tell good else tell bad
        return ok

  -- combine test steps ---------------------------------------------------------
  applyB :: Tester Bool -> Tester Bool -> Tester Bool
  applyB f g = f >>= \ok ->
    if ok then g else return False

  infix ~> 
  (~>) :: Tester Bool -> Tester Bool -> Tester Bool
  (~>) = applyB

  -- read sample and execute test ------------------------------------------------
  execTest :: FilePath -> Test -> IO Bool
  execTest p t = do 
    let f = snd t
    let d = fst t
    putStrLn $ "Test: " ++ (dscDesc $ fst t)
    m <- B.readFile (p </> f) 
    let (r, txt) = runWriter (applyTests f d m)
    putStrLn txt
    putStrLn ""
    return r

  ----------------------------------------------------------------------------------
  -- execute test battery
  ----------------------------------------------------------------------------------
  foldTests :: (Test -> IO Bool) -> [Test] -> IO Bool
  foldTests _ [] = return True
  foldTests f (t:ts) = do
    b <- f t
    if b then foldTests f ts else return False

  evalTests :: FilePath -> [Test] -> IO Bool
  evalTests p ts = do
    verdict <- foldTests (execTest p) ts -- mapM (execTest p) ts
    if verdict
      then 
        putStrLn "OK. All Tests passed"
      else do
        putStrLn "Bad. Not all tests passed"
    return verdict

  ----------------------------------------------------------------------------------
  -- execute test battery and deepCheck
  ----------------------------------------------------------------------------------
  allTests :: FilePath -> [Test] -> IO ()
  allTests p ts = do
    v <- evalTests p ts
    if not v then exitFailure
      else do
        r <- deepCheck prp_RepeatedHdrs 
          ?> deepCheck prp_Parse 
        case r of
          Success _ _ _ -> exitSuccess
          _             -> do putStrLn "Bad. Some tests failed"
                              exitFailure

  infixr ?>
  (?>) :: IO Result -> IO Result -> IO Result
  r1 ?> r2 = r1 >>= \r -> case r of
                            Success{} -> r2
                            _         -> return r
