module Main 
where

  import Types

  import Network.Mom.Stompl.Parser
  import Network.Mom.Stompl.Frame

  import qualified Data.ByteString      as B (readFile, ByteString) 
  import qualified Data.ByteString.UTF8 as U 

  import Data.Char (toUpper)

  import System.FilePath (FilePath, (</>))
  import System.Exit (exitSuccess, exitFailure)
  import System.Environment (getArgs)

  import Control.Monad.Writer
  import Control.Applicative ((<$>))

  import Codec.MIME.Type (showType)

  type Test     = (TestDesc, FilePath)
  type Msg      = String
  data TestDesc = TDesc {
                    dscDesc  :: String,
                    dscType  :: FrameType,
                    dscTrans :: Frame -> Maybe Frame,
                    dscRes   :: TestResult,
                    dscHdrs  :: [(String, String)]}
    -- deriving (Eq, Show, Read)

  data TestResult = Fail | Pass
    deriving (Eq, Show, Read)

  select :: FrameType -> [Test] -> [Test]
  select t = filter (hasType t)

  hasType :: FrameType -> Test -> Bool
  hasType t c = (t == (dscType . fst) c) 

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
             ("host", "Test-1")], "con-1.1.txt"),
     (TDesc "Connected 1.1" 
            Connected (Just . id) Pass 
            [("version", "1.1"),
             ("heart-beat", "500,500")], "cond-1.1.txt"),
     (TDesc "Connect 1.1 to Connected 1.1" 
            Connected (conToCond "test/0.1" "1" myBeat) Pass 
            [("version", "1.1"),
             ("server", "test/0.1"),
             ("heart-beat", "500,1000"),
             ("session", "1")], "con-1.1.txt"),
     (TDesc "Connect 1.1 to Connected 1.1 with 0 Client Send" 
            Connected (conToCond "test/0.1" "1" myBeat) Pass 
            [("version", "1.1"),
             ("server", "test/0.1"),
             ("heart-beat", "0,1000"),
             ("session", "1")], "con2-1.1.txt"),
     (TDesc "Connect 1.1 to Connected 1.1 without heart-beat" 
            Connected (conToCond "test/0.1" "1" myBeat) Pass 
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
     (TDesc "Nack 1.1" 
            Nack (Just . id) Pass 
            [("subscription", "sub-1"),
             ("message-id", "1234"),
             ("transaction", "trn-12345")], "nack1-1.1.txt"),
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
     (TDesc "Message 1.1" 
            Message (Just . id) Pass
            [("destination", "/queue/test"),
             ("message-id", "msg-54321"),
             ("content-length", "13"),
             ("content-type", "text/plain")], "msg1-1.1.txt"),
     (TDesc "Message with special headers" 
            Message (Just . id) Pass
            [("destination", "/queue/test"),
             ("message-id", "msg-54321"),
             ("content-length", "13"),
             ("content-type", "text/plain"),
             ("special1", "xyz"),
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
     (TDesc "Error 1.1 without content-length" 
            Error (Just . id) Pass
            [("message", "Malformed package received"),
             ("content-type", "text/plain")], "err1-1.1.txt")
    ]

  frmOk :: FrameType -> TestDesc -> Bool
  frmOk f d = f == dscType d

  headerOk :: String -> Frame -> TestDesc -> Bool
  headerOk k f d = 
    if acc f == value then True else False
    where acc   = getAccess k
          value = case lookup k $ dscHdrs d of
                    Nothing -> ""
                    Just v  -> v

  getValue :: String -> Frame -> String
  getValue s f = acc f
    where acc = getAccess s

  getAccess :: String -> (Frame -> String)
  getAccess k =  
    case k of
      "login"          -> getLogin
      "passcode"       -> getPasscode
      "destination"    -> getDest
      "subscription"   -> getSub
      "content-length" -> show . getLength
      "content-type"   -> showType . getMime
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
      _                -> (\_ -> "unknown")
    where getSpecial k f = case lookup k $ getHeaders f of
                             Nothing -> ""
                             Just v  -> v

  type Tester a = Writer String a

  testParse :: String -> TestDesc -> B.ByteString -> Tester (Either Bool Frame)
  testParse n d m = do
    let good = "Parse successfull."
    let bad  = "Parse failed"
    case stompAtOnce m of
      Left  e -> case dscRes d of
                   Fail -> do
                     tell $ bad ++ "(" ++ (U.toString m) ++ "): " ++ e ++ "\n"
                     return $ Left True
                   Pass -> do
                     tell $ bad ++ "(" ++ (U.toString m) ++ "): " ++ e ++ "\n"
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

  testHeader :: Frame -> TestDesc -> String -> Tester Bool
  testHeader f d h = do
    if headerOk h f d 
      then do
        tell $ "Header '" ++ h ++ "' is correct.\n"
        return True
      else do
        tell $ "Header '" ++ h ++ "' is not correct: '" ++ (getValue h f) ++ "'\n"
        return False

  testHeaders :: Frame -> TestDesc -> Tester Bool
  testHeaders f d = do
    oks <- mapM (testHeader f d) $ map fst $ dscHdrs d
    return $ and oks

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
        ok <- testFrame f d ?> testHeaders f d
        if ok then tell good else tell bad
        return ok

  applyB :: Tester Bool -> Tester Bool -> Tester Bool
  applyB f g = f >>= \ok ->
    if ok then g else return False

  infix ?> 
  (?>) :: Tester Bool -> Tester Bool -> Tester Bool
  (?>) = applyB

  execTest :: FilePath -> Test -> IO Bool
  execTest p t = do 
    let f = snd t
    let d = fst t
    putStrLn $ "Test: " ++ (dscDesc $ fst t)
    m <- B.readFile (p </> f) 
    -- m <- U.fromString <$> readFile (p </> f) 
    let (r, txt) = runWriter (applyTests f d m)
    putStrLn txt
    putStrLn ""
    return r
    -- testType  f d m

  foldTests :: (Test -> IO Bool) -> [Test] -> IO Bool
  foldTests _ [] = return True
  foldTests f (t:ts) = do
    b <- f t
    if b then foldTests f ts else return False

  evalTests :: FilePath -> [Test] -> IO ()
  evalTests p ts = do
    verdict <- foldTests (execTest p) ts -- mapM (execTest p) ts
    if verdict
      then do
        putStrLn "OK. All Tests passed"
        exitSuccess
      else do
        putStrLn "Bad. Not all tests passed"
        exitFailure

  main :: IO ()
  main = do 
    os <- getArgs
    case os of
      [typ, dir] -> do
        let ts = if (map toUpper) typ == "ALL"
                   then mkTestDir
                   else select (read typ) mkTestDir
        evalTests dir ts
      _          -> do
        putStrLn "Give me: "
        putStrLn " => The type of message to test (or ALL)"
        putStrLn " => and the directory where I can find the test messages"
        exitFailure
