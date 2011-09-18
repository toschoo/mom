module Main 
where

  import Network.Mom.Stompl.Parser
  import Network.Mom.Stompl.Frame

  import qualified Data.ByteString      as B (readFile, ByteString) 
  import qualified Data.ByteString.UTF8 as U (toString)

  import Data.Char (toUpper)

  import System.FilePath (FilePath, (</>))
  import System.Exit (exitSuccess, exitFailure)
  import System.Environment (getArgs)

  import Control.Monad.Writer

  type Test     = (TestDesc, FilePath)
  type Msg      = String
  data TestDesc = TDesc {
                    dscDesc  :: String,
                    dscType  :: FrameType,
                    dscRes   :: TestResult,
                    dscHdrs  :: [(String, String)]}
    deriving (Eq, Show, Read)

  data TestResult = Fail | Pass
    deriving (Eq, Show, Read)

  select :: FrameType -> [Test] -> [Test]
  select t = filter (hasType t)

  hasType :: FrameType -> Test -> Bool
  hasType t c = (t == (dscType . fst) c) 

  mkTestDir :: [Test]
  mkTestDir = 
    [(TDesc "Simple connect" 
            Connect Pass 
            [("login", "guest"),
             ("passcode", "guest")], "con.txt"),
     (TDesc "Simple begin" 
            Begin Pass 
            [("transaction", "trn-12345")], "begin.txt"),
     (TDesc "Simple commit" 
            Commit Pass 
            [("transaction", "trn-12345")], "commit.txt"),
     (TDesc "commit without transaction" 
            Commit Fail
            [("content-length", "12345")], "commit2.txt"),
     (TDesc "Simple abort" 
            Abort Pass 
            [("transaction", "trn-12345")], "abort.txt"),
     (TDesc "Ack" 
            Ack Pass 
            [("message-id", "1234"),
             ("transaction", "trn-12345")], "ack.txt"),
     (TDesc "send with content-length and receipt" 
            Send Pass 
            [("destination", "/queue/test"),
             ("receipt", "msg-123")], "send.txt"),
     (TDesc "send with content-length and many NULs" 
            Send Pass 
            [("destination", "/queue/test"),
             ("content-length", "23")], "send2.txt"),
     (TDesc "send witout content-length" 
            Send Pass 
            [("destination", "/queue/test")], 
             "send3.txt"),
     (TDesc "Send missing NUL" 
            Send Fail 
            [("destination", "/queue/test")], "send4.txt"),
     (TDesc "Empty send" 
            Send Pass 
            [("destination", "/queue/test")], "send5.txt"),
     (TDesc "Simple Message" 
            Message Pass
            [("destination", "/queue/test"),
             ("message-id", "msg-54321"),
             ("content-length", "13")], "msg.txt"),
     (TDesc "Message with some NULs in body" 
            Message Pass
            [("destination", "/queue/test"),
             ("message-id", "msg-54321"),
             ("content-length", "23")], "msg2.txt"),
     (TDesc "Message without content-length" 
            Message Pass
            [("destination", "/queue/test"),
             ("message-id", "msg-54321")], "msg3.txt"),
     (TDesc "Message missing NUL" 
            Message Fail
            [("destination", "/queue/test"),
             ("message-id", "msg-54321")], "msg4.txt"),
     (TDesc "Message with wrong content-length" 
            Message Fail
            [("destination", "/queue/test"),
             ("message-id", "msg-54321"),
             ("content-length", "22")], "msg5.txt"),
     (TDesc "Error without content-length" 
            Error Pass
            [("message", "Malformed package received")], "err1.txt")
    ]

  frmOk :: FrameType -> TestDesc -> Bool
  frmOk f d = f == dscType d

  headerOk :: String -> Frame -> TestDesc -> Bool
  headerOk k f d = 
    let acc= case k of
               "login"          -> getLogin
               "passcode"       -> getPasscode
               "destination"    -> getDest
               "content-length" -> show . getLength
               "transaction"    -> getTrans
               "id"             -> getId
               "message-id"     -> getId
               "message"        -> getMsg
               "receipt"        -> getReceipt
               _                -> (\_ -> "unknown")
    in if acc f == value then True else False
    where value = case lookup k $ dscHdrs d of
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
                     tell $ bad ++ ": " ++ e ++ "\n"
                     return $ Left True
                   Pass -> do
                     tell $ bad ++ ": " ++ e ++ "\n"
                     return $ Left False
      Right f -> case dscRes d of
                   Fail -> do
                     tell $ good ++ "\n"
                     return $ Left False
                   Pass -> do
                     tell $ good ++ "\n"
                     return $ Right f

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
        tell $ "Header '" ++ h ++ "' is not correct.\n"
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
    let (r, txt) = runWriter (applyTests f d m)
    putStrLn txt
    putStrLn ""
    return r
    -- testType  f d m

  evalTests :: FilePath -> [Test] -> IO ()
  evalTests p ts = do
    verdicts <- mapM (execTest p) ts
    if and verdicts 
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
