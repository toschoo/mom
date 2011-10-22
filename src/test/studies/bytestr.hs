module Main
where

  import           Test.QuickCheck
  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           System.Exit

  import qualified Data.Attoparsec as A

  deepCheck :: (Testable p) => p -> IO Result
  deepCheck = quickCheckWithResult stdArgs{maxSuccess=1000,
                                           maxDiscard=5000}

  prpPackEq :: String -> Property 
  prpPackEq s = (length s > 10) ==> (U.toString $ U.fromString s) == s

  applyTest :: IO Result -> IO Result -> IO Result
  applyTest r f = do
    r' <- r
    case r' of
      Success _ -> f
      x         -> return x

  infixr ?>
  (?>) :: IO Result -> IO Result -> IO Result
  (?>) = applyTest

  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    r <- (deepCheck prpPackEq) 
    case r of
      Success _ -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  infixr >|<, |>, <| 
  (>|<) :: B.ByteString -> B.ByteString -> B.ByteString
  (|>)  :: B.ByteString ->   Char       -> B.ByteString
  (<|)  ::   Char       -> B.ByteString -> B.ByteString
  x >|< y = x `B.append` y
  x <|  y = x `B.cons` y
  x  |> y = x `B.snoc` y

  body :: Int -> A.Parser B.ByteString
  body x = body' x B.empty
    where 
      body' l i = do
        n <- A.takeTill (== nul)
        let b = i >|< n 
        if l < 0 || l == B.length b
          then do
            _ <- A.word8 nul
            return b
          else 
            if l < B.length b 
              then fail $ "Body longer than indicated: " ++ (show l) ++ " / " ++ (show $ B.length b)
              else do
                _ <- A.word8 nul
                body' l (b |> '\x00')
  nul = 0

  main :: IO ()
  main = do 
    -- checkAll
    let s = "   hello world!\n"
    putStrLn $ "Length: " ++ (show $ length s)
    -- let b  = U.fromString  s
    let b  = B.pack s
    -- putStrLn $ "Length Bytestring: " ++ (show $ B.length b')
    putStrLn $ U.toString b
    putStrLn $ "Equal: " ++ (show $ (U.toString b) == s)
    putStrLn $ "Equal: " ++ (show $ (U.length b) == length s)
    case A.parseOnly (body 16) (b  |> '\x00') of
      Left  e -> putStrLn $ "Failed: "  ++ e
      Right r -> putStrLn $ "Success: " ++ (U.toString r)
    j <- B.readFile "test/samples/jap.txt"
    putStrLn $ "Japanese length: " ++ (show $ B.length j)


