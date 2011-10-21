module Main
where

  import Mime
  import Test.QuickCheck
  import Data.List (sort, nub, find)
  import Control.Applicative((<$>))
  import Text.Parsec (parse)

  import System.Exit
  import System.FilePath

  --------------------------------------------------------
  -- Value
  --------------------------------------------------------
  data ValStr = VStr String
    deriving (Eq, Read)

  instance Show ValStr where
    show (VStr s) = s

  data AtStr = AStr String
  
  instance Show AtStr where
    show (AStr s) = s

  data ParamStr = PStr String
    deriving (Eq, Read)

  instance Show ParamStr where
    show (PStr s) = s

  instance Arbitrary ValStr where
    arbitrary = VStr <$> buildStr 
      where buildStr = do
              dice <- choose (10, 100) :: Gen Int
              buildStr' dice
            buildStr' n = do
              die <- choose (1,6)    :: Gen Int
              let es = if die == 1
                         then specials
                         else ['a'..'z'] ++ ['A'..'Z'] ++ ['0'..'9']
              c <- elements es
              let s = if die == 1
                        then ['"', c, '"']
                        else [c]
              if n == 0 then return s else (s ++) <$> (buildStr' $ n-1) 

  instance Arbitrary AtStr where
    arbitrary = AStr <$> buildStr
      where buildStr = do
              dice <- choose (3, 10) :: Gen Int
              buildStr' dice
            buildStr' n = do
              c <- elements (['a'..'z'] ++ ['A'..'Z'])
              if n == 0 then return [c] else (c:) <$> (buildStr' $ n-1)

  instance Arbitrary ParamStr where
    arbitrary = PStr <$> buildStr
      where buildStr = do
              dice <- choose(0,10) :: Gen Int
              buildStr' dice
            buildStr' n = 
              if n == 0 then return ""
                else do
                  v <- arbitrary :: Gen ValStr
                  a <- arbitrary :: Gen AtStr
                  ((';' : ((show a) ++ "=" ++ (show v))) ++) <$> (buildStr' (n - 1))

                
  prp_value :: ValStr -> Bool
  prp_value (VStr s) = 
    case parse value "test" s of
      Left e  -> False
      Right v -> s == showVal v

  prp_param :: ParamStr -> Property
  prp_param (PStr s) =
    not (null s) && not (null at) ==>
      case parse params "test" s of
        Left  e -> False
        Right v -> True -- s == showParams v
    where at = takeWhile (/= '=') s

  deepCheck :: (Testable p) => p -> IO Result
  deepCheck = quickCheckWithResult stdArgs{maxSuccess=100,
                                           maxDiscard=500}

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
    r <- ((deepCheck prp_value) ?>
          (deepCheck prp_param) )
    case r of
      Success _ -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  type Sample = (String, FilePath)
  
  testSamples :: [Sample] -> IO ()
  testSamples [] = return ()
  testSamples (s:ss) = do
    m <- readFile (snd s)
    case parse mime "Mime" m of
      Left e  -> putStrLn $ "Failed " ++ (fst s) ++ ":  " ++ (show e)
      Right p -> do
        putStrLn $ "Success " ++ (fst s) ++ ": " ++ (show p)
        testSamples ss

  samples :: [Sample]
  samples = [("Simple Parameter", "test/mimesamples/p1.txt"),
             ("text/plain", "test/mimesamples/mime1.txt")]

  main :: IO ()
  main = do 
    testSamples samples 
    checkAll
  
