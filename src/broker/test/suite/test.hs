module Main 
where

  import Test
  import System.Exit 
  import Control.Monad.State

  test1 :: IO TestResult
  test1 = return Pass

  test2 :: IO TestResult
  test2 = do
    k <- getLine
    if (read k::Integer) > 0
      then return Pass
      else return Fail

  test3 :: IO TestResult
  test3 = return Pass

  tests :: TestGroup IO
  tests = mkGroup "IO Tests" (Stop Fail)
                   [mkTest "Test 1" test1,
                    mkTest "Test 2" test2,
                    mkTest "Test 3" test3]

  type Stateful = State String 

  testS1 :: Stateful TestResult
  testS1 = do
    s <- get
    if null s then return Fail
      else do 
        if s == "0" then put "1" else put "2"
        return Pass

  testS2 :: Stateful TestResult
  testS2 = do
    s <- get
    if s == "1"
      then return Pass
      else return Fail

  testsS :: TestGroup Stateful
  testsS = mkGroup "State Tests" (Stop Fail)
                   [mkTest "TestS1" testS1,
                    mkTest "TestS2" testS2]

  applySTests :: (TestResult, String)
  applySTests = evalState (execGroup testsS) "0"

  main :: IO ()
  main = do
    -- s <- evaluate (Stop Fail) tests
    -- s <- evaluate End tests
    let (r, s) = applySTests 
    putStrLn s
    if r == Pass then exitSuccess else exitFailure

