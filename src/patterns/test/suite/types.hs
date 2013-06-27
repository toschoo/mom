module Main
where

  import           Common
  import           System.Exit
  import           System.Timeout
  import qualified System.ZMQ as Z
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import qualified Data.ByteString.Char8 as B
  import           Data.Time.Clock
  import           Data.Monoid
  import           Data.List (sort)
  import qualified Data.Conduit as C
  import           Data.Conduit (($=), (=$), ($$))
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Monad (unless)
  import           Control.Monad.Trans (liftIO)
  import           Control.Exception (AssertionFailed(..), 
                                      try, throwIO, SomeException)

  import           Network.Mom.Patterns.Streams.Types


  isock,osock :: String
  isock = "inproc://_in"
  osock = "inproc://_out"
  osock1, osock2, osock3, osock4, osock5, osock6 :: String
  osock1 = "inproc://_out1"
  osock2 = "inproc://_out2"
  osock3 = "inproc://_out3"
  osock4 = "inproc://_out4"
  osock5 = "inproc://_out5"
  osock6 = "inproc://_out6"

  prpStreamList :: NonEmptyList String -> Property
  prpStreamList (NonEmpty s) = testContext s $ \ctx -> 
    try $ (map B.unpack) <$> C.runResourceT (
          streamList (map B.pack s) $$ consume)

  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    putStrLn "========================================="
    putStrLn "       Patterns Library Test Suite"
    putStrLn "                 Streams"
    putStrLn "========================================="
    r <- runTest "Stream list"
                  (deepCheck prpStreamList)          -- ?>


    case r of
      Success {} -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
