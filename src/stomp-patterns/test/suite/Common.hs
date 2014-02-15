module Common
where

  import qualified Data.ByteString.Char8 as B
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import           Control.Applicative ((<$>))
  import           Control.Monad (when)
  import           Control.Exception (SomeException)
  import           Control.Concurrent
  import           System.IO (stdout, hFlush)

  import           Types

  import           Network.Mom.Stompl.Client.Queue

  ------------------------------------------------------------------------
  -- For debugging it's much nicer to work with digits
  ------------------------------------------------------------------------
  data Digit = Digit Int
    deriving (Read, Eq, Ord)

  instance Show Digit where
    show (Digit d) = show d

  instance Arbitrary Digit where
    arbitrary = Digit <$> elements [1..9]

  ------------------------------------------------------------------------
  -- Ease working with either
  ------------------------------------------------------------------------
  infixl 9 ~>
  (~>) :: IO Bool -> IO Bool -> IO Bool
  x ~> f = x >>= \t -> if t then f else return False

  stringIn :: InBound String
  stringIn _ _ _ = return . B.unpack

  stringOut :: OutBound String
  stringOut = return . B.pack

  subtleErr :: MVar Int -> OnError
  subtleErr o _ _ = do 
    x <- modifyMVar o (\x -> let r = if x == 0 then 1 else 0  
                              in return (r,x))
    let ch = "\b" ++ if x /= 0 then "O" else "o"
    putStr ch >> hFlush stdout

  -------------------------------------------------------------
  -- controlled quickcheck, arbitrary tests
  -------------------------------------------------------------
  deepCheck :: (Testable p) => p -> IO Result
  deepCheck = quickCheckWithResult stdArgs{maxSuccess=100}

  -------------------------------------------------------------
  -- controlled quickcheck, arbitrary tests
  -------------------------------------------------------------
  someCheck :: (Testable p) => Int -> p -> IO Result
  someCheck n = quickCheckWithResult stdArgs{maxSuccess=n}

  -------------------------------------------------------------
  -- do just one test
  -------------------------------------------------------------
  oneCheck :: (Testable p) => p -> IO Result
  oneCheck = quickCheckWithResult stdArgs{maxSuccess=1}

  -------------------------------------------------------------
  -- combinator, could be a monad...
  -------------------------------------------------------------
  applyTest :: IO Result -> IO Result -> IO Result
  applyTest r f = do
    r' <- r
    case r' of
      Success {} -> f
      x          -> return x

  infixr ?>
  (?>) :: IO Result -> IO Result -> IO Result
  (?>) = applyTest

  -------------------------------------------------------------
  -- Name tests
  -------------------------------------------------------------
  runTest :: String -> IO Result -> IO Result
  runTest s t = putStrLn ("Test: " ++ s) >> t

