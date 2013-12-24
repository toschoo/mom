module Common
where

  import qualified Data.ByteString as B
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import           Control.Applicative ((<$>))
  import           Control.Monad (when)
  import           Control.Exception (SomeException)

  import           Types

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

  onErr :: Criticality -> SomeException -> String -> IO ()
  onErr c e m = putStrLn $ show c ++ ": " ++ show e ++ " - " ++ m

  -------------------------------------------------------------
  -- controlled quickcheck, arbitrary tests
  -------------------------------------------------------------
  deepCheck :: (Testable p) => p -> IO Result
  deepCheck = quickCheckWithResult stdArgs{maxSuccess=1000}

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

