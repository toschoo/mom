{-# OPTIONS -fno-cse #-}
module Main
where

  import Test.QuickCheck
  import Test.QuickCheck.Monadic
  import Data.List (find)
  import Control.Applicative((<$>))
  import Control.Concurrent
  import Control.Monad (when)
  import Data.Time.Clock
  import Data.Maybe

  import System.Exit
  import System.IO.Unsafe

  import Fifo 


  --------------------------------------------------------
  -- Just a test
  --------------------------------------------------------
  prp_simple :: Int -> Property
  prp_simple i = collect i $ monadicIO $ assert (i == i)

  --------------------------------------------------------
  -- n pushed == n popped
  --------------------------------------------------------
  prp_Eq :: NonEmptyList Int -> Property
  prp_Eq (NonEmpty is) = monadicIO $ do
    f <- run newFifo
    run $ mapM_ (push f) is
    is' <- run $ pops f
    assert (is == is')
    where pops f = do
            mb <- pop f
            case mb of
              Nothing -> return []
              Just x  -> do
                xs <- pops f
                return $ x : xs

  --------------------------------------------------------
  -- 1 pushed == 1 popped
  --------------------------------------------------------
  prp_Pop :: NonEmptyList Int -> Property
  prp_Pop (NonEmpty is) = monadicIO $ do
    f <- run newFifo
    run $ push f (head is)
    h <- run $ pop f
    assert (isJust h && fromJust h == head is)

  --------------------------------------------------------
  -- Empty
  --------------------------------------------------------
  prp_Empty :: Property
  prp_Empty = monadicIO $ do
    f <- run newFifo
    e <- run $ pop f
    assert (isNothing e)

  -------------------------------------------------------------
  -- controlled quickcheck, arbitrary tests
  -------------------------------------------------------------
  deepCheck :: (Testable p) => p -> IO Result
  deepCheck = quickCheckWithResult stdArgs{maxSuccess=1000,
                                           maxDiscard=5000}

  -------------------------------------------------------------
  -- do just one test
  -------------------------------------------------------------
  oneCheck :: (Testable p) => p -> IO Result
  oneCheck = quickCheckWithResult stdArgs{maxSuccess=1,
                                          maxDiscard=1}

  -------------------------------------------------------------
  -- combinator, could be a monad...
  -------------------------------------------------------------
  applyTest :: IO Result -> IO Result -> IO Result
  applyTest r f = do
    r' <- r
    case r' of
      Success _ -> f
      x         -> return x

  infixr ?>
  (?>) :: IO Result -> IO Result -> IO Result
  (?>) = applyTest

  -------------------------------------------------------------
  -- The test battery
  -------------------------------------------------------------
  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    r <- deepCheck prp_Pop   ?>
         deepCheck prp_Empty ?>
         deepCheck prp_Eq
    case r of
      Success _ -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
  
