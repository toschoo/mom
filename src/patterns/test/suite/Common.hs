module Common
where

  import           Helper
  import qualified System.ZMQ as Z
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import           Network.Mom.Patterns
  import qualified Data.Enumerator      as E
  import qualified Data.Enumerator.List as EL
  import           Data.Enumerator (($$))
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Monad.Trans (liftIO)
  import           Control.Exception (try, SomeException)

  ------------------------------------------------------------------------
  -- For debugging it's much nicer to work with digits
  ------------------------------------------------------------------------
  data Digit = Digit Int
    deriving (Read, Eq, Ord)

  instance Show Digit where
    show (Digit d) = show d

  instance Arbitrary Digit where
    arbitrary = Digit <$> elements [0..9]

  ------------------------------------------------------------------------
  -- Ease working with either
  ------------------------------------------------------------------------
  infixl 9 ~>
  (~>) :: IO Bool -> IO Bool -> IO Bool
  x ~> f = x >>= \t -> if t then f else return False

  ------------------------------------------------------------------------------
  -- Generic Tests
  ------------------------------------------------------------------------------
  testContext :: Eq a => a -> 
                 (Context -> IO (Either SomeException a)) -> Property
  testContext ss action = monadicIO $ do
    ei <- run $ withContext 1 action 
    case ei of
      Left  e -> run (print e) >> assert False
      Right x -> assert (x == ss)
      
  ------------------------------------------------------------------------------
  -- Generic Server Tests
  ------------------------------------------------------------------------------
  testServer :: Z.Context -> [String] -> (Service -> IO a) -> IO a
  testServer ctx ss =
    withServer ctx "Test" noparam 1
                   (Address "inproc://srv" []) Bind 
                   inString outString onErr
                   (\_ -> one "") -- ignore
                   (\_ _ _ -> mkStream ss)

  ------------------------------------------------------------------------------
  -- Connect without giving up
  ------------------------------------------------------------------------------
  trycon :: Z.Socket a -> String -> IO ()
  trycon s a = do ei <- try $ Z.connect s a
                  case ei of
                    Left  e -> do threadDelay 1000
                                  let _ = show (e::SomeException)
                                  trycon s a
                    Right _ -> return ()

  ------------------------------------------------------------------------
  -- stream from list
  ------------------------------------------------------------------------
  mkStream :: [a] -> E.Enumerator a IO b
  mkStream ss step = 
    case step of
      (E.Continue k) -> 
        if null ss then E.continue k
          else mkStream (tail ss) $$ k (E.Chunks [head ss])
      _ -> E.returnI step  

  ------------------------------------------------------------------------------
  -- Just build up a list and store it in MVar
  ------------------------------------------------------------------------------
  makeList :: MVar [Int] -> E.Iteratee Int IO ()
  makeList m = do
    mb <- EL.head
    case mb of
      Nothing -> return ()
      Just i  -> tryIO (modifyMVar_ m (\l -> return (i:l))) >> makeList m

  ------------------------------------------------------------------------------
  -- return a list in an MVar
  ------------------------------------------------------------------------------
  dump :: MVar [a] -> Dump a
  dump m _ _ = EL.consume >>= liftIO . putMVar m

  -------------------------------------------------------------
  -- controlled quickcheck, arbitrary tests
  -------------------------------------------------------------
  deepCheck :: (Testable p) => p -> IO Result
  deepCheck = quickCheckWithResult stdArgs{maxSuccess=100,
                                           maxDiscard=500}

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

