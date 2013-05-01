module Common
where

  import qualified System.ZMQ as Z
  import qualified Data.ByteString as B
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import           Network.Mom.Patterns.Streams.Types
  import           Network.Mom.Patterns.Streams.Streams
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
  {-
  testServer :: Context -> [String] -> (Controller -> IO a) -> IO a
  testServer ctx ss =
    withServer ctx "Test" "inproc://srv" Bind
                   (\_ -> return ()) onErr
                   (\_ _ _ -> return ())
  -}

  ignoreTmo :: StreamAction
  ignoreTmo _ = return ()

  onErr :: Criticality -> SomeException -> String -> IO ()
  onErr c e m = putStrLn $ show c ++ ": " ++ show e ++ " - " ++ m

  sendAll :: Z.Socket a -> [B.ByteString] -> IO ()
  sendAll _ [] = return ()
  sendAll s (m:ms) = let flg = if null ms 
                                 then []
                                 else [Z.SndMore]
                      in Z.send s m flg >> sendAll s ms

  recvAll :: Z.Socket a -> IO [B.ByteString]
  recvAll s = go []
    where go ms = do
            m <- Z.receive s [] 
            x <- Z.moreToReceive s
            if x then go     (m:ms)
                 else return $ reverse (m:ms)

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

