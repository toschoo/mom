module Main
where

  import           Helper
  import           System.Exit
  import           System.Timeout
  import qualified System.ZMQ as Z
  import           Test.QuickCheck
  import           Test.QuickCheck.Monadic
  import           Network.Mom.Patterns
  import qualified Data.Enumerator      as E
  import qualified Data.Enumerator.List as EL
  import           Data.Enumerator (($$))
  import qualified Data.Sequence as S
  import           Data.Sequence ((|>))
  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Control.Applicative ((<$>))
  import           Control.Concurrent
  import           Control.Monad.Trans (liftIO)
  import           Control.Exception (try, throwIO, AssertionFailed(..), SomeException)

  ------------------------------------------------------------------------
  -- For debugging it's much nicer to work with digits
  ------------------------------------------------------------------------
  data Digit = Digit Int
    deriving (Read, Eq, Ord)

  instance Show Digit where
    show (Digit d) = show d

  instance Arbitrary Digit where
    arbitrary = Digit <$> elements [0..9]

  ------------------------------------------------------------------------------
  -- Server / Client
  -- - 1 thread, many threads
  -- - bind/connect
  -- - on error
  -- - change parameter
  -- - start / stop
  ------------------------------------------------------------------------------
  ------------------------------------------------------------------------------
  -- Simple synchronous request
  ------------------------------------------------------------------------------
  prp_synReq :: NonEmptyList String -> Property
  prp_synReq (NonEmpty ss) = testContext ss $ \ctx -> 
    testServer ctx ss $ \_ -> 
      withClient ctx (Address "inproc://srv" []) outString inString $ \c -> 
        request c (just "") EL.consume

  ------------------------------------------------------------------------------
  -- Simple asynchronous request
  ------------------------------------------------------------------------------
  prp_asynReq :: NonEmptyList String -> Property
  prp_asynReq (NonEmpty ss) = testContext ss $ \ctx -> 
    testServer ctx ss $ \_ -> 
      withClient ctx (Address "inproc://srv" []) outString inString $ \c -> 
        askFor c (just "") >> waitClient c
    where waitClient c = do
            mb <- checkFor c EL.consume
            case mb of
              Just ei -> return ei
              Nothing -> do threadDelay 1000
                            waitClient c

  ------------------------------------------------------------------------------
  -- Bytestring Converter
  ------------------------------------------------------------------------------
  prp_bstrReq :: NonEmptyList String -> Property
  prp_bstrReq (NonEmpty ss) = testContext (map B.pack ss) $ \ctx ->
    withServer ctx "Test" noparam 128
                   (Address "inproc://srv" []) Bind 
                   idIn idOut onErr
                   (\_ -> one B.empty)-- ignore
                   (\_ _ _ -> mkStream $ map B.pack ss) $ \_ -> 
      withClient ctx (Address "inproc://srv" []) idOut idIn $ \c ->
        request c (just B.empty) EL.consume

  ------------------------------------------------------------------------------
  -- UTF Converter
  ------------------------------------------------------------------------------
  utf8 :: String
  utf8  = "此派男野人です。\n" ++
          "此派男野人です。それ派個共です。\n" ++
          "ひと"

  prp_utf8Req :: Property
  prp_utf8Req = testContext [utf8] $ \ctx ->
    withServer ctx "Test" noparam 128
                   (Address "inproc://srv" []) Bind 
                   inUTF8 outUTF8 onErr
                   (\_ -> one "") -- ignore
                   (\_ _ _ -> mkStream [utf8]) $ \_ -> 
      withClient ctx (Address "inproc://srv" []) outUTF8 inUTF8 $ \c -> do
        putStrLn $ "Test: " ++ utf8
        request c (just "") EL.consume

  {-
  ------------------------------------------------------------------------------
  -- Error
  ------------------------------------------------------------------------------
  prp_onErr :: Property
  prp_onErr = monadicIO $ run (withContext 1 tstDevice) >>= assert
    where mkErr           = throwIO (AssertionFailed "Test")
          onerr m c _ _ _ = putMVar m c
          tstFatal m _    = do
            e <- takeMVar m
            case e of
              Fatal -> return True
              _     -> return False
          tstError p m _  = do
            issue p (mkStream [B.pack "test"])
            e <- takeMVar m
            case e of
              Error -> return True
              _     -> return False
          tstDevice ctx   = 
            withPub ctx (Address "inproc://pub" []) idOut $ \p -> do
              errDevice ctx p mkErr tstFatal ~> (do
                threadDelay 10000 >> -- give the ZMQ some time
                  errDevice ctx p (return ()) (tstError p))
          errDevice ctx _ tmo action = do
              e <- newEmptyMVar
              withDevice ctx "Test" noparam 10000
                             [pollEntry "XSub" XSub 
                                  (Address "inproc://pub" []) Connect "",
                              pollEntry "XPub" XPub
                                  (Address "inproc://sub" []) Bind ""]
                              idIn idOut (onerr e)
                              (\_ -> tmo) (\_ _ _ -> tryIO mkErr) (action e)

  ------------------------------------------------------------------------------
  -- Param
  ------------------------------------------------------------------------------
  prp_Param :: String -> Property
  prp_Param s = monadicIO $ run (withContext 1 tstDevice) >>= assert
    where tstDevice ctx   = 
            withPub ctx (Address "inproc://pub" []) idOut $ \pub -> do
              m <- newEmptyMVar
              withDevice ctx "Test" noparam (-1)
                             [pollEntry "XSub" XSub 
                                  (Address "inproc://pub" []) Connect "",
                              pollEntry "XPub" XPub
                                  (Address "inproc://sub" []) Bind ""]
                              idIn idOut onErr_
                              (\_ -> return ()) 
                              (\p _ _ -> EL.consume >>= \_ -> liftIO $
                                           putMVar m p) $ \dv -> do
                  issue pub (mkStream [B.pack "test"])
                  x <- takeMVar m
                  case x of
                    "" -> do
                      changeParam dv s
                      issue pub (mkStream [B.pack "test"])
                      y <- takeMVar m 
                      return (y == s)
                    _ -> return False

  ------------------------------------------------------------------------------
  -- start / pause
  ------------------------------------------------------------------------------
  prp_Pause :: NonEmptyList String -> Property
  prp_Pause (NonEmpty ss) = monadicIO $ run (withContext 1 tstDevice) >>= assert
    where tstDevice ctx   = 
            withPub ctx (Address "inproc://pub" []) outString $ \pub -> do
              m <- newEmptyMVar
              withDevice ctx "Test" noparam (-1)
                             [pollEntry "XSub" XSub 
                                  (Address "inproc://pub" []) Connect "",
                              pollEntry "XPub" XPub
                                  (Address "inproc://sub" []) Bind ""]
                              inString outString onErr_
                              (\_ -> return()) 
                              (\_ -> passall) $ \dv ->
                withSub ctx "Sub" noparam "" (Address "inproc://sub" []) 
                      inString onErr_ (dump m) $ \_ -> do
                  issue pub (mkStream ss)
                  mbx <- timeout 50000 $ takeMVar m
                  case mbx of
                    Nothing -> return False
                    Just x  -> 
                      if x /= ss then 
                          putStrLn ("received: " ++ show x) >> return False
                        else do
                          pause dv
                          issue pub (mkStream ss)
                          y <- timeout 50000 $ takeMVar m
                          case y of
                            Just _  -> return False
                            Nothing -> do
                              resume dv
                              issue pub (mkStream ss)
                              mbz <- timeout 50000 $ takeMVar m
                              case mbz of
                                Nothing -> return False
                                Just z  -> return (z == ss)

  ------------------------------------------------------------------------------
  -- device commands
  -- note: we cannot test remove in this manner 
  --       since we use "inproc" sockets, withSub can only connect
  --       to a publisher that has already bound its address;
  --       when we remove this publisher, withSub runs into an error...
  ------------------------------------------------------------------------------
  prp_add :: NonEmptyList String -> Property
  prp_add (NonEmpty ss) = monadicIO $ run (withContext 1 tstDevice) >>= assert
    where tstSub ctx m action =
            withSub ctx "Sub2" noparam ""
                        (Address "inproc://sub2" []) 
                        inString onErr_ (dump m) $ \_ -> action
          tstReceive m mb = do
            mbx <- timeout 50000 $ takeMVar m
            case mbx of
              Nothing -> case mb of
                           Just _  -> putStrLn "Just expected" >> return False
                           Nothing -> return True
              Just x  -> case mb of
                           Nothing -> putStrLn "Nothing expected" >> return False
                           Just _  -> return (x == ss)
          tstDevice ctx   = 
            withPub ctx (Address "inproc://pub" []) outString $ \pub -> do
              m1 <- newEmptyMVar
              m2 <- newEmptyMVar
              withDevice ctx "Test" noparam (-1)
                             [pollEntry "XSub" XSub 
                                  (Address "inproc://pub" []) Connect "",
                              pollEntry "XPub1" XPub
                                  (Address "inproc://sub1" []) Bind ""]
                              inString outString onErr_
                              (\_ -> return())
                              (\_ -> passall) $ \dv -> 
                withSub ctx "Sub1" noparam "" 
                        (Address "inproc://sub1" []) 
                        inString onErr_ (dump m1) $ \_ -> do
                    let pe = pollEntry "XPub2" XPub 
                             (Address "inproc://sub2" []) Bind ""
                    issue pub (mkStream ss)
                    tstReceive m1 (Just ss) ~> (do
                        addDevice dv pe
                        tstSub ctx m2 $ do
                          issue pub (mkStream ss)
                          tstReceive m1 (Just ss) ~>
                            tstReceive m2 (Just ss))
  -}

  infixr ~>
  (~>) :: IO Bool -> IO Bool -> IO Bool
  x ~> y = x >>= \r -> if r then y else return False
  
  ------------------------------------------------------------------------------
  -- unicode
  ------------------------------------------------------------------------------

  ------------------------------------------------------------------------------
  -- Generic Tests
  ------------------------------------------------------------------------------
  testContext :: Eq a => a -> 
                 (Context -> IO (Either SomeException a)) -> Property
  testContext ss action = monadicIO $ do
    ei <- run $ withContext 1 action 
    case ei of
      Left  e -> run (putStrLn $ show e) >> assert False
      Right x -> assert (x == ss)
      
  ------------------------------------------------------------------------------
  -- Generic Server Tests
  ------------------------------------------------------------------------------
  testServer :: Z.Context -> [String] -> (Service -> IO a) -> IO a
  testServer ctx ss action =
    withServer ctx "Test" noparam 128
                   (Address "inproc://srv" []) Bind 
                   inString outString onErr
                   (\_ -> one "") -- ignore
                   (\_ _ _ -> mkStream ss) action

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
  dump m _ _ = EL.consume >>= liftIO . (putMVar m) 

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
      Success _ -> f
      x         -> return x

  infixr ?>
  (?>) :: IO Result -> IO Result -> IO Result
  (?>) = applyTest

  -------------------------------------------------------------
  -- Name tests
  -------------------------------------------------------------
  runTest :: String -> IO Result -> IO Result
  runTest s t = putStrLn ("Test: " ++ s) >> t

  checkAll :: IO ()
  checkAll = do
    let good = "OK. All Tests passed."
    let bad  = "Bad. Some Tests failed."
    r <- runTest "Simple synchronous request"
                  (deepCheck prp_synReq)        ?>    
         runTest "Simple asynchronous request" 
                  (deepCheck prp_asynReq)       ?> 
         runTest "ByteString Request"
                  (deepCheck prp_bstrReq)       ?>
         runTest "UTF8 Request"
                  (oneCheck prp_utf8Req)       
    case r of
      Success _ -> do
        putStrLn good
        exitSuccess
      _ -> do
        putStrLn bad
        exitFailure

  main :: IO ()
  main = checkAll
