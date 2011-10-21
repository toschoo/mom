module Main
where

  import Test
  import Protocol (Message(..))

  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Parser 
  import           Network.Mom.Stompl.Client.Queue

  import           Control.Concurrent

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.Maybe

  import           System.Exit
  import           System.Timeout

  import           Codec.MIME.Type (nullType, Type)
  
  maxRcv :: Int
  maxRcv = 1024

  host :: String
  host = "127.0.0.1"

  port :: Int
  port = 61613

  vers :: [F.Version]
  vers = [(1,0), (1,1)]

  beat :: F.Heart
  beat = (0,0)

  tQ1 :: String
  tQ1 = "/q/test1"

  iconv :: Converter String
  iconv = InBound (\_ _ _ -> return . U.toString)

  oconv :: Converter String
  oconv = OutBound (return . B.pack)

  tmo :: IO a -> IO (Maybe a)
  tmo = timeout 500000

  text1, text2, text3, text4 :: String
  text1 = "whose woods these are I think I know"
  text2 = "his house is in the village though"
  text3 = "he will not see me stopping here"
  text4 = "to watch is woods fill up with snow"

  main :: IO ()
  main = do
    r <- runTests mkTests
    case r of
      Pass   -> exitSuccess
      Fail e -> exitFailure

  runTests :: TestGroup IO -> IO TestResult
  runTests g = do
    (r, s) <- execGroup g
    putStrLn s
    return r

  mkTests :: TestGroup IO
  mkTests = 
    let t10 = mkTest "Send and Receive" testSndRcv
        t20 = mkTest "Transaction     " testTx1
    in  mkGroup "Send and Receive" (Stop (Fail "")) [t10, t20]

  testSndRcv :: IO TestResult
  testSndRcv = 
    stdCon $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv

      writeQ oQ nullType [] text1
      m <- readQ iQ
      if msgCont m == text1
        then return Pass
        else return $ Fail $ "Received: " ++ (msgCont m) ++ " - Expected: " ++ text1

  testTx1 :: IO TestResult
  testTx1 =
    stdCon $ \c -> do
      iQ <- newQueue c "IN"  tQ1 [OReceive] [] iconv
      oQ <- newQueue c "OUT" tQ1 [OSend]    [] oconv
      ok <- withTransaction c [] $ \_ -> do
        writeQ oQ nullType [] text1
        writeQ oQ nullType [] text2
        writeQ oQ nullType [] text3
        writeQ oQ nullType [] text4
        mbM <- tmo $ readQ iQ
        case mbM of
          Nothing -> return True
          Just _  -> return False
      if not ok then return $ Fail "obtained messages sent in TX before commit!"
        else do
          mbM1 <- tmo $ readQ iQ
          mbM2 <- tmo $ readQ iQ
          mbM3 <- tmo $ readQ iQ
          mbM4 <- tmo $ readQ iQ
          let ms = catMaybes [mbM1, mbM2, mbM3, mbM4]
          if (length ms) /= 4
            then return $ Fail "Did not receive all messages!"
            else let zs = zip ms [text1, text2, text3, text4]
                     ts = map (\x -> (msgCont . fst) x == snd x) zs
                 in if and ts then return Pass
                              else return $ Fail $
                                     "Messages are in wrong order: " ++ 
                                     (show $ map msgCont ms)

  stdCon :: (Con -> IO TestResult) -> IO TestResult
  stdCon act =
    withConnection host port maxRcv "guest" "guest" beat act
      
