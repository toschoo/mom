module Main
where

  import Network.Mom.Stompl.Client.Queue
  import qualified Protocol as P
  -- import Network.Mom.Stompl.Client.Factory
  -- import Network.Mom.Stompl.Client.State
  import qualified Data.ByteString.Char8 as B
  import System.Environment
  import System.Exit
  import Network.Socket
  import Codec.MIME.Type    (nullType)
  import Control.Monad      (forever)
  import Control.Concurrent (threadDelay)

  delay :: Int
  delay = 1000

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q] -> withSocketsDo $ conAndSend q 
      _   -> do
        putStrLn "I need a queue name."
        exitFailure

  conAndSend :: String -> IO ()
  conAndSend qn = do
    withConnection "127.0.0.1" 61613 [] [] $ \c -> do
      -- let conv = return . B.pack
      -- q <- newWriter c "Test-Q" qn [] [] conv
      -- forever $ psend c 
      mapM_ (\_ -> psend c) ([1..10000]::[Int]) 

  psend :: Con -> IO ()
  psend cid = do
    c <- getCon cid
    let x = "hello world!"
    let s = B.pack x
    let m = P.mkMessage P.NoMsg NoSub "/q/test" ""
                                nullType (B.length s) NoTx s x
    P.send (conCon c) m (show NoRec) []
    -- threadDelay delay
