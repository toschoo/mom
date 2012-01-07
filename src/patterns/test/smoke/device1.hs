module Main 
where

  import           Network.Mom.Device -- Patterns.Device
  import           Network.Mom.Patterns -- Patterns.Device
  import           System.Environment
  import qualified Data.Enumerator       as E
  import qualified Data.Enumerator.List  as EL
  import qualified Data.ByteString.Char8 as B
  import           Control.Monad.Trans
  import           Control.Concurrent
  import           Control.Monad
  import           Control.Exception

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    let src = pollEntry "Publisher"   Sub (Address "tcp://localhost:5556" []) Connect ""
    let trg = pollEntry "Subscribers" Pub (Address "tcp://*:5557"         []) Bind    ""
    withContext 1 $ \ctx -> 
      withDevice ctx "Weather Report Forwarder" noparam (-1) [src,trg]
              (return . B.unpack) (return . B.pack)
              (\e nm _ _ -> putStrLn $ "Error in Subscription " ++ nm ++ 
                                       ": " ++ show e)
              (\_ -> return ()) putThrough wait
  
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000
     
  putThrough :: String -> Transformer String
  putThrough _ s' os' = EL.head >>= \mbo -> go mbo s' os'
    where go mbo s _ = do
            mbo' <- EL.head
            case mbo of
              Nothing -> return ()
              Just x  -> do
                let lst = case mbo' of
                            Nothing -> True
                            Just _  -> False
                pass s ["Subscribers"] x lst (go mbo')

