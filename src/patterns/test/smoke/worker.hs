{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Network.Mom.Patterns
  import qualified Data.Enumerator.List  as EL
  import qualified Data.ByteString.Char8 as B
  import           Control.Monad.Trans
  import           Control.Concurrent
  import           Control.Monad

  noparam :: String
  noparam = ""

  main :: IO ()
  main = 
    withContext 1 $ \ctx -> 
      withPuller ctx "Worker" noparam 
            (Address "tcp://localhost:5558" [])
            (return . B.unpack)
            (\e nm _ _ -> do putStrLn $ "Error in Subscription " ++ nm ++ 
                                        ": " ++ show e)
            it wait
         
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000
           

  it :: String -> Dump String 
  it p c = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> liftIO (putStrLn i) >> it p c 
