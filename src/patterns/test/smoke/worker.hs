{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Network.Mom.Patterns
  import qualified Data.Enumerator       as E
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
            (Address "tcp://*:5557" [])
            (return . B.unpack)
            (\e nm _ _ -> do putStrLn $ "Error in Subscription " ++ nm ++ 
                                        ": " ++ show e
                             return Nothing)
            (\_ _ -> return ()) it (\_ _ _ -> return ()) 
            wait
         
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000
           

  it :: String -> () -> E.Iteratee String IO ()
  it p _ = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> liftIO (putStrLn i) >> it p ()
