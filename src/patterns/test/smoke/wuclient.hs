{-# LANGUAGE BangPatterns #-}
module Main 
where

  import           Network.Mom.Patterns
  import           System.Environment
  import qualified Data.Enumerator       as E
  import qualified Data.Enumerator.List  as EL
  import qualified Data.ByteString.Char8 as B
  import           Control.Exception
  import           Control.Monad.Trans
  import           Control.Concurrent
  import           Control.Monad

  noparam :: String
  noparam = ""

  main :: IO ()
  main = do
    os <- getArgs
    let topic = case os of
                  [x] -> x       
                  _   -> "10001" 
    withContext 1 $ \ctx -> rcv ctx topic 
  
  wait :: Service -> IO ()
  wait s = forever $ do putStrLn $ "Waiting for " ++ srvName s ++ "..."
                        threadDelay 1000000
           

  rcv :: Context -> String -> IO ()
  rcv ctx sub = 
    withSub ctx "Weather Report" noparam sub 100 
            (Address "tcp://localhost:5556" [])
            (return . B.unpack)
            (\e nm _ -> putStrLn $ "Error in Subscription " ++ nm ++ 
                                   ": " ++ show e)
            it wait

  it :: String -> E.Iteratee String IO ()
  it p = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> liftIO (putStrLn i) >> it p 
