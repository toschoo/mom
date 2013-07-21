module Main
where

  import           Control.Monad.Trans (liftIO)
  import           Control.Concurrent
  import qualified Data.Conduit as C
  import           Data.Conduit (($=), (=$), (=$=), ($$))

  main :: IO ()
  main = do
    ch <- newChan
    writeChan ch "hello"
    m  <- newMVar (1::Int)
    C.runResourceT (produceCh ch $$ ctrlTx m group [] =$= process m =$ consumeCh ch)
    where group xs = length xs >= 5

  type RIO = C.ResourceT IO

  produceCh :: Chan o -> C.Producer RIO o
  produceCh ch = do
    x <- liftIO (readChan ch)
    C.yield x
    liftIO (threadDelay 500000)
    produceCh ch

  consumeCh :: Chan i -> C.Consumer i RIO ()
  consumeCh ch = C.awaitForever $ \i -> liftIO (writeChan ch i)

  process :: Show a => MVar Int -> C.Conduit a RIO a
  process m = C.awaitForever $ \x -> do
    s <- liftIO (withMVar m $  \i -> return $ 
                   show i ++ ": " ++ show x)
    liftIO (putStrLn s) 
    C.yield x

  ctrlTx :: MVar Int -> ([a] -> Bool) -> [a] -> C.Conduit a RIO a
  ctrlTx m group xs = do
    mbX <- C.await
    case mbX of
      Nothing -> liftIO (modifyMVar_ m $ \_ -> return 0)
      Just x  -> let xx = x : xs
                  in if group xx
                       then do liftIO (modifyMVar_ m $ \i -> return $ i + 1)
                               C.yield x
                               ctrlTx m group [x]
                       else do C.yield x
                               ctrlTx m group xx
