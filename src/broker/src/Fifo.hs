module Fifo (Fifo, newFifo, push, pop, count, empty)
where

  import Control.Concurrent

  ------------------------------------------------------------------------
  -- Queue for fast write and medium read
  ------------------------------------------------------------------------
  data Queue a = Queue {
                   qCount :: Int,
                   qRead  :: [a],
                   qWrite :: [a]
                 }
    deriving (Show)

  newQueue :: Queue a
  newQueue = Queue 0 [] [] 

  ------------------------------------------------------------------------
  -- Read: 
  -- qRead = reverse of qWrite if qWrite not empty
  -- head of qRead if qRead not empty
  -- Nothing otherwise
  ------------------------------------------------------------------------
  readQ :: Queue a -> (Maybe a, Queue a)
  readQ q = 
    if null r then (Nothing, q) else (Just $ head r, 
                                      Queue (qCount q - 1) (tail r) w)
    where (r, w) = if null (qRead q) 
                     then if null (qWrite q) then ([], [])
                          else (reverse $ qWrite q, [])
                     else (qRead q, qWrite q)

  ------------------------------------------------------------------------
  -- cons to qWrite
  ------------------------------------------------------------------------
  writeQ :: Queue a -> a -> Queue a
  writeQ q x = q {qCount = qCount q + 1, 
                  qWrite = x : qWrite q}

  ------------------------------------------------------------------------
  -- concurrent queue
  ------------------------------------------------------------------------
  newtype Fifo a = Fifo {runFifo :: MVar (Queue a)}

  ------------------------------------------------------------------------
  -- constructor
  ------------------------------------------------------------------------
  newFifo :: IO (Fifo a)
  newFifo = newMVar newQueue >>= return . Fifo

  ------------------------------------------------------------------------
  -- push
  ------------------------------------------------------------------------
  push :: Fifo a -> a -> IO ()
  push p x = modifyMVar_ v $ \q -> return $ writeQ q x
    where v = runFifo p

  ------------------------------------------------------------------------
  -- pop
  ------------------------------------------------------------------------
  pop :: Fifo a -> IO (Maybe a)
  pop p = modifyMVar v $ \q -> let (mbX, q') = readQ q in return (q', mbX)
    where v = runFifo p

  ------------------------------------------------------------------------
  -- count message
  ------------------------------------------------------------------------
  count :: Fifo a -> IO Int
  count p = readMVar v >>= return . qCount
    where v = runFifo p

  empty :: Fifo a -> IO Bool
  empty p = readMVar v >>= return . (== 0) . qCount
    where v = runFifo p
