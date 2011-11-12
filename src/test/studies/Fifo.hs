module Fifo
where

  import Control.Concurrent

  data Queue a = Queue {
                   qRead  :: [a],
                   qWrite :: [a]
                 }
    deriving (Show)

  newQueue :: Queue a
  newQueue = Queue [] []

  readQ :: Queue a -> (Maybe a, Queue a)
  readQ q = 
    let (r, w) = if null (qRead q) 
                   then if null (qWrite q) then ([], [])
                        else (reverse $ qWrite q, [])
                   else (qRead q, qWrite q)
    in if null r then (Nothing, Queue r w)
       else (Just $ head r, Queue (tail r) w)

  writeQ :: Queue a -> a -> Queue a
  writeQ q x = q {qWrite = x : qWrite q}

  newtype Fifo a = Fifo {runFifo :: MVar (Queue a)}

  newFifo :: IO (Fifo a)
  newFifo = do v <- newMVar newQueue
               return $ Fifo v

  push :: Fifo a -> a -> IO ()
  push p x = modifyMVar_ v $ \q -> return $ writeQ q x
    where v = runFifo p

  pop :: Fifo a -> IO (Maybe a)
  pop p = modifyMVar v $ \q -> let (mbX, q') = readQ q
                                in return (q', mbX)
    where v = runFifo p
      
