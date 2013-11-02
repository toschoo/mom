{-# LANGUAGE RankNTypes, Rank2Types #-}
module Network.Mom.Stompl.Client.Streams (
                             RIO, Producer, Consumer, Conduit,
                             streamList, passThrough, consume,
                             Control(..), ctrAddRec, nullCtrl, 
                             injectCtrl, 
                             sourceQ,
                             sinkQ, sinkAdHoc,
                             sinkQWith, sinkAdHocWith,
                             process,
                             waitEachRec, recBuffer,
                             controlTx)
                             
where

  import           Network.Mom.Stompl.Client.Queue
  import           Network.Mom.Stompl.Frame (Header)
  import           Codec.MIME.Type (Type, nullType)
  import qualified Data.Conduit as C
  import           Data.Conduit (($=), ($$), (=$), (=$=))
  import           Data.Maybe (isNothing)
  import qualified Data.ByteString.Char8 as B
  import           Control.Monad.Trans (liftIO)
  import           Control.Monad       (when)
  import           Control.Exception   (throwIO)
  import           Control.Concurrent                       
  import           Control.Applicative ((<$>))
  import           System.Timeout

  type RIO            = C.ResourceT                IO 
  type Producer a o   = C.Producer                RIO (Control a, o) 
  type Consumer a i r = C.Consumer (Control a, i) RIO r
  type Conduit  a i o = C.Conduit  (Control a, i) RIO (Control a, o)

  streamList :: Control a -> [o] -> Producer a o
  streamList c = mapM_ (\o -> C.yield (c, o))

  passThrough :: Conduit a i i
  passThrough = C.awaitForever C.yield

  consume :: Consumer a i [i]
  consume = loop []
    where loop is = do
            mbI <- C.await
            case mbI of
              Nothing    -> return $ reverse is
              Just (c,i) -> loop (i:is) 

  data Control a = Ctrl {
                     ctrHeaders :: [Header],
                     ctrType    :: Type,
                     ctrDest    :: String,
                     ctrRecs    :: [Receipt],
                     ctrObj     :: a
                   }

  ctrAddRec :: Control a -> Receipt -> Control a
  ctrAddRec c r = c {ctrRecs = r : ctrRecs c}

  nullCtrl :: a -> Control a
  nullCtrl x = Ctrl [] nullType "" [] x

  injectCtrl :: Control a -> C.Conduit i RIO (Control a, i)
  injectCtrl c = C.awaitForever $ \i -> C.yield (c, i)

  ------------------------------------------
  -- source: send to this one
  -- role: header, body, tail
  ------------------------------------------
  sourceQ :: Reader i -> Control a -> Producer a (Message i)
  sourceQ q c  = liftIO (readQ q) >>= \m -> 
                   C.yield (c, m) >> sourceQ q c

  sinkQ :: Writer o -> Consumer a o ()
  sinkQ q = C.awaitForever $ \(c,i) -> liftIO (writeQ q (ctrType    c) 
                                                        (ctrHeaders c) i)

  sinkAdHoc :: Writer o -> Consumer a o ()
  sinkAdHoc q = C.awaitForever $ \(c,i) -> 
                  liftIO (writeAdHoc q (ctrDest    c) 
                                       (ctrType    c)
                                       (ctrHeaders c) i)

  sinkQWith :: Writer o -> Conduit a o o
  sinkQWith q = C.awaitForever $ \(c,i) -> do
                  c' <- ctrAddRec c <$> liftIO (
                          writeQWith q (ctrType c)
                                       (ctrHeaders c) i)
                  C.yield (c', i)
                  sinkQWith q

  sinkAdHocWith :: Writer o -> Conduit a o o
  sinkAdHocWith q = C.awaitForever $ \(c,i) -> do
                      c' <- ctrAddRec c <$> liftIO (
                              writeAdHocWith q (ctrDest c)
                                               (ctrType c)
                                               (ctrHeaders c) i)
                      C.yield (c', i)
                      sinkAdHocWith q

  process :: (Control a -> i -> IO (Control a, o)) -> Conduit a i o
  process f = C.awaitForever $ \(c, i) -> do
                (c', o) <- liftIO (f c i)
                C.yield (c', o)
                                                                
  waitEachRec :: Con -> Int -> Conduit a i i
  waitEachRec cid tmo = C.awaitForever $ \(c, i) -> 
                          case ctrRecs c of
                            []    -> C.yield (c, i)
                            (r:_) -> do
                              let c' = c {ctrRecs = tail $ ctrRecs c}
                              liftIO (cWaitReceipt cid tmo r)
                              C.yield (c', i)
  
  recBuffer :: Con -> Int -> Control a -> Consumer a () (Control a)
  recBuffer cid tmo c = case ctrRecs c of
                          [] -> return c
                          rs -> liftIO (mapM_ go rs) >> return c{ctrRecs = []}
    where go = cWaitReceipt cid tmo

  cWaitReceipt :: Con -> Int -> Receipt -> IO ()
  cWaitReceipt c tmo r = do mb_ <- timeout tmo $ waitReceipt c r
                            when (isNothing mb_) $ throwIO $ AppException $
                                      "Timeout waiting on Receipt " ++ show r

  -- ack/nack

  ------------------------------------------------------------------------
  -- Transaction Control
  -- someSrc $$ inTx sink
  -- or:
  -- sink = do
  --   mbX <- C.await
  --   case mbX of
  --     Nothing -> ...
  --     Just i  -> if group i 
  --                  then do terminateTx tx
  --                          inTx sink
  --                  else sink
  ------------------------------------------------------------------------
  controlTx :: Con -> [Topt] -> ([i] -> IO Bool) -> Conduit a i i
  controlTx cid os group = 
    C.bracketP (do tx <- beginTx cid os
                   newMVar tx)
               (\m -> withMVar m $ \tx -> abortTx cid tx)
               (loop [])
    where loop is m = do
            mbI <- C.await
            case mbI of
              Nothing    -> liftIO (withMVar m $ \tx -> commit cid tx)
              Just (c,i) -> do
                let ii = i:is
                ok <- liftIO (group ii)
                if ok then do liftIO (modifyMVar_ m $ \tx -> 
                                commit  cid tx >> beginTx cid os)
                              C.yield (c,i) 
                              loop [i] m
                      else do C.yield (c,i) 
                              loop ii  m
