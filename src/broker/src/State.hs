{-# OPTIONS -fglasgow-exts -fno-cse #-}
module State (withQ, withQAdd, withAllQs,
              addQ, rmQ,
              addSub, rmSub)
where
 
  import           Types
  import           Exception
  import           Queue
  import           Fifo

  import           Control.Concurrent
  import           Control.Monad (when)
  import           Control.Exception 
  import           Prelude hiding (catch)

  import           System.IO.Unsafe
  import qualified Data.ByteString as B
  import           Data.List (delete, deleteBy, find)
  import           Data.Map   (Map)
  import qualified Data.Map   as   M
  import           Data.IOMap (IOMap)
  import qualified Data.IOMap as IOM

  data State = NewState {
                 stSubs :: SubDir,
                 stQs   :: IOMap QName Queue
               }

  type SubDir = MVar (Map SubId QName)

  newSubDir :: IO SubDir
  newSubDir = newMVar M.empty

  {-# NOINLINE _st #-}
  _st :: MVar State
  _st = unsafePerformIO $ do 
          s <- newSubDir
          q <- IOM.empty
          newMVar $ NewState s q

  updState :: (State -> IO (State, a)) -> IO a
  updState = modifyMVar _st

  updState_ :: (State -> IO State) -> IO ()
  updState_ = modifyMVar_ _st

  withState :: (State -> IO ()) -> IO ()
  withState act = readMVar _st >>= act

  getState :: IO State
  getState = readMVar _st

  withQ :: (Queue -> IO Queue) -> QName -> IO ()
  withQ act q = withState actOnQ
    where actOnQ st = IOM.with_ (stQs st) q act 

  withQAdd :: (Queue -> IO Queue) -> QName -> IO ()
  withQAdd act q = withState addWith
    where addWith st = do
            fifo <- newFifo
            qu   <- act $ newQueue q One Nothing fifo
            IOM.insert (stQs st) upd q qu
          upd o _ = act o

  withAllQs :: (Queue -> IO Queue) -> IO ()
  withAllQs act = withState actOnQs
    where actOnQs st = IOM.withAll 1000 (stQs st) act

  updQ :: Queue -> Queue -> IO Queue
  updQ o n | null (subsInQ n) = return o
           | otherwise        = return $ addSubToQ (head $ subsInQ n) o

  addQ :: Queue -> IO ()
  addQ q = withState addToQs
    where addToQs st = do
            report (mkLog $ qName q) DEBUG "Adding Queue" 
            IOM.insert (stQs st) updQ (qName q) q

  rmQ :: Queue -> IO ()
  rmQ q = withState rmFromQs
    where rmFromQs st = do
            report (mkLog $ qName q) DEBUG "Removing Queue" 
            IOM.delete (stQs st) (qName q) 

  addSub :: QName -> Sub -> IO ()
  addSub qn s = withState addSubToQ 
    where addSubToQ st = do
            report (mkLog qn) DEBUG $ "Adding Subscription " ++ show (subId s)
            f <- newFifo
            let q = newQueue qn One (Just s) f -- define pattern!
            modifyMVar_ (stSubs st) (return . M.insert (subId s) qn)
            IOM.insert  (stQs st) updQ qn q

  rmSub :: SubId -> IO () -- if sub is null, we still need the queue!
  rmSub s = withState rmSubFromSt
    where rmSubFromSt st = do
            mbQ <- withMVar (stSubs st) (return . M.lookup s) 
            case mbQ of
              Nothing -> return ()
              Just q  -> do 
                report (mkLog q) DEBUG $ "Removing Subscription " ++ show s
                IOM.with_   (stQs   st) q (return . rmSubFromQ s)
                modifyMVar_ (stSubs st)   (return . M.delete   s)

  -- remove a connection!
  -- or remove connection when it's not working?

  mkLog :: QName -> String
  mkLog q = "Queue " ++ show q

