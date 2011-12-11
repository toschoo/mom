{-# OPTIONS -fglasgow-exts -fno-cse #-}
module Config 
where

  import           Types

  import qualified Network.Mom.Stompl.Frame as F

  import           Control.Concurrent
  import           System.IO.Unsafe

  data Config = Cfg {
                  cfgSnd :: Chan SndRequest
                  -- as in Broker1
                  -- number of qs
                  -- number of sessions
                  -- ...
                }

  {-# NOINLINE _cfg #-}
  _cfg :: MVar Config
  _cfg = unsafePerformIO newEmptyMVar 

  initCfg :: Config -> IO ()
  initCfg = putMVar _cfg 

  handle :: ConId -> F.Frame -> Con -> IO ()
  handle cid f c = withMVar _cfg sendReq
    where sendReq cfg = writeChan (cfgSnd cfg) (FrameReq cid f c)

  waitRequest :: IO SndRequest
  waitRequest = readMVar _cfg >>= \cfg -> readChan (cfgSnd cfg)
  
