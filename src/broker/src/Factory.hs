{-# OPTIONS -fglasgow-exts -fno-cse #-}
module Factory (newConId, newMsgId)
where

  import Types

  import System.IO.Unsafe
  import Control.Concurrent
  import Control.Applicative ((<$>))

  ------------------------------------------------------------------------
  -- Source for unique connection identifiers
  ------------------------------------------------------------------------
  {-# NOINLINE conid #-}
  conid :: MVar Int
  conid = unsafePerformIO $ newMVar 1

  ------------------------------------------------------------------------
  -- Source for unique subscription identifiers
  ------------------------------------------------------------------------
  {-# NOINLINE msgid #-}
  msgid :: MVar Int
  msgid = unsafePerformIO $ newMVar 1

  newConId :: IO ConId
  newConId = ConId <$> show <$> mkUniqueId conid inc

  newMsgId :: IO MsgId
  newMsgId = MsgId <$> ("msg-"++) <$> show <$> mkUniqueId msgid inc

  mkUniqueId :: MVar Int -> (Int -> Int) -> IO Int
  mkUniqueId v f = modifyMVar v $ \x -> 
    let x' = f x in return (x', x')

  inc :: Int -> Int
  inc x = if x >= 999999999 then 1 else x + 1

