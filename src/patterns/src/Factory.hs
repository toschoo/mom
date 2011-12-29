{-# OPTIONS -fglasgow-exts -fno-cse #-}
module Factory (
        mkUniqueId)
where

  import System.IO.Unsafe
  import Control.Concurrent

  ------------------------------------------------------------------------
  -- Source for unique identifiers
  ------------------------------------------------------------------------
  {-# NOINLINE _addid #-}
  _addid :: MVar Int
  _addid = unsafePerformIO $ newMVar 1

  mkUniqueId :: IO Int
  mkUniqueId = modifyMVar _addid $ \x -> 
    let x' = incX x
     in return (x', x')

  incX :: Int -> Int
  incX i = if i == 99999999 then 1 else i+1
  
