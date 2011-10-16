{-# OPTIONS -fglasgow-exts -fno-cse #-}
module Factory (
        Con(..), mkUniqueConId,
        Sub(..), mkUniqueSubId,
        Tx (..), mkUniqueTxId)
where

  import System.IO.Unsafe
  import Control.Concurrent
  import Control.Applicative ((<$>))

  newtype Con = Con Int
    deriving (Eq)

  instance Show Con where
    show (Con i) = show i

  newtype Sub = Sub Int
    deriving (Eq)

  instance Show Sub where
    show (Sub i) = show i

  newtype Tx = Tx Int
    deriving (Eq)

  instance Show Tx where
    show (Tx i) = show i

  {-# NOINLINE conid #-}
  conid :: MVar Con
  conid = unsafePerformIO $ newMVar (Con 1)

  {-# NOINLINE subid #-}
  subid :: MVar Sub
  subid = unsafePerformIO $ newMVar (Sub 1)

  {-# NOINLINE txid #-}
  txid :: MVar Tx
  txid = unsafePerformIO $ newMVar (Tx 1)

  mkUniqueConId :: IO Con
  mkUniqueConId = mkUniqueId conid incCon

  mkUniqueSubId :: IO Sub
  mkUniqueSubId = mkUniqueId subid incSub

  mkUniqueTxId :: IO Tx
  mkUniqueTxId = mkUniqueId txid incTx

  mkUniqueId :: MVar a -> (a -> a) -> IO a
  mkUniqueId v f = modifyMVar v $ \x -> do
    let x' = f x in return (x', x')

  incCon :: Con -> Con
  incCon (Con n) = Con (n+1)

  incSub :: Sub -> Sub
  incSub (Sub n) = Sub (n+1)

  incTx :: Tx -> Tx
  incTx (Tx n) = Tx (n+1)
  
