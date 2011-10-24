{-# OPTIONS -fglasgow-exts -fno-cse #-}
module Factory (
        Con(..), mkUniqueConId,
        Sub(..), mkUniqueSubId,
        Tx (..), mkUniqueTxId,
        Rec(..), mkUniqueRecc, parseRec)
where

  import System.IO.Unsafe
  import Control.Concurrent
  import Data.Char (isDigit)

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

  data Rec = Rec Int | NoRec
    deriving (Eq)

  instance Show Rec where
    show (Rec i) = show i
    show  NoRec  = ""

  parseRec :: String -> Maybe Rec
  parseRec s = 
    if numeric s then Just (Rec $ read s) else Nothing

  numeric :: String -> Bool
  numeric = and . map isDigit

  {-# NOINLINE conid #-}
  conid :: MVar Con
  conid = unsafePerformIO $ newMVar (Con 1)

  {-# NOINLINE subid #-}
  subid :: MVar Sub
  subid = unsafePerformIO $ newMVar (Sub 1)

  {-# NOINLINE txid #-}
  txid :: MVar Tx
  txid = unsafePerformIO $ newMVar (Tx 1)

  {-# NOINLINE recc #-}
  recc :: MVar Rec
  recc = unsafePerformIO $ newMVar (Rec 1)

  mkUniqueConId :: IO Con
  mkUniqueConId = mkUniqueId conid incCon

  mkUniqueSubId :: IO Sub
  mkUniqueSubId = mkUniqueId subid incSub

  mkUniqueTxId :: IO Tx
  mkUniqueTxId = mkUniqueId txid incTx

  mkUniqueRecc :: IO Rec
  mkUniqueRecc = mkUniqueId recc incRecc

  mkUniqueId :: MVar a -> (a -> a) -> IO a
  mkUniqueId v f = modifyMVar v $ \x -> do
    let x' = f x in return (x', x')

  incCon :: Con -> Con
  incCon (Con n) = Con (incX n)

  incSub :: Sub -> Sub
  incSub (Sub n) = Sub (incX n)

  incTx :: Tx -> Tx
  incTx (Tx n) = Tx (incX n)

  incRecc :: Rec -> Rec
  incRecc (Rec n) = Rec (incX n)
  incRecc (NoRec) = NoRec

  incX :: Int -> Int
  incX i = if i == 99999999 then 1 else i+1
  
