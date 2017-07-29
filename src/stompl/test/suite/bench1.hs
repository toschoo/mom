{-# Language BangPatterns #-}
module Main 
where

  import Network.Mom.Stompl.Parser

  import qualified Data.ByteString      as B 

  import System.Exit (exitFailure)
  import System.Environment (getArgs)

  import Control.Monad (when)

  import Data.Time.Clock.POSIX

  testParse :: Int -> B.ByteString -> IO (Int,Int,POSIXTime)
  testParse x m = go x 0 0 0
     where go 0 b g u = return (b,g,u)
           go i b g u = do 
             !t1 <- getPOSIXTime
             let !r = stompAtOnce m
             !t2 <- getPOSIXTime
             case r of
               Left  _ -> go (i-1) (b+1) g (u+t2-t1)
               Right _ -> go (i-1) b (g+1) (u+t2-t1)

  main :: IO ()
  main = do
    os <- getArgs
    when (length os < 1) (do
      putStrLn "I need a file name"
      exitFailure)
    let f = head os
    !m  <- B.readFile f
    (!b,!g,!u) <- testParse 1000 m
    putStrLn ("good: "  ++ show g ++ 
              ", bad: " ++ show b ++ 
              " in "    ++ show u)
