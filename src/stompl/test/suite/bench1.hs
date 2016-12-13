{-# Language BangPatterns #-}
module Main 
where

  import Network.Mom.Stompl.Parser
  import Network.Mom.Stompl.Frame

  import qualified Data.ByteString      as B 

  import System.Exit (exitSuccess, exitFailure)
  import System.Environment (getArgs)

  import Control.Applicative ((<$>))
  import Control.Monad (when)

  import Data.Time.Clock.POSIX

  testParse :: Int -> B.ByteString -> (Int,Int)
  testParse x m = go x 0 0
     where go 0 b g = (b,g)
           go i b g = case stompAtOnce m of
                        Left  _ -> go (i-1) (b+1) g
                        Right _ -> go (i-1) b (g+1)

  main :: IO ()
  main = do
    os <- getArgs
    when (length os < 1) (do
      putStrLn "I need a file name"
      exitFailure)
    let f = head os
    !m  <- B.readFile f
    !t1 <- getPOSIXTime
    let !(b,g) = testParse 100 m
    !t2 <- getPOSIXTime
    putStrLn ("good: " ++ show g ++ ", bad: " ++ show b ++ " in " ++ show (t2-t1))
