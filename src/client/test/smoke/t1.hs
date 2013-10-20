module Main
where

  import Control.Concurrent (threadDelay)

  main :: IO ()
  main = do
    let i = [1..100]
    let s = foldr (+) 0 i
    let m = length i
    print (s `div` m)
    threadDelay 1000000
