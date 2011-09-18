module Main 
where

  import Network.Mom.Stompl.Types
  import Test.QuickCheck
  import Data.List (foldl')

  deepCheck :: (Testable p) => p -> IO ()
  deepCheck = quickCheckWith stdArgs{maxSuccess=1000,
                                     maxDiscard=5000}

  -- dropNothing --
  type Mayflower = (Maybe Int, Int)

  con :: [a] -> Bool
  con x = length x > 10

  -- length of dropNothing = length - countNothing
  pLenMinusNothing :: [Mayflower] -> Bool
  pLenMinusNothing xs = length (dropNothing xs) == length xs - (countNothing xs) 

  -- dropNothing on xs without Nothing = dropNothing xs
  pNothingEq :: [Mayflower] -> Bool
  pNothingEq xs = 
    let noN   = filter minusNothing xs
    in dropNothing noN == dropNothing xs

  -- dropNothing on the Nothings in xs only == []
  pNothingNul :: [Mayflower] -> Bool
  pNothingNul xs =
    let onlyN = filter (not . minusNothing) xs
    in  null $ dropNothing onlyN 

  testNothing :: [Mayflower] -> Property
  testNothing xs = con xs ==> pLenMinusNothing xs && 
                              pNothingEq       xs &&
                              pNothingNul      xs 

  minusNothing :: Mayflower -> Bool
  minusNothing x = case fst x of
                     Nothing -> False
                     Just _  -> True
  

  countNothing :: [Mayflower] -> Int
  countNothing = foldl' ign 0
    where ign acc x = case fst x of
                        Nothing -> 1 + acc
                        Just _  ->     acc

  main :: IO ()
  main = deepCheck testNothing
  
