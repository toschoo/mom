module Test (
         TestResult(..), Stop(..),
         Test, TestGroup,
         execGroup, evaluate,
         mkTest, mkGroup, mkMetaGroup,
         (?>), (%&), (%|), neg, andR)
where

  import Control.Monad()
  import Control.Monad.Writer
  import Data.List (foldl')

  data TestResult = Fail String | Pass

  instance Eq TestResult where
    t1 == t2 = case t1 of
                 Pass -> case t2 of 
                           Pass -> True
                           _    -> False
                 _    -> case t2 of
                           Pass -> False
                           _    -> True

  instance Show TestResult where
    show  Pass    = "Pass"
    show (Fail s) = "Fail: " ++ s

  instance Read TestResult where
    readsPrec _ s = case s of
                      "Pass" -> [(Pass, "")]
                      f      -> 
                        let pre = takeWhile (/= ':') s
                            msg = drop 1 $ dropWhile (/= ':') s
                        in if pre == "Fail"
                             then [(Fail msg, "")]
                             else error $ "Can't parse TestResult: " ++ s

  data Stop = Stop {stopOn :: TestResult}
            | End

  type Test m = (String, m TestResult)

  execGroup :: Monad m => TestGroup m -> m (TestResult, String)
  execGroup g = do
    (r, txt) <- runGroup 0 g
    if r == Pass
      then return (r, good txt)
      else return (r, bad  txt)

  data TestGroup m = TLeaf {
                       tgTitle  :: String,
                       tgStop   :: Stop,
                       tgTest   :: [Test m]
                     }
                   | TTree {
                       tgTitle  :: String,
                       tgStop   :: Stop,
                       tgGroup  :: [TestGroup m]
                     }

  runGroup :: Monad m => Int -> TestGroup m -> m (TestResult, String)
  runGroup i (TLeaf t s ts) = do  
    (r, txt) <- evaluate i s ts
    return (r, mkTitle i t txt)

  runGroup i (TTree t s g) = do
    (r, txt) <- foldUntil s (i + 1) g 
    return (r, mkTitle i t txt)

  foldUntil :: Monad m => Stop -> Int -> [TestGroup m] -> m (TestResult, String)
  foldUntil _ _ [] = return (Pass, "")
  foldUntil s i (t:ts) = do
    (r, txt) <- runGroup i t
    case s of
      Stop tr -> 
        if tr == r 
          then return (r, txt)
          else continue r txt
      End ->   continue r txt
    where continue r txt = do
            (r', txt') <- foldUntil s i ts
            return (r' %& r, txt ++ "\n" ++ txt')

  type TestRunner m = WriterT String m 

  mkTest :: Monad m => String -> m TestResult -> Test m
  mkTest = (,)

  mkGroup :: Monad m => String -> Stop -> [Test m] -> TestGroup m
  mkGroup t s ts = TLeaf t s ts

  mkMetaGroup :: Monad m => String -> Stop -> [TestGroup m] -> TestGroup m
  mkMetaGroup t s gs = TTree t s gs

  evaluate :: Monad m => Int -> Stop -> [Test m] -> m (TestResult, String)
  evaluate i u ts = runWriterT (execUntil i u ts)

  execUntil :: Monad m => Int -> Stop -> [Test m] -> TestRunner m TestResult 
  execUntil _ _ [] = return $ Pass
  execUntil i End ts = do
    tt <- mapM (exec i End) ts
    return $ andR tt

  execUntil i u (x:xs) = do
    t <- exec i u x
    if stopOn u == t 
      then return t
      else do
        t' <- execUntil i u xs
        return $ t %& t'

  exec :: Monad m => Int -> Stop -> Test m -> TestRunner m TestResult
  exec i u x = do
    let s  = fst x
    let mt = snd x
    t <- lift mt
    tellLn i $ s ++ ": " ++ (show t)
    return t

  mkTitle :: Int -> String -> String -> String 
  mkTitle i t x = 
    let l = length t
        u = ind ++ map (\_ -> '-') [1..l]
    in ind ++ t ++ "\n" ++ u ++ "\n" ++ x
    where ind = indent i

  format :: String -> String -> String
  format s x = x ++ "\n" ++ s 

  good, bad :: String -> String
  bad      x = format "Bad. Some Tests failed." x
  good     x = format "Ok. All Tests passed."   x

  tellLn :: Monad m => Int -> String -> WriterT String m ()
  tellLn i = tell . (\x -> (indent i) ++ x ++ "\n") 

  indent :: Int -> String
  indent i = map (\_ -> ' ') [1..(i*2)]

  infixr 8 %&
  (%&) :: TestResult -> TestResult -> TestResult
  Pass   %& Pass     = Pass
  Pass   %& (Fail e) = Fail e
  Fail e %& _        = Fail e

  infixr 8 %|
  (%|) :: TestResult -> TestResult -> TestResult
  Fail x %| Fail y = Fail (x ++ " " ++ y)
  _      %| _      = Pass

  infixr 8 ?>
  (?>) :: Monad m => m TestResult -> m TestResult -> m TestResult
  f ?> g = do
    r <- f
    case r of
      Fail e -> return $ Fail e
      Pass   -> g

  neg :: TestResult -> TestResult
  neg  Pass    = Fail ""
  neg (Fail _) = Pass

  andR :: [TestResult] -> TestResult
  andR = foldl' (%&) Pass
