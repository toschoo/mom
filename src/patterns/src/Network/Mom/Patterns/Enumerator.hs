{-# LANGUAGE BangPatterns, ExistentialQuantification #-}
-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns/Enumerator.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
-- 
-- Enumerators for basic patterns
-------------------------------------------------------------------------------
module Network.Mom.Patterns.Enumerator (
          -- * Enumerators
          Fetch_, Fetch, FetchHelper,
          fetcher, listFetcher,
          once, just, -- getFor
          fetch1, fetchFor, err,
          -- * Iteratees
          Dump, sink, sinkI, sinkLess, store,
          one, mbOne, toList, toString, append, fold)
where

  import           Types

  import qualified Data.Enumerator        as E
  import           Data.Enumerator (($$))
  import qualified Data.Enumerator.List   as EL (head)
  import qualified Data.Monoid            as M

  import           Control.Applicative ((<$>))
  import           Control.Monad
  import           Control.Monad.Trans
  import           Prelude hiding (catch)
  import           Control.Exception (AssertionFailed(..), catch, throwIO)

  import qualified System.ZMQ as Z

  ------------------------------------------------------------------------
  -- standard enumerators
  ------------------------------------------------------------------------
  fetcher :: FetchHelper i o -> Fetch i o 
  fetcher fetch ctx p i step =
    case step of
      (E.Continue k) -> chainIOe (fetch ctx p i) $ \mbo ->
        case mbo of 
          Nothing -> E.continue k
          Just o  -> fetcher fetch ctx p i $$ k (E.Chunks [o]) 
      _ -> E.returnI step

  fetch1 :: FetchHelper i o -> Fetch i o
  fetch1 = go True 
    where go first fetch ctx p i step =
            case step of
              (E.Continue k) -> 
                if first then chainIOe (fetch ctx p i) $ \mbX ->
                  case mbX of
                    Nothing -> E.continue k 
                    Just x  -> go False fetch ctx p i $$ k (E.Chunks [x])
                else E.continue k
              _ -> E.returnI step

  once :: (i -> IO (Maybe o)) -> i -> E.Enumerator o IO ()
  once = go True
    where go first get i step =
            case step of
              (E.Continue k) -> 
                if first then chainIOe (get i) $ \mbX ->
                  case mbX of
                    Nothing -> E.continue k 
                    Just x  -> go False get i $$ k (E.Chunks [x])
                else E.continue k
              _ -> E.returnI step

  just :: o -> E.Enumerator o IO ()
  just = go True
    where go first i step = 
            case step of
              E.Continue k -> 
                if first then go False i $$ k (E.Chunks [i])
                  else E.continue k
              _ -> E.returnI step
              

  listFetcher :: [o] -> Fetch_ o 
  listFetcher l ctx p _ step =
    case step of
      (E.Continue k) -> do
        if null l then E.continue k
                  else listFetcher (tail l) ctx p () $$ k (E.Chunks [head l])
      _ -> E.returnI step

  fetchFor :: (Z.Context -> String -> Int -> IO o) -> (Int, Int) -> Fetch () o
  fetchFor fetch (i,e) c p _ step =
    case step of
      (E.Continue k) -> do
         if i >= e then E.continue k
                   else chainIOe (fetch c p i) $ \x -> 
                     fetchFor fetch (i+1, e) c p () $$ k (E.Chunks [x])
      _ -> E.returnI step

  err :: Fetch_ o
  err _ _ _ s = do
    ei <- liftIO $ catch 
            (throwIO (AssertionFailed "Test") >>= \_ -> return $ Right ())
            (\e -> return $ Left e)
    case ei of
      Left e  -> E.returnI (E.Error e)
      Right _ -> E.returnI s


  ------------------------------------------------------------------------
  -- standard iteratees
  ------------------------------------------------------------------------
  sink :: (Z.Context -> String ->           IO s ) -> 
          (Z.Context -> String -> s ->      IO ()) -> 
          (Z.Context -> String -> s -> i -> IO ()) -> Dump i
  sink op cl save ctx p = go Nothing
    where go mbs = E.catchError (body mbs) (onerr mbs)
          body mbs = do
            s <- case mbs of
                   Nothing -> tryIO $ op ctx p
                   Just s  -> return s
            mbi <- EL.head
            case mbi of
              Nothing -> tryIO (cl ctx p s)
              Just i  -> tryIO (save ctx p s i) >> go (Just s)
          onerr mbs e =  case mbs of
                           Nothing -> E.throwError e
                           Just s  -> tryIO (cl ctx p s) >> E.throwError e

  sinkI :: (Z.Context -> String ->      i -> IO s ) -> 
           (Z.Context -> String -> s      -> IO ()) -> 
           (Z.Context -> String -> s -> i -> IO ()) -> Dump i
  sinkI op cl save ctx p = go Nothing
    where go mbs = E.catchError (body mbs) (onerr mbs)
          body mbs = do
            mbi <- EL.head
            case mbi of
              Nothing -> case mbs of
                           Nothing -> return ()
                           Just s  -> tryIO (cl ctx p s)
              Just i  -> case mbs of
                           Nothing -> Just <$> tryIO (op ctx p i) >>= go
                           Just s  -> tryIO (save ctx p s i) >> go (Just s)
          onerr mbs e =  case mbs of
                           Nothing -> E.throwError e
                           Just s  -> tryIO (cl ctx p s) >> E.throwError e

  sinkLess :: (Z.Context -> String -> i -> IO ()) -> Dump i
  sinkLess save ctx p = store (save ctx p)

  store :: (i -> IO ()) -> E.Iteratee i IO ()
  store save = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> tryIO (save i) >> store save

  one :: i -> E.Iteratee i IO i
  one x = do
    mbi <- EL.head
    case mbi of
      Nothing -> return x
      Just i  -> return i

  mbOne :: E.Iteratee i IO (Maybe i)
  mbOne = EL.head

  ------------------------------------------------------------------------
  -- the following iteratees cause space leaks!
  ------------------------------------------------------------------------
  toList :: E.Iteratee i IO [i]
  toList = do
    mbi <- EL.head
    case mbi of
      Nothing -> return []
      Just i  -> do
        is <- toList
        return (i:is)

  toString :: String -> E.Iteratee String IO String
  toString s = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ""
      Just i  -> do
        is <- toString s
        return $ concat [i, s, is]

  append :: M.Monoid i => E.Iteratee i IO i
  append = do
    mbi <- EL.head
    case mbi of
      Nothing -> return M.mempty
      Just i  -> do
        is <- append
        return (i `M.mappend` is)

  fold :: (i -> a -> a) -> a -> E.Iteratee i IO a
  fold f acc = do
    mbi <- EL.head
    case mbi of
      Nothing -> return acc
      Just i  -> do
        is <- fold f acc
        return (f i is)
