{-# LANGUAGE CPP #-}
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
          -- $enums

          -- ** Raw Enumerators
          enumWith, enumFor, once, just,
          -- ** Fetchers
          Fetch, Fetch_, 
          FetchHelper, FetchHelper',
          FetchHelper_, FetchHelper_',
          fetcher, fetcher_,
          fetch1, fetch1_,
          fetchFor, fetchFor_,
          fetchJust, fetchJust_,
          listFetcher, listFetcher_,
#ifdef _TEST
          err,
#endif
          -- * Iteratees
          -- $its

          -- ** Raw Iteratees
          one, mbOne, toList, toString, append,
          store,
          -- ** Dumps 
          Dump, sink, sinkI, nosink) 
where

  import           Types

  import qualified Data.Enumerator        as E
  import           Data.Enumerator (($$))
  import qualified Data.Enumerator.List   as EL
  import qualified Data.Monoid            as M
  import           Data.List (foldl', intercalate)

  import           Control.Applicative ((<$>))
  import           Control.Monad
  import           Prelude hiding (catch)

  import qualified System.ZMQ as Z

  ------------------------------------------------------------------------
  -- $enums
  -- Enumerators generate streams
  -- and pass chunks of the stream for further processing
  -- to Iteratees.
  -- The Enumerator-Iteratee abstraction is very powerful
  -- and is by far not discussed exhaustively here.
  -- For more details, please refer to the documentation
  -- of the Enumerator package.
  -- 
  -- The Patterns package provides a small set
  -- of enumerators that may be useful for many messaging patterns.
  -- Enumerators are split into raw enumerators,
  -- which can be used with patterns under direct control
  -- of application code such as 'Client', 'Pub' and 'Peer',
  -- and Fetchers, which are used with services, /i.e./
  -- 'withServer' and 'withPeriodicPub'.
  ------------------------------------------------------------------------
  -- | Calls an application-defined /getter/ function
  --   until this returns 'Nothing';
  --   if the getter throws an exception,
  --   the enumerator returns 'E.Error'.
  ------------------------------------------------------------------------
  enumWith :: (i -> IO (Maybe o)) -> i -> E.Enumerator o IO ()
  enumWith get i step = 
    case step of
      E.Continue k -> chainIOe (get i) $ \mbO ->
                      case mbO of
                        Nothing -> E.continue k
                        Just o  -> enumWith get i $$ k (E.Chunks [o])
      _            -> E.returnI step

  ------------------------------------------------------------------------
  -- | Calls the application-defined /getter/ function /n/ times;
  --   The enumerator receives a pair ('Int', 'Int'),
  --   where the first integer is a counter and 
  --   the second is the upper bound.
  --   /n/ is defined as /snd - fst/, /i.e./
  --   the counter is incremented until it reaches the value
  --   of the bound. The counter must be a value less than the bound
  --   to avoid protocol errors, /i.e./ the /getter/ must be called
  --   at least once.
  --   The current value of the counter and additional input
  --   are passed to the /getter/.
  --   if the getter throws an exception,
  --   the enumerator returns 'E.Error'.
  ------------------------------------------------------------------------
  enumFor :: (Int -> i -> IO o) -> (Int, Int) -> i -> E.Enumerator o IO ()
  enumFor get runner i = go runner
    where go (c,e) step = 
            case step of
              E.Continue k -> 
                if c >= e then E.continue k
                          else chainIOe (get c i) $ \o ->
                               go (c+1,e) $$ k (E.Chunks [o])
              _            -> E.returnI step

  ------------------------------------------------------------------------
  -- | Calls the application-defined /getter/ function once;
  --   the enumerator must return a value 
  --   (the result type is not 'Maybe'),
  --   otherwise, the sending iteratee has nothing to send 
  --   which would most likely result in a protocol error.
  --   if the getter throws an exception,
  --   the enumerator returns 'E.Error'.
  ------------------------------------------------------------------------
  once :: (i -> IO o) -> i -> E.Enumerator o IO ()
  once = go True
    where go first get i step =
            case step of
              E.Continue k -> 
                if first then chainIOe (get i) $ \o ->
                     go False get i $$ k (E.Chunks [o])
                else E.continue k
              _ -> E.returnI step

  ------------------------------------------------------------------------
  -- | Passes just the input value to iteratee;
  --   
  --   > just "hello world"
  --
  --   hence, reduces to just "hello world" sent over the wire.
  ------------------------------------------------------------------------
  just :: o -> E.Enumerator o IO ()
  just = go True
    where go first o step = 
            case step of
              E.Continue k -> 
                if first then go False o $$ k (E.Chunks [o])
                  else E.continue k
              _ -> E.returnI step

  ------------------------------------------------------------------------
  -- | Calls the application-defined 'FetchHelper'
  --   until it returns 'Nothing';
  --   note that the 'FetchHelper' shall return at least one 'Just'
  --   value to avoid a protocol error.
  --   If the 'FetchHelper' throws an exception,
  --   the 'fetcher' returns 'E.Error'.
  ------------------------------------------------------------------------
  fetcher :: FetchHelper i o -> Fetch i o 
  fetcher fetch ctx p i step =
    case step of
      (E.Continue k) -> chainIOe (fetch ctx p i) $ \mbo ->
        case mbo of 
          Nothing -> E.continue k
          Just o  -> fetcher fetch ctx p i $$ k (E.Chunks [o]) 
      _ -> E.returnI step

  ------------------------------------------------------------------------
  -- | A variant of 'fetcher' without input;
  ------------------------------------------------------------------------
  fetcher_ :: FetchHelper_ o -> Fetch_ o
  fetcher_ = fetcher

  ------------------------------------------------------------------------
  -- | Calls the application-defined 'FetchHelper'' once;
  --   If the 'FetchHelper'' throws an exception,
  --   the 'fetcher' returns 'E.Error'.
  ------------------------------------------------------------------------
  fetch1 :: FetchHelper' i o -> Fetch i o
  fetch1 = go True 
    where go first fetch ctx p i step =
            case step of
              (E.Continue k) -> 
                if first then chainIOe (fetch ctx p i) $ \o ->
                     go False fetch ctx p i $$ k (E.Chunks [o])
                else E.continue k
              _ -> E.returnI step

  ------------------------------------------------------------------------
  -- | A variant of 'fetch1' without input;
  ------------------------------------------------------------------------
  fetch1_ :: FetchHelper_' o -> Fetch_ o
  fetch1_ = fetch1

  ------------------------------------------------------------------------
  -- | Calls the iteratee for each element of the input list
  ------------------------------------------------------------------------
  listFetcher :: Fetch [o] o
  listFetcher ctx p os step = 
    case step of
      (E.Continue k) -> 
        if null os then E.continue k
                   else listFetcher ctx p (tail os) $$ k (E.Chunks [head os])
      _ -> E.returnI step

  ------------------------------------------------------------------------
  -- | A variant of 'listFetcher' for services without input;
  --   the list, in this case, is passed as an additional argument
  --   to the fetcher.
  ------------------------------------------------------------------------
  listFetcher_ :: [o] -> Fetch_ o
  listFetcher_ l ctx p _ step =
    case step of
      (E.Continue k) -> 
        if null l then E.continue k
                  else listFetcher_ (tail l) ctx p () $$ k (E.Chunks [head l])
      _ -> E.returnI step

  ------------------------------------------------------------------------
  -- | Calls the application-defined /getter/ /n/ times;
  --   The /getter/ is a variant of 'FetchHelper'' 
  --   with the current value of the counter as additional argument.
  --   For more details, refer to 'enumFor'.
  ------------------------------------------------------------------------
  fetchFor :: (Z.Context -> Parameter -> Int -> i -> IO o) -> 
              (Int, Int) -> Fetch i o
  fetchFor fetch (c,e) ctx p i step =
    case step of
      (E.Continue k) ->
         if c >= e then E.continue k
                   else chainIOe (fetch ctx p c i) $ \x -> 
                        fetchFor fetch (c+1, e) ctx p i $$ k (E.Chunks [x])
      _ -> E.returnI step

  ------------------------------------------------------------------------
  -- | A variant of 'fetchFor' without input
  ------------------------------------------------------------------------
  fetchFor_ :: (Z.Context -> Parameter -> Int -> () -> IO o) -> 
               (Int, Int) -> Fetch_ o
  fetchFor_ = fetchFor

  ------------------------------------------------------------------------
  -- | Passes just the input value to the iteratee;
  --   
  --   > fetchJust "hello world"
  --
  --   hence, reduces to just \"hello world\" sent over the wire.
  --   Note that the input /i/ is ignored.
  ------------------------------------------------------------------------
  fetchJust :: o -> Fetch i o
  fetchJust o _ _ _ = go True
    where go first step = 
            case step of
              (E.Continue k) ->
                if first then go False $$ k (E.Chunks [o]) -- yield?
                  else E.continue k
              _ -> E.returnI step

  ------------------------------------------------------------------------
  -- | A variant of 'fetchJust' without input
  ------------------------------------------------------------------------
  fetchJust_ :: o -> Fetch_ o
  fetchJust_ = fetchJust

  ------------------------------------------------------------------------
  -- For testing only
  ------------------------------------------------------------------------
#ifdef _TEST
  err :: Fetch_ o
  err _ _ _ s = do
    ei <- liftIO $ catch 
            (throwIO (AssertionFailed "Test") >>= \_ -> return $ Right ())
            (\e -> return $ Left e)
    case ei of
      Left e  -> E.returnI (E.Error e)
      Right _ -> E.returnI s
#endif

  ------------------------------------------------------------------------
  -- $its
  -- Iteratees process chunks of streams
  -- passed in by an enumerator.
  -- The Enumerator-Iteratee abstraction is very powerful
  -- and is by far not discussed exhaustively here.
  -- For more details, please refer to the documentation
  -- of the Enumerator package.
  -- 
  -- The Patterns package provides a small set
  -- of iteratees that may be useful for many messaging patterns.
  -- Iteratees are split into raw iteratees,
  -- which can be used with patterns under direct control
  -- of application code such as 'Client', 'Pub', 'Peer'
  -- and, for obtaining the request, 'withServer',
  -- and Dumps, which are used with services, /i.e./
  -- 'withSub' and 'withPuller'.
  ------------------------------------------------------------------------
  -- | Calls the application-defined IO action
  --   for each element of the stream;
  --   The IO action could, for instance, 
  --   write to an already opened file,
  --   store values in an 'MVar' or
  --   send them through a 'Chan' to another thread 
  --   for further processing.
  --   An exception thrown in the IO action
  --   is re-thrown by 'E.throwError'.
  ------------------------------------------------------------------------
  store :: (i -> IO ()) -> E.Iteratee i IO ()
  store save = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> tryIO (save i) >> store save

  ------------------------------------------------------------------------
  -- | Returns one value of type /i/;
  --   if the enumerator creates a value, this value is returned;
  --   otherwise, the input value is returned.
  ------------------------------------------------------------------------
  one :: i -> E.Iteratee i IO i
  one x = do
    mbi <- EL.head
    case mbi of
      Nothing -> return x
      Just i  -> return i

  ------------------------------------------------------------------------
  -- | Returns one value of type 'Maybe' /i/;
  --   equal to 'Data.Enumerator.List.head'
  ------------------------------------------------------------------------
  mbOne :: E.Iteratee i IO (Maybe i)
  mbOne = EL.head

  ------------------------------------------------------------------------
  -- | Returns a list containing all chunks of the stream;
  --   equal to 'Data.Enumerator.List.consume';
  --   note that this iteratee causes a space leak
  --   and is not suitable for huge streams or streams of unknown size.
  ------------------------------------------------------------------------
  toList :: E.Iteratee i IO [i] -- equal to EL.consume
  toList = EL.consume

  ------------------------------------------------------------------------
  -- | Returns a string containing all chunks of the stream
  --   intercalated with the input string, /e.g./:
  --   if the stream consists of the two elements \"hello\" and \"world\"
  --
  --   > toString " " 
  --
  --   returns "hello world".
  --   Note that this iteratee causes a space leak
  --   and is not suitable for huge streams or streams of unknown size.
  ------------------------------------------------------------------------
  toString :: String -> E.Iteratee String IO String 
  toString s = intercalate s <$> EL.consume

  ------------------------------------------------------------------------
  -- | Merges the elements of a stream using 'M.mappend';
  --   if the stream is empty, 'append' returns 'M.mempty'.
  --   The type /i/ must be instance of 'M.Monoid'.
  --   Note that this iteratee causes a space leak
  --   and is not suitable for huge streams or streams of unknown size.
  ------------------------------------------------------------------------
  append :: M.Monoid i => E.Iteratee i IO i
  append = foldl' M.mappend M.mempty <$> EL.consume

  ------------------------------------------------------------------------
  -- | Opens a data sink, dumps the stream into this sink
  --   and closes the sink when the stream terminates
  --   or when an error occurs;
  --   the first IO action is used to open the sink (of type /s/),
  --   the second closes the sink and
  --   the third writes one element into the sink.
  ------------------------------------------------------------------------
  sink :: (Z.Context -> String ->           IO s ) -> 
          (Z.Context -> String -> s ->      IO ()) -> 
          (Z.Context -> String -> s -> i -> IO ()) -> Dump i
  sink op cl sv ctx p = tryIO (op ctx p) >>= go
    where go s   = E.catchError (body s) (onerr s)
          body s = do
            mbi <- EL.head
            case mbi of
              Nothing -> tryIO (cl ctx p s)
              Just i  -> tryIO (sv ctx p s i) >> body s
          onerr  s e  =  tryIO (cl ctx p s)   >> E.throwError e

  ------------------------------------------------------------------------
  -- | Variant of 'sink' that uses the first segment of the stream
  --   as input parameter to open the sink.
  --   The first segment, which could contain 
  --   a file name or parameters for an /SQL/ query,
  --   is not written to the sink.
  --   As with 'sink', the sink is closed when the stream terminates or
  --   when an error occurs.
  ------------------------------------------------------------------------
  sinkI :: (Z.Context -> String ->      i -> IO s ) -> 
           (Z.Context -> String -> s      -> IO ()) -> 
           (Z.Context -> String -> s -> i -> IO ()) -> Dump i
  sinkI op cl sv ctx p = do
          mbi <- EL.head
          case mbi of
            Nothing -> return ()
            Just i  -> do
              s <- tryIO (op ctx p i)
              E.catchError (body s) (onerr s)
    where body s = do
            mbi <- EL.head
            case mbi of
              Nothing -> tryIO (cl ctx p s)
              Just i  -> tryIO (sv ctx p s i) >> body s
          onerr s e   =  tryIO (cl ctx p s)   >> E.throwError e

  ------------------------------------------------------------------------
  -- | Similar to 'sink', but uses a data sink that is opened and closed
  --   outside the scope of the service or does not need to be 
  --   opened and closed at all;
  --   examples may be services that write to 'MVar' or 'Chan'.
  --   'nosink' is implemented as a closure of 'store':
  --
  --   > nosink save ctx p = store (save ctx p)
  ------------------------------------------------------------------------
  nosink :: (Z.Context -> String -> i -> IO ()) -> Dump i
  nosink sv ctx p = store (sv ctx p)

