{-# LANGUAGE BangPatterns #-}
module Network.Mom.Patterns (
          -- * Service Access Point
          AccessPoint(..),
          -- * Converters
          InBound, OutBound,
          -- * Error Handlers
          OnErrorIO, OnErrorEnum,
          -- * Service Povider
          serveIt, 
          OpenSource, CloseSource, 
          FetchResponse, FetchHelper,
          fetcher, listFetcher,
          -- fileFetcher, dbFetcher,
          -- fileOpen, fileClose
          -- dbExec, dbClose, 
          -- noopen, noclose,
          -- * Service Request
          Service, withService, request,
          store, toList, toString, append, fold,
          -- * ZMQ Context
          Z.Context, Z.withContext)
          
where

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U -- standard converters
  import           Data.Maybe (fromJust)
  import qualified Data.Enumerator      as E
  import           Data.Enumerator (($$))
  import qualified Data.Enumerator.List as EL (head)
  import qualified Data.Monoid as M

  import           Control.Concurrent 
  import           Control.Applicative ((<$>))
  import           Control.Monad
  import           Control.Monad.Trans
  import           Prelude hiding (catch)
  import           Control.Exception (bracket, catch,
                                      throwIO, SomeException, try)

  import qualified System.ZMQ as Z
  -- import           System.Posix.Signals

    ------------------------------------------------------------------------
  -- | Converters are user-defined actions passed to 
  --   'newReader' ('InBound') and
  --   'newWriter' ('OutBound')
  --   that convert a 'B.ByteString' to a value of type /a/ ('InBound') or
  --                a value of type /a/ to 'B.ByteString' ('OutBound'). 
  --   Converters are, hence, similar to /put/ and /get/ in the /Binary/
  --   monad. 
  --
  --   The reason for using explicit, user-defined converters 
  --   instead of /Binary/ /encode/ and /decode/
  --   is that the conversion with queues
  --   may be much more complex, involving reading configurations 
  --   or other 'IO' actions.
  --   Furthermore, we have to distinguish between data types and 
  --   there binary encoding when sent over the network.
  --   This distinction is made by /MIME/ types.
  --   Two applications may send the same data type,
  --   but one encodes this type as \"text/plain\",
  --   the other as \"text/xml\".
  --   'InBound' conversions have to consider the /MIME/ type
  --   and, hence, need more input parameters than provided by /decode/.
  --   /encode/ and /decode/, however,
  --   can be used internally by user-defined converters.
  --
  --   The parameters expected by an 'InBound' converter are:
  --
  --     * the /MIME/ type of the content
  --
  --     * the content size 
  --
  --     * the list of 'F.Header' coming with the message
  --
  --     * the contents encoded as 'B.ByteString'.
  --
  --   The simplest possible in-bound converter for plain strings
  --   may be created like this:
  --
  --   > let iconv _ _ _ = return . toString
  ------------------------------------------------------------------------
  type InBound  a = B.ByteString -> IO a
  ------------------------------------------------------------------------
  -- | Out-bound converters are much simpler.
  --   Since the application developer knows,
  --   which encoding to use, the /MIME/ type is not needed.
  --   The converter receives only the value of type /a/
  --   and converts it into a 'B.ByteString'.
  --   A simple example to create an out-bound converter 
  --   for plain strings could be:
  --
  --   > let oconv = return . fromString
  ------------------------------------------------------------------------
  type OutBound a = a -> IO B.ByteString

  ------------------------------------------------------------------------
  -- standard converters:
  -- - id (ByteString)
  -- - toString / fromString
  -- - toUTF8 / fromUTF8
  ------------------------------------------------------------------------

  data AccessPoint = Address {
                       acAdd :: String,
                       acOs  :: [Z.SocketOption]}
  
  instance Show AccessPoint where
    show (Address s _) = s

  type OnError   a   = IO (Maybe a)
  type OnErrorIO i o = SomeException -> Maybe i -> Maybe o -> IO (Maybe B.ByteString)
  type OnErrorIn i   = SomeException -> Maybe i -> IO i
  type OnErrorOut  o = Maybe o -> IO o

  type Response i o = Z.Context -> i -> IO o
             
  serve :: Z.Context     -> Int               ->
           AccessPoint   -> Maybe AccessPoint ->
           InBound i     -> OutBound o        ->
           OnErrorIO i o -> Response i o      -> IO ()
  serve ctx n ext int iconv oconv onErr work 
    | n <= 1 =
      Z.withSocket ctx Z.Rep $ \client -> do
        Z.bind client (acAdd ext)
        forever $ do
          m   <- Z.receive client []
          mbR <- doWork ctx iconv oconv onErr work m
          case mbR of
            Nothing -> return ()
            Just r  -> Z.send client r []
    | otherwise = do
      Z.withSocket ctx Z.XRep $ \clients -> do
        Z.bind clients (acAdd ext)
        Z.withSocket ctx Z.XReq $ \workers -> do
          Z.bind workers (acAdd $ fromJust int) 
          replicateM_ n (startWork $ acAdd $ fromJust int)
          Z.device Z.Queue clients workers
    where startWork add = forkIO $ Z.withSocket ctx Z.Rep $ \worker -> do
            Z.connect worker add
            forever $ do
              m   <- Z.receive worker []
              mbO <- doWork ctx iconv oconv onErr work m
              case mbO of
                Nothing -> return ()
                Just o  -> Z.send worker o []

  doWork :: Z.Context                     ->
            InBound i     -> OutBound o   ->
            OnErrorIO i o -> Response i o -> 
            B.ByteString  -> IO (Maybe B.ByteString)
  doWork ctx iconv oconv onErr work m = do
    ei <- try $ iconv m
    case ei of
      Left  e -> onErr e Nothing Nothing
      Right i -> do
        eiO <- try $ work ctx i
        case eiO of
          Left  e -> onErr e (Just i) Nothing
          Right o -> do
            eiO' <- try $ oconv o
            case eiO' of
              Left  e  -> onErr e (Just i) (Just o)
              Right o' -> return  (Just o')

  type FetchResponse s o = Z.Context -> s -> E.Enumerator o IO ()
  type FetchHelper   s o = Z.Context -> s -> IO (Maybe o)
  type OpenSource    i s = Z.Context -> i -> IO s
  type CloseSource   i s = Z.Context -> i -> s -> IO ()

  type OnErrorEnum s i o = SomeException                 -> 
                           Maybe s -> Maybe i -> Maybe o -> 
                           IO (Maybe B.ByteString) 

  serveIt :: Z.Context         -> Int               ->
             AccessPoint       -> Maybe AccessPoint ->
             InBound i         -> OutBound o        ->
             OnErrorEnum s i o ->
             OpenSource    i s ->
             FetchResponse s o -> 
             CloseSource   i s -> IO ()

  ------------------------------------------------------------------------
  -- prepare service for single client
  ------------------------------------------------------------------------
  serveIt ctx n ext int iconv oconv onErr openS fetch closeS
    | n <= 1 =
      Z.withSocket ctx Z.Rep $ \client -> do
        Z.bind client (acAdd ext)
        go client

  ------------------------------------------------------------------------
  -- prepare service for multiple clients 
  ------------------------------------------------------------------------
    | otherwise = 
      Z.withSocket ctx Z.XRep $ \clients -> do
        Z.bind clients (acAdd ext)
        Z.withSocket ctx Z.XReq $ \workers -> do
          Z.bind workers (acAdd $ fromJust int) 
          replicateM_ n (forkIO $ startWork $ acAdd $ fromJust int)
          Z.device Z.Queue clients workers

  ------------------------------------------------------------------------
  -- start worker for multiple clients 
  ------------------------------------------------------------------------
    where startWork add = Z.withSocket ctx Z.Rep $ \worker -> do
            Z.connect worker add
            go worker

  ------------------------------------------------------------------------
  -- receive requests and do the job
  ------------------------------------------------------------------------
          go worker = forever $ do
              m   <- Z.receive worker []
              ei  <- try $ iconv m
              ifLeft ei
                (\e -> handle e worker Nothing Nothing Nothing) $ \i -> 
                bracket (openS ctx i `catch` (\e -> do
                                        handle e worker 
                                                 Nothing (Just i) Nothing
                                        throwIO e))
                        (closeS ctx i)
                        (\s -> do 
                           eiR <- E.run (fetch ctx s $$ 
                                         itFetch worker oconv)
                           ifLeft eiR
                             (\e -> handle e worker (Just s) (Just i) Nothing)
                             (\_ -> return ())) 
                                    
  ------------------------------------------------------------------------
  -- generic error handler
  ------------------------------------------------------------------------
          handle e sock mbs mbi mbo = 
            onErr e mbs mbi mbo >>= \mbX ->
              case mbX of
                Nothing -> return ()
                Just x  -> Z.send sock x []
 
  ------------------------------------------------------------------------
  -- sending iteratee 
  ------------------------------------------------------------------------
  itFetch :: Z.Socket a -> OutBound o -> E.Iteratee o IO ()
  itFetch s oconv = do
    mbO <- EL.head
    case mbO of
      Nothing -> liftIO $ Z.send s (B.pack "END") []
      Just o  -> do
        x <- liftIO $ oconv o
        liftIO $ Z.send s x [Z.SndMore]
        itFetch s oconv

  ------------------------------------------------------------------------
  -- standard enumerator
  ------------------------------------------------------------------------
  fetcher :: (Z.Context -> s -> IO (Maybe o)) -> FetchResponse s o 
  fetcher fetch ctx s step =
    case step of
      (E.Continue k) -> do
        stp <- liftIO $ fetch ctx s
        case stp of 
          Nothing -> E.continue k
          Just o  -> fetcher fetch ctx s $$ k (E.Chunks [o]) 
      _ -> E.returnI step

  listFetcher :: FetchResponse [o] o 
  listFetcher ctx l step =
    case step of
      (E.Continue k) -> do
        if null l then E.continue k
                  else listFetcher ctx (tail l) $$ k (E.Chunks [head l])
      _ -> E.returnI step

  -- file fetcher
  -- db open, close, fetcher
  -- empty open, close 

  ------------------------------------------------------------------------
  -- Service data type
  ------------------------------------------------------------------------
  data Service i o = Service {
                       srvCtx  :: Z.Context,
                       srvSock :: Z.Socket Z.Req,
                       srvAdd  :: AccessPoint,
                       srvOut  :: OutBound o,
                       srvIn   :: InBound  i
                 }

  ------------------------------------------------------------------------
  -- Create a Service
  ------------------------------------------------------------------------
  withService :: Z.Context  -> AccessPoint -> 
                 OutBound o -> InBound i   -> 
                 (Service i o -> IO a)     -> IO a
  withService ctx ac oconv iconv act = Z.withSocket ctx Z.Req $ \s -> do 
    Z.connect s (acAdd ac)
    act Service {
        srvCtx  = ctx,
        srvSock = s,
        srvAdd  = ac,
        srvOut  = oconv,
        srvIn   = iconv}

  ------------------------------------------------------------------------
  -- request 
  ------------------------------------------------------------------------
  request :: Service i o -> o -> E.Iteratee i IO a -> IO (Either SomeException a) 
  request s o it = tryout ?> trysend ?> receive
    where tryout    = try $ (srvOut s) o
          trysend x = try $ Z.send (srvSock s) x [] 
          receive _ = E.run (reqEnum s $$ it)
    
  ------------------------------------------------------------------------
  -- receive 
  ------------------------------------------------------------------------
  reqEnum :: Service i o -> E.Enumerator i IO a
  reqEnum s step = 
    case step of 
      E.Continue k -> do
        x    <- liftIO $ Z.receive (srvSock s) []
        more <- liftIO $ Z.moreToReceive (srvSock s)
        if more
          then do
            i <- liftIO $ (srvIn s) x
            reqEnum s $$ k (E.Chunks [i])
          else E.continue k
      _ -> E.returnI step

  ------------------------------------------------------------------------
  -- standard iteratees
  ------------------------------------------------------------------------
  store :: (i -> IO ()) -> E.Iteratee i IO ()
  store save = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> liftIO (save i) >> store save

  ------------------------------------------------------------------------
  -- the following iteratees cause space leaks!
  ------------------------------------------------------------------------
  toList :: E.Iteratee i IO [i]
  toList = do
    mbi <- EL.head
    case mbi of
      Nothing -> return []
      Just i  -> do
        !is <- toList
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

  ------------------------------------------------------------------------
  -- some helpers
  ------------------------------------------------------------------------
  ifLeft :: Either a b -> (a -> c) -> (b -> c) -> c
  ifLeft e l r = either l r e

  eiCombine :: IO (Either a b) -> (b -> IO (Either a c)) -> IO (Either a c)
  eiCombine x f = x >>= \mbx ->
                  case mbx of
                    Left  e -> return $ Left e
                    Right y -> f y

  infixl 9 ?>
  (?>) :: IO (Either a b) -> (b -> IO (Either a c)) -> IO (Either a c)
  (?>) = eiCombine
