{-# LANGUAGE BangPatterns #-}
module Network.Mom.Patterns (
          -- * Service Access Point
          AccessPoint(..),
          -- * Generic Serivce
          Service, srvName, srvContext, pause, resume,
          -- * Converters
          InBound, OutBound,
          idIn, idOut, inString, outString, inUTF8, outUTF8,
          -- * Error Handlers
          OnErrorIO, OnError, 
          tryIO,
          -- * Service Povider
          serve, withServer,
          OpenSourceIO, CloseSourceIO, 
          -- * Publisher
          publish, withPub,
          OpenSource, CloseSource,
          -- * Pipeline
          pull, withPuller,
          Pipe, withPipe, push, 
          -- * Fetchers
          Fetch, FetchHelper,
          fetcher, listFetcher,
          once, fetchFor,
          handleFetcher, fileFetcher, dbFetcher, err,
          noopen, noclose, dbExec, dbClose, fileOpen, fileClose,
          -- * Service Request
          Client, withClient, request,
          -- * Subscription
          withSub, subscribe, unsubscribe, resubscribe,
          -- * Iteratees
          store, toList, toString, append, fold,
          -- * ZMQ Context
          Z.Context, Z.withContext,
          Z.SocketOption(..),
          -- * Helpers
          Millisecond)
          
where

  import           Service

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U -- standard converters
  import           Data.Maybe (fromJust)
  import qualified Data.Enumerator      as E
  import           Data.Enumerator (($$))
  import qualified Data.Enumerator.List   as EL (head)
  import qualified Data.Enumerator.Binary as EB 
  import qualified Data.Monoid as M
  import qualified Database.HDBC as SQL

  import           Control.Concurrent 
  import           Control.Applicative ((<$>))
  import           Control.Monad
  import           Control.Monad.Trans
  import           Prelude hiding (catch)
  import           Control.Exception (bracket, catch, 
                                      AssertionFailed(..), assert,
                                      throwIO, SomeException, try)

  import qualified System.ZMQ as Z
  import qualified System.IO  as IO
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
  ------------------------------------------------------------------------
  idIn :: InBound B.ByteString
  idIn = return

  idOut :: OutBound B.ByteString
  idOut = return

  outUTF8 :: OutBound String
  outUTF8 = return . U.fromString

  inUTF8 :: InBound String
  inUTF8 = return . U.toString

  outString :: OutBound String
  outString = return . B.pack

  inString :: InBound String
  inString = return . B.unpack

  data AccessPoint = Address {
                       acAdd :: String,
                       acOs  :: [Z.SocketOption]}
  
  instance Show AccessPoint where
    show (Address s _) = s

  type Fetch s o = Z.Context -> s -> E.Enumerator o IO ()
  type FetchHelper   s o = Z.Context -> s -> IO (Maybe o)

  type Work s i = Z.Context -> s -> E.Iteratee i IO ()
  type WorkHelper s i = Z.Context -> s -> IO (Maybe i)

  type OpenSourceIO  i s = Z.Context -> i -> IO s
  type CloseSourceIO i s = Z.Context -> i -> s -> IO ()

  type OpenSource      s = Z.Context      -> IO s
  type CloseSource     s = Z.Context -> s -> IO ()

  type OnErrorIO s i o = SomeException                 -> 
                         String                        ->
                         Maybe s -> Maybe i -> Maybe o -> 
                         IO (Maybe B.ByteString) 

  type OnError   s a  = SomeException ->
                         String        ->
                         Maybe s -> Maybe a -> IO (Maybe B.ByteString)

  ------------------------------------------------------------------------
  -- | Server 
  ------------------------------------------------------------------------
  withServer :: Z.Context -> String -> String -> Int ->
                AccessPoint -> Maybe AccessPoint ->
                InBound i -> OutBound o ->
                OnErrorIO s i o ->
                (String -> OpenSourceIO  i s) ->
                (String -> Fetch s o) -> 
                (String -> CloseSourceIO i s) -> 
                (Service -> IO ()) -> IO ()
  withServer ctx name param n ext int iconv oconv onerr openS fetch closeS action =
    withService ctx name param service action
    where service = serve_ True n ext int iconv oconv onerr 
                           openS fetch closeS

  ------------------------------------------------------------------------
  -- | Provide a Service
  ------------------------------------------------------------------------
  serve :: Z.Context         -> String -> Int               ->
           AccessPoint       -> Maybe AccessPoint ->
           InBound i         -> OutBound o        ->
           OnErrorIO   s i o ->
           OpenSourceIO  i s ->
           Fetch s o -> 
           CloseSourceIO i s -> IO ()
  serve ctx name n ext int iconv oconv onErr openS fetch closeS =
    serve_ False n ext int iconv oconv onErr 
           (\_ -> openS) (\_ -> fetch) (\_ -> closeS)
           ctx name "" "" 

  ------------------------------------------------------------------------
  -- the server implementation
  ------------------------------------------------------------------------
  serve_ :: Bool -> Int       ->
            AccessPoint       -> Maybe AccessPoint ->
            InBound i         -> OutBound o        ->
            OnErrorIO   s i o ->
            (String -> OpenSourceIO  i s) ->
            (String -> Fetch s o) -> 
            (String -> CloseSourceIO i s) -> 
            Z.Context -> String -> String -> String -> IO ()
  serve_ controlled n ext int iconv oconv onerr 
         openS fetch closeS ctx name sockname param
  ------------------------------------------------------------------------
  -- prepare service for single client
  ------------------------------------------------------------------------
    | n <= 1 =
      Z.withSocket ctx Z.Rep $ \client -> do
        Z.bind client (acAdd ext)
        if controlled
          then Z.withSocket ctx Z.Sub $ \cmd -> do
                 Z.connect cmd sockname
                 poll False [Z.S cmd Z.In, Z.S client Z.In] (go client) param
          else forever $ go client ""
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
            if controlled
              then Z.withSocket ctx Z.Sub $ \cmd -> do
                     Z.connect cmd sockname
                     poll False [Z.S cmd Z.In, Z.S worker Z.In] (go worker) param
              else forever $ go worker "" -- catch and do something...
  ------------------------------------------------------------------------
  -- receive requests and do the job
  ------------------------------------------------------------------------
          go worker param = do
              m   <- Z.receive worker []
              ei  <- try $ iconv m
              ifLeft ei
                (\e -> handle worker e Nothing Nothing Nothing) $ \i -> 
                bracket (openS param ctx i `catch` (\e -> do
                                 handle worker e Nothing (Just i) Nothing
                                 throwIO e))
                        (closeS param ctx i)
                        (\s -> do 
                           eiR <- E.run (fetch param ctx s $$ 
                                         itFetch worker oconv)
                           ifLeft eiR
                             (\e -> handle worker e (Just s) (Just i) Nothing)
                             (\_ -> return ())) 
  ------------------------------------------------------------------------
  -- generic error handler
  ------------------------------------------------------------------------
          handle sock e mbs mbi mbo = 
            onerr e name mbs mbi mbo >>= \mbX ->
              case mbX of
                Nothing -> 
                  Z.send sock B.empty []
                Just x  -> do 
                  Z.send sock x [Z.SndMore]
                  Z.send sock B.empty []

  ------------------------------------------------------------------------
  -- | withPublisher
  ------------------------------------------------------------------------
  withPub :: Z.Context          -> 
             String -> String   ->
             Millisecond        ->
             AccessPoint        -> 
             OutBound o         ->
             OnError        s o ->
             (String -> OpenSource     s)   ->
             (String -> Fetch  s o) -> 
             (String -> CloseSource    s)   -> 
             (Service -> IO ()) -> IO ()
  withPub ctx name param period ac oconv onerr openS fetch closeS action =
    withService ctx name param service action
    where service = publish_ True period ac oconv onerr openS fetch closeS

  publish_ :: Bool ->
              Millisecond        ->
              AccessPoint        -> 
              OutBound o         ->
              OnError        s o ->
              (String -> OpenSource     s  )   ->
              (String -> Fetch  s o) -> 
              (String -> CloseSource    s  )   -> 
              Z.Context -> String -> String -> String -> IO ()
  publish_ controlled period ac oconv onerr openS fetch closeS ctx name sockname param =
    Z.withSocket ctx Z.Pub $ \sock -> do
      Z.bind sock (acAdd ac)
      if controlled
        then Z.withSocket ctx Z.Sub $ \cmd -> do
               Z.connect cmd sockname
               periodicSend False period cmd (go sock) param
        else periodic period $ go sock param
  ------------------------------------------------------------------------
  -- do the job periodically
  ------------------------------------------------------------------------
    where go sock p = 
            bracket (openS p ctx `catch` (\e -> do
                                   handle sock e Nothing Nothing
                                   throwIO e))
                    (closeS p ctx)
                    (\s -> do
                       eiR <- E.run (fetch p ctx s $$ 
                                     itFetch sock oconv)
                       ifLeft eiR
                         (\e -> handle sock e (Just s) Nothing)
                         (\_ -> return ()))
  ------------------------------------------------------------------------
  -- generic error handler
  ------------------------------------------------------------------------
          handle sock e mbs mbo = 
            onerr e name mbs mbo >>= \mbX ->
              case mbX of
                Nothing -> 
                  Z.send sock B.empty []
                Just x  -> do 
                  Z.send sock x [Z.SndMore]
                  Z.send sock B.empty []

  ------------------------------------------------------------------------
  -- | Publish
  ------------------------------------------------------------------------
  publish :: Z.Context          -> 
             String             ->
             Millisecond        ->
             AccessPoint        -> 
             OutBound o         ->
             OnError        s o ->
             OpenSource     s   ->
             Fetch  s o -> 
             CloseSource    s   -> IO ()
  publish ctx name period ac oconv onErr openS fetch closeS = 
    publish_ False period ac oconv onErr 
             (\_ -> openS ) (\_ -> fetch ) (\_ -> closeS) 
             ctx name "" ""

  ------------------------------------------------------------------------
  -- | Pipeline, Pull
  ------------------------------------------------------------------------
  withPuller :: Z.Context ->
                String    -> String ->
                AccessPoint ->
                InBound i   ->
                OnError   s i ->
                (String -> OpenSource s)  ->
                (String -> s -> E.Iteratee i IO ()) -> 
                (String -> CloseSource s) -> 
                (Service -> IO ()) -> IO ()
  withPuller ctx name param ac iconv onerr openS work closeS action =
    withService ctx name param service action
    where service = pull_ True ac iconv onerr openS work closeS 

  pull :: Z.Context ->
          String    -> 
          AccessPoint ->
          InBound i   ->
          OnError   s i ->
          OpenSource s  ->
          (s -> E.Iteratee i IO ()) -> 
          CloseSource s -> IO ()
  pull ctx name ac iconv onerr openS work closeS =
    pull_ False ac iconv onerr 
          (\_ -> openS) (\_ -> work) (\_ -> closeS) ctx name "" ""

  pull_ :: Bool        ->
           AccessPoint ->
           InBound i   ->
           OnError s i ->
           (String -> OpenSource s) ->
           (String -> s -> E.Iteratee i IO ()) ->
           (String -> CloseSource s) -> 
           Z.Context -> String -> String -> String -> IO ()
  pull_ controlled ac iconv onerr openS work closeS ctx name sockname param =
    Z.withSocket ctx Z.Pull $ \sock -> do
      Z.bind sock (acAdd ac)
      if controlled
        then Z.withSocket ctx Z.Sub $ \cmd -> do
               Z.connect cmd sockname
               poll False [Z.S cmd Z.In, Z.S sock Z.In] (go sock) param
        else forever $ go sock param
  ------------------------------------------------------------------------
  -- do the job 
  ------------------------------------------------------------------------
    where go sock p = 
            bracket (openS p ctx `catch` (\e -> do
                                   handle sock e Nothing Nothing
                                   throwIO e))
                    (closeS p ctx)
                    (\s -> do
                       eiR <- E.run (rcvEnum sock iconv $$ work p s) 
                       ifLeft eiR
                         (\e -> handle sock e (Just s) Nothing)
                         (\_ -> return ()))
  ------------------------------------------------------------------------
  -- generic error handler
  ------------------------------------------------------------------------
          handle sock e mbs mbi = -- equal for publisher, pull 
            onerr e name mbs mbi >>= \mbX ->
              case mbX of
                Nothing -> 
                  Z.send sock B.empty []
                Just x  -> do 
                  Z.send sock x [Z.SndMore]
                  Z.send sock B.empty []
 
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
  -- standard OpenSource / CloseSource
  ------------------------------------------------------------------------
  noopen :: OpenSourceIO i ()
  noopen _ _ = return ()

  noclose :: CloseSourceIO i ()
  noclose _ _ = return

  dbExec :: SQL.Statement -> OpenSourceIO [SQL.SqlValue] SQL.Statement
  dbExec s _ keys = 
    liftIO (SQL.execute s keys) >>= (\_ -> return s)

  dbClose :: CloseSourceIO [SQL.SqlValue] SQL.Statement
  dbClose _ _ _ = return ()

  fileOpen :: OpenSourceIO FilePath IO.Handle
  fileOpen _ f = IO.openFile f IO.ReadMode

  fileClose :: CloseSourceIO FilePath IO.Handle
  fileClose _ _ h = IO.hClose h 

  ------------------------------------------------------------------------
  -- standard enumerators
  ------------------------------------------------------------------------
  fetcher :: FetchHelper s o -> Fetch s o 
  fetcher fetch ctx s step =
    case step of
      (E.Continue k) -> tryIO (fetch ctx s) $ \mbo ->
        case mbo of 
          Nothing -> E.continue k
          Just o  -> fetcher fetch ctx s $$ k (E.Chunks [o]) 
      _ -> E.returnI step

  once :: FetchHelper s o -> Fetch s o
  once = go True 
    where go first fetch ctx s step =
            case step of
              (E.Continue k) -> 
                if first then tryIO (fetch ctx s) $ \mbX ->
                  case mbX of
                    Nothing -> E.continue k 
                    Just x  -> go False fetch ctx s $$ k (E.Chunks [x])
                else E.continue k
              _ -> E.returnI step

  listFetcher :: Fetch [o] o 
  listFetcher ctx l step =
    case step of
      (E.Continue k) -> do
        if null l then E.continue k
                  else listFetcher ctx (tail l) $$ k (E.Chunks [head l])
      _ -> E.returnI step

  fetchFor :: (Z.Context -> Int -> IO o) -> Fetch (Int, Int) o
  fetchFor fetch c (i,e) step =
    case step of
      (E.Continue k) -> do
         if i == e then E.continue k
                   else tryIO (fetch c i) $ \x -> 
                     fetchFor fetch c (i+1, e) $$ k (E.Chunks [x])
      _ -> E.returnI step

  dbFetcher :: Fetch SQL.Statement [SQL.SqlValue]
  dbFetcher c s step = do
    case step of
      (E.Continue k) -> tryIO (SQL.fetchRow s) $ \mbr ->
         case mbr of
           Nothing -> E.continue k
           Just r  -> dbFetcher c s $$ k (E.Chunks [r]) 
      _ -> E.returnI step

  fileFetcher :: Fetch IO.Handle B.ByteString 
  fileFetcher = handleFetcher 4096

  handleFetcher :: Integer -> Fetch IO.Handle B.ByteString
  handleFetcher bufSize _ h = EB.enumHandle bufSize h

  err :: Fetch s o
  err _ _ s = do
    ei <- liftIO $ catch (do _ <- throwIO $ AssertionFailed "Test"
                             return $ Right ())
                         (\e -> return $ Left e)
    case ei of
      Left e  -> E.returnI (E.Error e)
      Right _ -> E.returnI s

  ------------------------------------------------------------------------
  -- Client data type
  ------------------------------------------------------------------------
  data Client i o = Client {
                       cliCtx  :: Z.Context,
                       cliSock :: Z.Socket Z.Req,
                       cliAdd  :: AccessPoint,
                       cliOut  :: OutBound o,
                       cliIn   :: InBound  i
                 }

  ------------------------------------------------------------------------
  -- Create a Client
  ------------------------------------------------------------------------
  withClient :: Z.Context  -> AccessPoint -> 
                 OutBound o -> InBound i   -> 
                 (Client i o -> IO a)     -> IO a
  withClient ctx ac oconv iconv act = Z.withSocket ctx Z.Req $ \s -> do 
    Z.connect s (acAdd ac)
    act Client {
        cliCtx  = ctx,
        cliSock = s,
        cliAdd  = ac,
        cliOut  = oconv,
        cliIn   = iconv}

  ------------------------------------------------------------------------
  -- request 
  ------------------------------------------------------------------------
  request :: Client i o -> o -> E.Iteratee i IO a -> IO (Either SomeException a) 
  request c o it = tryout ?> trysend ?> receive
    where tryout    = try $ (cliOut c) o
          trysend x = try $ Z.send (cliSock c) x [] 
          receive _ = E.run (rcvEnum (cliSock c) (cliIn c) $$ it)

  ------------------------------------------------------------------------
  -- Subscription
  ------------------------------------------------------------------------
  type SubError = SomeException -> String -> String -> IO ()

  withSub :: Z.Context -> 
             String -> String -> String -> Millisecond -> AccessPoint -> 
             InBound i   -> SubError ->
             (String -> E.Iteratee i IO ()) -> (Service -> IO ()) -> IO ()
  withSub ctx name sub param period ac iconv onErr it action = 
    withService ctx name param service action
    where service = subscribe_ True sub period ac iconv onErr it

  subscribe_ :: Bool -> String -> 
                Millisecond -> AccessPoint -> 
                InBound i   -> 
                SubError -> 
                (String -> E.Iteratee i IO ()) -> 
                Z.Context -> String -> String -> String -> IO ()
  subscribe_ controlled sub period ac iconv onErr it ctx name sockname param = 
    Z.withSocket ctx Z.Sub $ \sock -> do
      Z.connect sock (acAdd ac)
      -- throwIO (AssertionFailed "test")
      Z.subscribe sock sub
      if controlled
        then Z.withSocket ctx Z.Sub $ \cmd -> do
               Z.connect cmd sockname
               poll False [Z.S cmd Z.In, Z.S sock Z.In] (rcv sock) param
        else forever $ rcv sock "" 
    where rcv :: Z.Socket a -> String -> IO ()
          rcv s p = do E.run (rcvEnum s iconv $$ it p) >>= \ei ->
                         ifLeft ei (\e -> onErr e name sub)
                                   (\_ -> return ())

  subscribe :: Z.Context -> String -> String -> 
               Millisecond -> AccessPoint -> 
               InBound i   -> 
               SubError -> 
               E.Iteratee i IO () -> IO ()
  subscribe ctx name sub period ac iconv onerr it = 
    subscribe_ False sub period ac iconv onerr (\_ -> it) ctx name "" ""

  unsubscribe :: Service -> IO ()
  unsubscribe = pause

  resubscribe :: Service -> IO ()
  resubscribe = resume

  ------------------------------------------------------------------------
  -- Pipeline
  ------------------------------------------------------------------------
  data Pipe o = Pipe {
                  pipCtx  :: Z.Context,
                  pipSock :: Z.Socket Z.Push,
                  pipAdd  :: AccessPoint,
                  pipOut  :: OutBound o
                }

  withPipe :: Z.Context  -> AccessPoint -> 
              OutBound o -> 
              (Pipe o -> IO ())     -> IO ()
  withPipe ctx ac oconv act = Z.withSocket ctx Z.Push $ \s -> do 
    Z.connect s (acAdd ac)
    act Pipe {
        pipCtx  = ctx,
        pipSock = s,
        pipAdd  = ac,
        pipOut  = oconv}

  push :: Pipe o -> E.Enumerator o IO () -> IO (Either SomeException ()) 
  push p enum = E.run (enum $$ itFetch (pipSock p) (pipOut p))

  ------------------------------------------------------------------------
  -- receive 
  ------------------------------------------------------------------------
  rcvEnum :: Z.Socket a -> InBound i -> E.Enumerator i IO b
  rcvEnum s iconv step = 
    case step of 
      E.Continue k -> do
        x    <- liftIO $ Z.receive s []
        more <- liftIO $ Z.moreToReceive s
        if more
          then do
            i <- liftIO $ iconv x
            rcvEnum s iconv $$ k (E.Chunks [i])
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

  ------------------------------------------------------------------------
  -- queue
  -- forwarder
  -- publish
  -- subscribe
  -- push
  -- pull
  -- peer
  ------------------------------------------------------------------------

  ------------------------------------------------------------------------
  -- some helpers
  ------------------------------------------------------------------------
  ifLeft :: Either a b -> (a -> c) -> (b -> c) -> c
  ifLeft e l r = either l r e

  tryIO :: IO a -> (a -> E.Iteratee b IO ()) -> E.Iteratee b IO ()
  tryIO x f = liftIO (try x) >>= \ei ->
                case ei of
                  Left  e -> E.returnI (E.Error e)
                  Right y -> f y

  eiCombine :: IO (Either a b) -> (b -> IO (Either a c)) -> IO (Either a c)
  eiCombine x f = x >>= \mbx ->
                  case mbx of
                    Left  e -> return $ Left e
                    Right y -> f y

  infixl 9 ?>
  (?>) :: IO (Either a b) -> (b -> IO (Either a c)) -> IO (Either a c)
  (?>) = eiCombine

