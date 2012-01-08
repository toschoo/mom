{-# LANGUAGE BangPatterns, ExistentialQuantification #-}
-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Patterns.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
-- 
-- There are common patterns often reused 
-- in many message-oriented applications,
-- such as Server\/Client (a.k.a Request\/Response),
--         Publish\/Subscribe,
--         Pipline (a.k.a. Push\/Pull) and
--         Exclusive Pair (a.k.a. Peer-to-Peer).
-- The Patterns package implements those patterns 
-- using the /zeromq/ library without using centralised brokers.
-- Please refer to <http://www.zeromq.org> for more information.
--
-- Patterns are here implemented by separation of concerns;
-- all interfaces provide means for
--
--   * Type Conversion and Consistency 
-- 
--   * Error Handling
--
--   * Resource Handling
--
--   * Fetching and Dumping results
-- 
--   * Control
-------------------------------------------------------------------------------
module Network.Mom.Patterns (
          -- * Service/Client
          serve, withServer,
          Client, withClient, 
          request, askFor, checkFor,
          -- * Publish/Subscribe
          Pub, pubContext, withPub, issue,
          publish, withPeriodicPub,
          withSub, subscribe, unsubscribe, resubscribe,
          -- * Pipeline
          pull, withPuller,
          Pipe, withPipe, push, 
          -- * Exclusive Pair
          Peer, peerContext, withPeer, send, receive,
          -- * Service Access Point
          AccessPoint(..), LinkType(..),
          -- * Converters
          InBound, OutBound,
          idIn, idOut, inString, outString, inUTF8, outUTF8,
          -- * Error Handlers
          OnError, OnError_,
          chainIO, chainIOe, tryIO, tryIOe,
          -- * Generic Serivce
          Service, srvName, srvContext, pause, resume,
          -- * Enumerators
          Fetch_, Fetch, FetchHelper,
          fetcher, listFetcher,
          once, -- getFor
          fetch1, fetchFor, err,
          -- * Iteratees
          Dump, sink, sinkI, sinkLess, store,
          one, mbOne, toList, toString, append, fold,
          -- * ZMQ Context
          Z.Context, Z.withContext,
          Z.SocketOption(..),
          -- * Helpers
          Millisecond)
where

  import           Service
  import           Factory

  import qualified Data.ByteString.Char8  as B
  import qualified Data.ByteString.UTF8   as U -- standard converters
  import qualified Data.Enumerator        as E
  import           Data.Enumerator (($$))
  import qualified Data.Enumerator.List   as EL (head)
  import qualified Data.Monoid            as M

  import           Control.Concurrent 
  import           Control.Applicative ((<$>))
  import           Control.Monad
  import           Control.Monad.Trans
  import           Prelude hiding (catch)
  import           Control.Exception (AssertionFailed(..), SomeException,
                                      catch, throwIO, try)

  import qualified System.ZMQ as Z

  data AccessPoint = Address {
                       acAdd :: String,
                       acOs  :: [Z.SocketOption]}
  
  instance Show AccessPoint where
    show (Address s _) = s

  type Fetch       i o = Z.Context -> i -> E.Enumerator o IO ()
  type Fetch_        o = Fetch () o
  type FetchHelper i o = Z.Context -> i -> IO (Maybe o)

  type Dump i = Z.Context -> E.Iteratee i IO ()

  type OnError   = SomeException -> String -> IO (Maybe B.ByteString)
  type OnError_  = SomeException -> String -> IO ()

  data LinkType = 
         -- | Bind the address
         Bind 
         -- | Connect to the address
         | Connect
    deriving (Show, Read)

  link :: LinkType -> AccessPoint -> Z.Socket a -> IO ()
  link t ac s = case t of
                  Bind    -> Z.bind s (acAdd ac)
                  Connect -> Z.connect s (acAdd ac)

  ------------------------------------------------------------------------
  -- | Server/Client Pattern (Req/Rep) 
  ------------------------------------------------------------------------
  withServer :: Z.Context   -> String          -> 
                String      -> Int             ->
                AccessPoint                    -> 
                LinkType                       ->
                InBound c   -> OutBound o      -> 
                OnError                        ->
                (String  -> E.Iteratee c IO i) ->
                (String  -> Fetch i o)         -> 
                (Service -> IO ())             -> IO ()
  withServer ctx name param n ac t iconv oconv onerr build fetch action =
    withService ctx name param service action
    where service = serve_ True n ac t iconv oconv onerr 
                           build fetch

  ------------------------------------------------------------------------
  -- | Provide a Service
  ------------------------------------------------------------------------
  serve :: Z.Context         -> String -> Int ->
           AccessPoint       -> 
           LinkType          ->
           InBound c         -> OutBound o    ->
           OnError           ->
           E.Iteratee c IO i ->
           Fetch i o         -> IO ()
  serve ctx name n ac t iconv oconv onErr build fetch =
    serve_ False n ac t iconv oconv onErr 
           (\_ -> build) (\_ -> fetch) ctx name "" "" 

  ------------------------------------------------------------------------
  -- the server implementation
  ------------------------------------------------------------------------
  serve_ :: Bool -> Int                    ->
            AccessPoint                    ->
            LinkType                       -> 
            InBound c                      ->
            OutBound o                     ->
            OnError                        ->
            (String  -> E.Iteratee c IO i) ->
            (String -> Fetch i o)          -> 
            Z.Context -> String -> String  -> String -> IO ()
  serve_ controlled n ac t iconv oconv onerr 
         build fetch ctx name sockname param
  ------------------------------------------------------------------------
  -- prepare service for single client
  ------------------------------------------------------------------------
    | n <= 1 = (
      Z.withSocket ctx Z.Rep $ \client -> do
        link t ac client
        if controlled
          then Z.withSocket ctx Z.Sub $ \cmd -> do
                 Z.connect   cmd sockname
                 Z.subscribe cmd ""
                 poll False [Z.S cmd Z.In, Z.S client Z.In] (go client) param
          else forever $ go client param)
        `catch` (\e -> onerr e name >>= \_ -> return ()) -- throwIO e)
  ------------------------------------------------------------------------
  -- prepare service for multiple clients 
  ------------------------------------------------------------------------
    | otherwise = (
        Z.withSocket ctx Z.XRep $ \clients -> do
          link t ac clients 
          Z.withSocket ctx Z.XReq $ \workers -> do
            add <- ("inproc://wrk_" ++) <$> show <$> mkUniqueId
            Z.bind workers add 
            replicateM_ n (forkIO $ startWork add)
            Z.device Z.Queue clients workers) 
        `catch` (\e -> onerr e name >>= \_ -> return ()) -- throwIO e)
  ------------------------------------------------------------------------
  -- start worker for multiple clients 
  ------------------------------------------------------------------------
    where startWork add = Z.withSocket ctx Z.Rep $ \worker -> (do
            Z.connect worker add
            if controlled
              then Z.withSocket ctx Z.Sub $ \cmd -> do
                     Z.connect   cmd sockname
                     Z.subscribe cmd ""
                     poll False 
                          [Z.S cmd Z.In, Z.S worker Z.In] (go worker) param
              else forever $ go worker param)
            `catch` (\e -> onerr e name >>= \_ -> return ()) -- throwIO e)
  ------------------------------------------------------------------------
  -- receive requests and do the job
  ------------------------------------------------------------------------
          go worker p = do
              ei <- E.run (rcvEnum worker iconv $$ build p)
              ifLeft ei (\e -> handle worker e) $ \i ->
                        catch (body worker p i)
                              (\e -> handle worker e)
          body worker p i = do
               eiR <- E.run (fetch p ctx i $$ itSend worker oconv)
               ifLeft eiR
                 (\e -> handle worker e)
                 (\_ -> return ())
  ------------------------------------------------------------------------
  -- generic error handler
  ------------------------------------------------------------------------
          handle sock e = onerr e name >>= \mbX ->
              case mbX of
                Nothing -> 
                  Z.send sock B.empty []
                Just x  -> do 
                  Z.send sock x [Z.SndMore]
                  Z.send sock B.empty []

  ------------------------------------------------------------------------
  -- Client data type
  ------------------------------------------------------------------------
  data Client i o = Client {
                         cliCtx  :: Z.Context,
                         cliSock :: Z.Socket Z.Req,
                         cliAdd  :: AccessPoint,
                         cliOut  :: OutBound o,
                         cliIn   :: InBound  i}

  ------------------------------------------------------------------------
  -- Create a Client
  ------------------------------------------------------------------------
  withClient :: Z.Context  -> AccessPoint -> 
                OutBound o -> InBound i   -> 
                (Client i o -> IO a)      -> IO a
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
  request :: Client i o           ->   
             E.Enumerator o IO () ->
             E.Iteratee i IO a    -> IO (Either SomeException a) 
  request c enum it = tryout ?> reicv
    where tryout    = try $ askFor    c enum
          reicv  _  =       rcvClient c it

  askFor :: Client i o -> E.Enumerator o IO () -> IO ()
  askFor c enum = E.run_ (enum $$ itSend (cliSock c) (cliOut c))

  checkFor :: Client i o -> E.Iteratee i IO a -> 
              IO (Maybe (Either SomeException a))
  checkFor c it = Z.poll [Z.S (cliSock c) Z.In] 0 >>= \[s] ->
    case s of
      Z.S _ Z.In -> Just <$> rcvClient c it
      _          -> return Nothing

  rcvClient :: Client i o -> E.Iteratee i IO a -> IO (Either SomeException a)
  rcvClient c it = E.run (rcvEnum (cliSock c) (cliIn c) $$ it)

  ------------------------------------------------------------------------
  -- | Publish/Subscribe
  ------------------------------------------------------------------------
  data Pub o = Pub {
                 pubCtx  :: Z.Context,
                 pubSock :: Z.Socket Z.Pub,
                 pubAdd  :: AccessPoint,
                 pubOut  :: OutBound o}

  pubContext :: Pub o -> Z.Context
  pubContext = pubCtx

  withPub :: Z.Context -> AccessPoint -> OutBound o -> 
             (Pub o -> IO ()) -> IO ()
  withPub ctx ac oconv act = Z.withSocket ctx Z.Pub $ \s -> do
    Z.bind s (acAdd ac)
    act Pub {
          pubCtx  = ctx,
          pubSock = s,
          pubAdd  = ac,
          pubOut  = oconv}

  issue :: Pub o -> E.Enumerator o IO () -> IO ()
  issue p enum = E.run_ (enum $$ itSend (pubSock p) (pubOut p))
             
  withPeriodicPub :: Z.Context            -> 
                     String -> String     ->
                     Millisecond          ->
                     AccessPoint          -> 
                     OutBound o           ->
                     OnError_             ->
                     (String -> Fetch_ o) -> 
                     (Service -> IO ())   -> IO ()
  withPeriodicPub ctx name param period ac oconv onerr fetch action =
    withService ctx name param service action
    where service = publish_ True period ac oconv onerr fetch

  publish_ :: Bool                  ->
              Millisecond           ->
              AccessPoint           -> 
              OutBound o            ->
              OnError_              ->
              (String -> Fetch_  o) -> 
              Z.Context -> String   -> 
              String -> String      -> IO ()
  publish_ controlled period ac oconv onerr 
           fetch ctx name sockname param = (
    Z.withSocket ctx Z.Pub $ \sock -> do
      Z.bind sock (acAdd ac)
      if controlled
        then Z.withSocket ctx Z.Sub $ \cmd -> do
               Z.connect   cmd sockname
               Z.subscribe cmd ""
               periodicSend False period cmd (go sock) param
        else periodic period $ go sock param)
    `catch` (\e -> onerr e name >> throwIO e)
  ------------------------------------------------------------------------
  -- do the job periodically
  ------------------------------------------------------------------------
    where go sock p   = catch (body sock p) 
                              (\e -> onerr e name)
          body sock p = do
            eiR <- E.run (fetch p ctx () $$ itSend sock oconv)
            ifLeft eiR
              (\e -> onerr e name)
              (\_ -> return ())

  ------------------------------------------------------------------------
  -- | Publish
  ------------------------------------------------------------------------
  publish :: Z.Context   -> 
             String      ->
             Millisecond ->
             AccessPoint -> 
             OutBound o  ->
             OnError_    ->
             Fetch_   o  -> IO ()
  publish ctx name period ac oconv onErr fetch = 
    publish_ False period ac oconv onErr (\_ -> fetch ) ctx name "" ""

  ------------------------------------------------------------------------
  -- Subscription
  ------------------------------------------------------------------------
  withSub :: Z.Context                   -> 
             String -> String -> String  -> 
             AccessPoint                 -> 
             InBound i   -> OnError_     ->
             (String  -> Dump i)         -> 
             (Service -> IO ())          -> IO ()
  withSub ctx name sub param ac iconv onErr dump action =
    withService ctx name param service action
    where service = subscribe_ True sub ac iconv onErr dump

  subscribe_ :: Bool -> String -> 
                AccessPoint    -> 
                InBound i      -> 
                OnError_       -> 
                (String        -> Dump i)  -> 
                Z.Context      -> 
                String -> String -> String -> IO ()
  subscribe_ controlled sub ac iconv onerr dump 
             ctx name sockname param = (
    Z.withSocket ctx Z.Sub $ \sock -> do
      Z.connect sock (acAdd ac)
      Z.subscribe sock sub
      if controlled
        then Z.withSocket ctx Z.Sub $ \cmd -> do
               Z.connect   cmd sockname
               Z.subscribe cmd ""
               poll False [Z.S cmd Z.In, Z.S sock Z.In] (go sock) param
        else forever $ go sock param)
    `catch` (\e -> onerr e name >> throwIO e) 
    where go :: Z.Socket a -> String -> IO ()
          go sock p = E.run_ (rcvEnum sock iconv $$ dump p ctx)
                      `catch` (\e -> do onerr e name)

  subscribe :: Z.Context     -> 
               String        -> 
               String        -> 
               AccessPoint   -> 
               InBound i     -> 
               OnError_      -> 
               Dump   i      -> IO ()
  subscribe ctx name sub ac iconv onerr dump = 
    subscribe_ False sub ac iconv onerr (\_ -> dump) ctx name "" ""

  unsubscribe :: Service -> IO ()
  unsubscribe = pause

  resubscribe :: Service -> IO ()
  resubscribe = resume

  ------------------------------------------------------------------------
  -- | Pipeline
  ------------------------------------------------------------------------
  withPuller :: Z.Context                  ->
                String    -> String        ->
                AccessPoint                ->
                InBound i                  ->  
                OnError_                   ->
                (String  -> Dump   i)      -> 
                (Service -> IO ())         -> IO ()
  withPuller ctx name param ac iconv onerr dump action =
    withService ctx name param service action
    where service = pull_ True ac iconv onerr dump 

  pull :: Z.Context     ->
          String        -> 
          AccessPoint   ->
          InBound i     ->
          OnError_      ->
          Dump   i      -> IO ()
  pull ctx name ac iconv onerr dump =
    pull_ False ac iconv onerr (\_ -> dump) ctx name "" ""

  pull_ :: Bool                 ->
           AccessPoint          ->
           InBound i            ->
           OnError_             ->
           (String -> Dump   i) ->
           Z.Context -> String  -> 
           String    -> String  -> IO ()
  pull_ controlled ac iconv onerr dump ctx name sockname param = (
    Z.withSocket ctx Z.Pull $ \sock -> do
      Z.connect sock (acAdd ac)
      if controlled
        then Z.withSocket ctx Z.Sub $ \cmd -> do
               Z.connect   cmd sockname
               Z.subscribe cmd ""
               poll False [Z.S cmd Z.In, Z.S sock Z.In] (go sock) param
        else forever $ go sock param)
    `catch` (\e -> onerr e name)
  ------------------------------------------------------------------------
  -- do the job 
  ------------------------------------------------------------------------
    where go sock p = E.run_ (rcvEnum sock iconv $$ dump p ctx)
                      `catch` (\e -> onerr e name)
 
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
    Z.bind s (acAdd ac)
    act Pipe {
        pipCtx  = ctx,
        pipSock = s,
        pipAdd  = ac,
        pipOut  = oconv}

  push :: Pipe o -> E.Enumerator o IO () -> IO (Either SomeException ()) 
  push p enum = E.run (enum $$ itSend (pipSock p) (pipOut p))

  ------------------------------------------------------------------------
  -- | Exclusive Pair
  ------------------------------------------------------------------------
  data Peer a = Peer {
                  peeCtx  :: Z.Context,
                  peeSock :: Z.Socket Z.Pair,
                  peeAdd  :: AccessPoint,
                  peeIn   :: InBound a,
                  peeOut  :: OutBound a
                }

  withPeer :: Z.Context -> AccessPoint -> LinkType ->
              InBound a -> OutBound a  ->
              (Peer a -> IO ())        -> IO ()
  withPeer ctx ac t iconv oconv act = Z.withSocket ctx Z.Pair $ \s -> 
    link t ac s >> act Peer {
                         peeCtx  = ctx,
                         peeSock = s,
                         peeAdd  = ac,
                         peeIn   = iconv,
                         peeOut  = oconv}

  send :: Peer o -> E.Enumerator o IO () -> IO ()
  send p enum = E.run_ (enum $$ itSend (peeSock p) (peeOut p))

  receive :: Peer i -> E.Iteratee i IO a -> IO (Either SomeException a)
  receive p it = E.run (rcvEnum (peeSock p) (peeIn p) $$ it)

  peerContext :: Peer a -> Z.Context
  peerContext = peeCtx

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

  ------------------------------------------------------------------------
  -- enumerator
  ------------------------------------------------------------------------
  rcvEnum :: Z.Socket a -> InBound i -> E.Enumerator i IO b
  rcvEnum s iconv = go True
    where go more step = 
            case step of 
              E.Continue k -> do
                if more then do
                    x <- liftIO $ Z.receive s []
                    m <- liftIO $ Z.moreToReceive s
                    i <- tryIO  $ iconv x
                    go m $$ k (E.Chunks [i])
                  else E.continue k
              _ -> E.returnI step

  ------------------------------------------------------------------------
  -- iteratee 
  ------------------------------------------------------------------------
  itSend :: Z.Socket a -> OutBound o -> E.Iteratee o IO ()
  itSend s oconv = EL.head >>= go
    where go mbO =
            case mbO of
              Nothing -> return () -- liftIO $ Z.send s (B.empty) []
              Just o  -> do
                x    <- tryIO $ oconv o           
                mbO' <- EL.head
                let opt = case mbO' of
                            Nothing -> []
                            Just _  -> [Z.SndMore]
                liftIO $ Z.send s x opt
                go mbO'

  ------------------------------------------------------------------------
  -- standard enumerators
  ------------------------------------------------------------------------
  fetcher :: FetchHelper i o -> Fetch i o 
  fetcher fetch ctx i step =
    case step of
      (E.Continue k) -> chainIOe (fetch ctx i) $ \mbo ->
        case mbo of 
          Nothing -> E.continue k
          Just o  -> fetcher fetch ctx i $$ k (E.Chunks [o]) 
      _ -> E.returnI step

  fetch1 :: FetchHelper i o -> Fetch i o
  fetch1 = go True 
    where go first fetch ctx i step =
            case step of
              (E.Continue k) -> 
                if first then chainIOe (fetch ctx i) $ \mbX ->
                  case mbX of
                    Nothing -> E.continue k 
                    Just x  -> go False fetch ctx i $$ k (E.Chunks [x])
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

  listFetcher :: [o] -> Fetch_ o 
  listFetcher l ctx _ step =
    case step of
      (E.Continue k) -> do
        if null l then E.continue k
                  else listFetcher (tail l) ctx () $$ k (E.Chunks [head l])
      _ -> E.returnI step

  fetchFor :: (Z.Context -> Int -> IO o) -> (Int, Int) -> Fetch () o
  fetchFor fetch (i,e) c _ step =
    case step of
      (E.Continue k) -> do
         if i >= e then E.continue k
                   else chainIOe (fetch c i) $ \x -> 
                     fetchFor fetch (i+1, e) c () $$ k (E.Chunks [x])
      _ -> E.returnI step

  err :: Fetch_ o
  err _ _ s = do
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
          (Z.Context -> String -> s -> i -> IO ()) -> String -> Dump i
  sink op cl save p ctx = go Nothing
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
           (Z.Context -> String -> s -> i -> IO ()) -> String -> Dump i
  sinkI op cl save p ctx = go Nothing
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

  sinkLess :: (Z.Context -> String -> i -> IO ()) -> String -> Dump i
  sinkLess save p ctx = store (save ctx p)

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

  ------------------------------------------------------------------------
  -- some helpers
  ------------------------------------------------------------------------
  ifLeft :: Either a b -> (a -> c) -> (b -> c) -> c
  ifLeft e l r = either l r e

  chainIOe :: IO a -> (a -> E.Iteratee b IO c) -> E.Iteratee b IO c
  chainIOe x f = liftIO (try x) >>= \ei ->
                   case ei of
                     Left  e -> E.returnI (E.Error e)
                     Right y -> f y

  chainIO :: IO a -> (a -> E.Iteratee b IO c) -> E.Iteratee b IO c
  chainIO x f = liftIO (try x) >>= \ei ->
                  case ei of
                    Left  e -> E.throwError (e::SomeException)
                    Right y -> f y

  tryIO :: IO a -> E.Iteratee i IO a
  tryIO act = 
    liftIO (try act) >>= \ei ->
      case ei of
        Left  e -> E.throwError (e::SomeException)
        Right x -> return x

  tryIOe :: IO a -> E.Iteratee i IO a
  tryIOe act = 
    liftIO (try act) >>= \ei ->
      case ei of
        Left  e -> E.returnI (E.Error e)
        Right x -> return x

  eiCombine :: IO (Either a b) -> (b -> IO (Either a c)) -> IO (Either a c)
  eiCombine x f = x >>= \mbx ->
                  case mbx of
                    Left  e -> return $ Left e
                    Right y -> f y

  infixl 9 ?>
  (?>) :: IO (Either a b) -> (b -> IO (Either a c)) -> IO (Either a c)
  (?>) = eiCombine

