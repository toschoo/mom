-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Stompl/Patterns/Basic.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- The Stomp specification defines only one queuing pattern:
-- /publish and subscribe/.
-- In some situations, other patterns may be more appropriate
-- such as /peer-to-peer/ or /client server/.
-- Whereas patterns like peer-to-peer 
-- are easy to simulate with the means provided by Stomp,
-- client/server needs some more coordination
-- between the involved parties, the clients and the server.
--
-- This module provides abstractions that implement
-- a simple client/server protocol on top of Stomp.
-- A server is an application that provides a /service/
-- to others.
-- The service must be explicitly requested by a client
-- and only the requesting client must see the response
-- produced by the server.
--
-- The module, basically, provides two data types ('ClientA' and 'ServerA')
-- and two functions working on these data types, namely
-- 'request' and 'reply'.
-- With the request function, the client requests a service
-- and waits for a response.
-- With the reply function, a server waits for a request,
-- produces a response and sends it back through a channel
-- indicated by the client.
--
-- Internally, 'request' and 'reply' use a message header
-- called \"__client__\" to agree on the reply queue
-- 
-------------------------------------------------------------------------------
module Network.Mom.Stompl.Patterns.Basic (
                          -- * Client
                          ClientA, withClient, request, checkRequest,
                          -- * Server
                          ServerA, withServer, reply,
                          RegistryDesc, withServerThread,
                          -- * Pub
                          PubA, withPub, publish, withPubThread,
                          -- * Sub
                          SubA, withSub, checkIssue,
                          withSubThread, withSubMVar,
                          -- * Worker
                          PusherA, withPusher, push,
                          withTaskThread)
where

  import           Registry
  import           Types

  import           Network.Mom.Stompl.Client.Queue 
  import qualified Network.Mom.Stompl.Frame as F
  import           System.Timeout
  import           Data.Time.Clock
  import           Data.Char (isDigit)
  import qualified Data.ByteString.Char8 as B
  import           Codec.MIME.Type (Type, nullType)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, 
                                      SomeException, Handler(..),
                                      AsyncException(ThreadKilled),
                                      bracket, catch, catches, finally)
  import           Control.Concurrent 
  import           Control.Monad (forever, unless, when, void)

  ------------------------------------------------------------------------
  -- | The client data type
  ------------------------------------------------------------------------
  data ClientA i o = Cl {
                      clChn :: String,
                      clJob :: JobName,
                      clIn  :: Reader i,
                      clOut :: Writer o}
  
  ------------------------------------------------------------------------
  -- | The function creates a client that lives within its scope.
  --
  --   Parameters:
  --
  --   * 'Con': Connection to a Stomp broker
  --
  --   * 'String': Name of the Client, used for debugging.
  --
  --   * 'ReaderDesc' i: Description of a reader queue;
  --                     this is the queue through which the server
  --                     will send its response.
  --
  --   * 'WriterDesc' o: Description of a writer queue;
  --                     this is the queue through which the server
  --                     is expecting requests.
  --
  --   * 'ClientA' i o -> IO r: An application-defined action
  --                            whose scope defines the client's lifetime
  ------------------------------------------------------------------------
  withClient :: Con -> String  ->
                       JobName ->
                       ReaderDesc i ->
                       WriterDesc o ->
                       (ClientA i o  -> IO r) -> IO r
  withClient c n jn rd@(rn, _, _, _) wd act =
    withPair c n rd wd $ \(r,w) -> act $ Cl rn jn r w

  -- the reply queue header ----------------------------------------------
  channel :: String
  channel = "__client__"

  ------------------------------------------------------------------------
  -- | The client will send the request of type /o/
  --   and wait for the reply until the timeout exprires.
  --   The reply is of type /i/ and is returned as 'Message' /i/.
  --   If the timeout expires before the reply has been received,
  --   the function returns 'Nothing'.
  --
  --   Parameters:
  --
  --   * 'ClientA' i o: The client; note that i is the type of the reply,
  --                                          o is the type of the request.
  --
  --   * 'Int': The timeout in microseconds.
  --
  --   * 'Type': The /MIME/ type of the request.
  --
  --   * ['F.Header']: List of additional headers 
  --                   to be sent with the request;
  --                   note that the function, internally,
  --                   uses a header named \"__client__\".
  --                   This header name, hence, is reserved
  --                   and must not be used by the application.
  --
  --  * /o/: The request 
  ------------------------------------------------------------------------
  request :: ClientA i o -> 
             Int -> Type -> [F.Header] -> o -> IO (Maybe (Message i))
  request c tmo t hs r = 
    let hs' = [(channel  , clChn c),
               ("__job__", clJob c)] ++ hs
     in writeQ (clOut c) t hs' r >> timeout tmo (readQ (clIn c))

  ------------------------------------------------------------------------
  -- | This function serves as a \"delayed\" receiver for the case
  --   that the timeout of a request has expired.
  --   When using this function, it is assumed
  --   that a request has been made, but no response has been received.
  --   It can be used in time-critical applications,
  --   where the client may use the time between request and reply
  --   productively, instead of blocking on the reply queue.
  --
  --   Use this function with care! It can be easily abused
  --   to break the client/server pattern, when it is called
  --   without a request having been made before.
  --   If, in this case, /timout/ is /-1/,
  --   the application will block for ever.
  --
  --   For parameters, please refer to 'request'.
  ------------------------------------------------------------------------
  checkRequest :: ClientA i o -> Int -> IO (Maybe (Message i))
  checkRequest c tmo = timeout tmo $ readQ (clIn c)

  ------------------------------------------------------------------------
  -- | The server data type
  ------------------------------------------------------------------------
  data ServerA i o = Srv {
                      srvName :: String,
                      srvIn   :: Reader i,
                      srvOut  :: Writer o}
  
  ------------------------------------------------------------------------
  -- | The function creates a server
  --   that lives within its scope.
  --
  --   Parameters:
  --
  --   * 'Con': Connection to a Stomp broker
  --
  --   * 'String': Name of the Server, used for debugging.
  --
  --   * 'ReaderDesc' i: Description of a reader queue;
  --                     this is the queue through which clients
  --                     are expected to send requests.
  --
  --   * 'WriterDesc' o: Description of a writer queue;
  --                     this is the queue through which
  --                     a specific client will expect the reply.
  --                     Note that the server will overwrite
  --                     the destination of this queue
  --                     using 'writeAdHoc'; 
  --                     the destination of this queue, hence,
  --                     is irrelevant.
  --
  --   * 'ServerA' i o -> IO r: An application-defined action
  --                            whose scope defines the server's lifetime
  ------------------------------------------------------------------------
  withServer :: Con -> String ->
                       ReaderDesc i ->
                       WriterDesc o ->
                       (ServerA i o  -> IO r) -> IO r
  withServer c n rd wd act =
    withPair c n rd wd $ \(r,w) -> act $ Srv n r w

  ------------------------------------------------------------------------
  -- | Waits for a client request, 
  --   calls the application-defined transformer to generate a reply
  --   and sends this reply through the queue
  --   whose name is indicated by the value of the \"__client__\" header.
  --   The time a server waits for a request may be restricted
  --   by the timeout. Typically, you would call reply with 
  --   timeout set to /-1/ (for /wait eternally/).
  --   There may be situations, however, where it actually
  --   makes sense to restrict the waiting time,
  --   /i.e./ to perform some housekeeping in between.
  --
  --   Typically, you call reply in a loop like
  --
  --   > forever $ reply srv (-1) nullType [] f
  --
  --   where /f/ is a function of type 
  --
  --   > 'Message' i -> 'IO' o.
  --
  --   Parameters:
  --
  --   * 'ServerA' i o: The server; note that i is the request queue
  --                                     and  o the reply queue.
  --
  --   * 'Int': The timeout in microseconds.
  --
  --   * 'Type': The /MIME/ type of the reply.
  --
  --   * ['F.Header']: Additional headers to be sent with the reply.
  --
  --   * 'Message' i -> IO o: Transforms the request into a reply -
  --                          this defines the service provided by this
  --                          application.
  ------------------------------------------------------------------------
  reply :: ServerA i o -> Int -> Type -> [F.Header] -> 
           (Message i -> IO o) -> IO ()
  reply s tmo t hs transform = do
    mbM <- timeout tmo $ readQ (srvIn s)
    case mbM of
      Nothing -> return ()
      Just m  -> 
        case lookup channel $ msgHdrs m of
          Nothing -> throwIO $ HeaderX channel 
                                 "No reply channel defined!"
          Just c  -> transform m >>=
                        writeAdHoc (srvOut s) c t hs 

  type RegistryDesc = (QName, Int, (Int, Int, Int))

  ------------------------------------------------------------------------
  -- | Create a server that works in a background thread 
  ------------------------------------------------------------------------
  withServerThread :: Con -> String ->
                      JobName -> Type -> [F.Header] -> (Message i -> IO o) ->
                      ReaderDesc i -> 
                      WriterDesc o ->
                      RegistryDesc -> 
                      OnError -> (IO r) ->  IO r
  withServerThread c n
                     jn t hs transform
                     rd@(rn, ros, rh, iconv)
                     wd@(wn, wos, wh, oconv) 
                     (reg, tmo, (best, mn, mx))
                     onErr action =
    withServer c n rd wd $ \s -> do
      (sc,me) <- register c jn Service reg rn tmo best
      case sc of
        OK -> 
          if me < mn || me > mx
            then throwIO $ UnacceptableHbX me
            else do hb  <- mkHB me
                    m   <- newMVar hb 
                    stp <- newEmptyMVar -- wait for thread to terminate
                    let p = if me <= 0 then (-1) else 1000 * me 
                    bracket (forkIO $ finally (srv m p s) (putMVar stp ()))
                            (killAndWait stp) 
                            (\_   -> action)
        e -> throwIO $ NotOKX e "on register"
      where srv m p s = withWriter c "HB" reg [] [] nobody $ \w -> 
                          forever $ catches (
                            reply s p t hs transform >> heartbeat m w jn rn)
                            $ ignoreHandler Error (srvName s) onErr

  ------------------------------------------------------------------------
  -- | The publisher data type
  ------------------------------------------------------------------------
  data PubA o = Pub {
                  pubName :: String,
                  pubJob  :: JobName,
                  pubReg  :: Registry,
                  pubOut  :: Writer o}

  ------------------------------------------------------------------------
  -- | Creates a publisher with the lifetime of the scope
  --   of the user action passed in.
  ------------------------------------------------------------------------
  withPub :: Con -> String -> JobName -> QName -> OnError -> 
             WriterDesc o ->
             (PubA o      -> IO r) -> IO r
  withPub c n jn rn onErr wd@(wn, wos, wh, oconv) act = 
    withRegistry c jn rn (0,0) onErr $ \r ->
      withWriter c jn wn wos wh oconv $ \w -> act $ Pub n jn r w

  ------------------------------------------------------------------------
  -- | Publishh data of type /o/.
  ------------------------------------------------------------------------
  publish :: PubA o -> Type -> [F.Header] -> o -> IO ()
  publish p t hs m = void $ mapR  (pubReg p) (pubJob p) $ \prv -> 
                       writeAdHoc (pubOut p) (prvQ prv) t hs m

  ------------------------------------------------------------------------
  -- | Create a publisher that works in a background thread 
  ------------------------------------------------------------------------
  withPubThread :: Con -> String -> JobName -> QName ->
                   Type -> [F.Header] -> (IO o) ->
                   WriterDesc o    -> Int -> 
                   OnError -> (IO r) ->  IO r
  withPubThread c n jn rn
                t hs create
                wd period
                onErr action = do
    stp <- newEmptyMVar -- wait for thread to terminate
    bracket (forkIO $ finally doPub (putMVar stp ()))
            (killAndWait stp) 
            (\_   -> action)
    where doPub = withPub c n jn rn onErr wd $ \p ->
                    forever $ catches (do
                      threadDelay period -- simplistic
                      m <- create
                      publish p t hs m) (
                    ignoreHandler Error (pubName p) onErr)

  ------------------------------------------------------------------------
  -- | Subscriber data type
  ------------------------------------------------------------------------
  data SubA i = Sub {
                 subName :: String,
                 subIn   :: Reader i
                }

  ------------------------------------------------------------------------
  -- | Creates a subscriber with the lifetime of the scope
  --   of the user action passed in.
  ------------------------------------------------------------------------
  withSub :: Con -> String -> JobName -> QName -> Int ->
             ReaderDesc i ->
             (SubA i      -> IO r) -> IO r
  withSub c n jn wn tmo rd@(rn, ros, rh, iconv) act = 
    withReader c n rn ros rh iconv $ \r -> finally (go r) finalise
    where go r     = do
            (sc, _) <- register c jn Topic wn rn tmo 0
            if (sc /= OK) then throwIO $ NotOKX sc "on register "
                          else act $ Sub n r
          finalise = do sc <- unRegister c jn wn rn tmo
                        unless (sc == OK) $ 
                              throwIO $ NotOKX sc "on unregister"

  ------------------------------------------------------------------------
  -- | Create a subscriber that works in a background thread 
  ------------------------------------------------------------------------
  withSubThread :: Con -> String -> JobName    -> QName  -> Int     ->
                   ReaderDesc i  -> (Message i -> IO ()) -> OnError -> 
                   (IO r) -> IO r
  withSubThread c n jn wn tmo rd@(rn, ros, rh, iconv) job onErr action = do
    stp <- newEmptyMVar -- wait for thread to terminate
    bracket (forkIO $ finally go (putMVar stp ()))
            (killAndWait stp) 
            (\_   -> action)
    where go = withSub c n jn wn tmo rd $ \s -> 
                 forever $ catches (checkIssue s >>= job)
                                   (ignoreHandler Error (subName s) onErr)

  ------------------------------------------------------------------------
  -- | Create a subscriber that works in a background thread 
  --   and updates an MVar, whenever new data are available
  ------------------------------------------------------------------------
  withSubMVar :: Con -> String -> JobName    -> QName  -> Int     ->
                 ReaderDesc i  -> MVar i     -> OnError -> 
                 (IO r) -> IO r
  withSubMVar c n jn wn tmo rd m onErr action = 
    withSubThread c n jn wn tmo rd (job m) onErr action
    where job v m = modifyMVar_ v $ \_ -> return $ msgContent m 

  ------------------------------------------------------------------------
  -- | Check if there is a new issue
  ------------------------------------------------------------------------
  checkIssue :: SubA i -> IO (Message i)
  checkIssue s = readQ (subIn s)

  ------------------------------------------------------------------------
  -- | Pusher
  ------------------------------------------------------------------------
  data PusherA o = Pusher {
                    pushName :: String,
                    pushJob  :: JobName,
                    pushQ    :: Writer o
                  }

  ------------------------------------------------------------------------
  -- | Create a 'Pusher' with the lifetime of the action passed in
  ------------------------------------------------------------------------
  withPusher :: Con -> String -> JobName -> WriterDesc o -> 
                (PusherA o -> IO r) -> IO r
  withPusher c n jn wd@(wq, wos, wh, oconv) action = 
    withWriter c n wq wos wh oconv $ \w -> action $ Pusher n jn w

  ------------------------------------------------------------------------
  -- | Push a 'Job'
  ------------------------------------------------------------------------
  push :: PusherA o -> Type -> [F.Header] -> o -> IO ()
  push p t hs m = let hs' = ("__job__", pushJob p) : hs
                   in writeQ (pushQ p) t hs' m

  ------------------------------------------------------------------------
  -- | Create a worker that works in a background thread 
  ------------------------------------------------------------------------
  withTaskThread :: Con -> String ->
                    JobName -> (Message i -> IO ()) ->
                    ReaderDesc i -> 
                    RegistryDesc -> 
                    OnError -> (IO r) ->  IO r
  withTaskThread c n
                   jn task
                   rd@(rn, ros, rh, iconv)
                   (reg, tmo, (best, mn, mx))
                   onErr action = do
      (sc,me) <- register c jn Task reg rn tmo best
      case sc of
        OK -> 
          if me < mn || me > mx
            then throwIO $ UnacceptableHbX me
            else do hb  <- mkHB me
                    m   <- newMVar hb 
                    stp <- newEmptyMVar -- wait for thread to terminate
                    let p = if me <= 0 then (-1) else 1000 * me 
                    bracket (forkIO $ finally (tsk m p) (putMVar stp ()))
                            (killAndWait stp) 
                            (\_   -> action)
        e -> throwIO $ NotOKX e "on register"
      where tsk m p = withWriter   c "HB" reg [] [] nobody $ \w -> 
                        withReader c n rn ros rh iconv $ \r ->
                          forever $ catches (do
                            mbM <- timeout p $ readQ r 
                            case mbM of
                              Nothing -> return ()
                              Just x  -> task x 
                            heartbeat m w jn rn)
                          (ignoreHandler Error n onErr)

  ------------------------------------------------------------------------
  -- | Send heartbeats from a publisher
  ------------------------------------------------------------------------
  withPubProxy :: Con -> String -> JobName -> QName ->
                  ReaderDesc i  ->
                  RegistryDesc  ->
                  (IO r) -> IO r
  withPubProxy c n jn wn rd@(rn, _, _, _)
               (reg, tmo, (best, mn, mx)) act = do
      stp <- newEmptyMVar
      bracket (forkIO $ finally go (putMVar stp ()))
              (killAndWait stp) 
              (\_   -> act)
      where go = withSub c n jn wn tmo rd $ \s -> 
                   withWriter c "HB" wn [] [] nobody $ \w -> do
                     (sc, h) <- register c jn Topic wn rn tmo 0
                     if (sc /= OK) 
                       then throwIO $ NotOKX sc "on register proxy"
                       else 
                         if h < mn || h > mx
                           then throwIO $ UnacceptableHbX h
                           else do hb <- mkHB h >>= newMVar
                                   beat hb (h * 1000) s w 0
            beat :: MVar HB -> Int -> SubA i -> Writer () -> Int -> IO ()
            beat hb h s w i = do
              mbM  <- timeout h $ checkIssue s
              case mbM of
                Nothing -> if i == 10 
                             then throwIO $ MissingHbX "No input from pub"
                             else beat hb h s w (i+1)
                Just m  -> heartbeat hb w jn wn >> 
                           beat hb h s w 0
      
