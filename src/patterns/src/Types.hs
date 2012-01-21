module Types (
          -- * Service Access Point
          AccessPoint(..), LinkType(..), parseLink, link,
          AccessType(..), access, safeClose,
          -- * PollEntry
          PollEntry(..), pollEntry,
          -- * Enumerators
          Fetch, Fetch_, FetchHelper, Dump,
          rcvEnum, itSend,
          -- * Converters
          InBound, OutBound,
          idIn, idOut, inString, outString, inUTF8, outUTF8,
          -- * Error Handlers
          Criticality(..),
          OnError, OnError_,
          chainIO, chainIOe, tryIO, tryIOe,
          -- * ZMQ Context
          Z.Context, Z.withContext,
          Z.SocketOption(..),
          -- * Helpers
          retries, trycon, ifLeft, (?>), noparam,
          Timeout, OnTimeout,
          Millisecond, Identifier)

where

  import qualified Data.ByteString.Char8  as B
  import qualified Data.ByteString.UTF8   as U -- standard converters
  import           Data.Char (toLower)
  import           Data.List (intercalate)
  import           Data.List.Split (splitOn)
  import qualified Data.Enumerator        as E
  import           Data.Enumerator (($$))
  import qualified Data.Enumerator.List   as EL (head)

  import           Control.Concurrent
  import           Control.Monad
  import           Control.Monad.Trans
  import           Prelude hiding (catch)
  import           Control.Exception (SomeException, try, catch, throwIO)
  import           System.ZMQ as Z

  ------------------------------------------------------------------------
  -- | Type of a 'PollEntry';
  ------------------------------------------------------------------------
  data AccessType = 
         -- | Represents a Service and expects connections from Clients;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.Rep'
         XServer    
         -- | Represents a Client and connects to a Service;
         --   should be used with 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.Req'
         | XClient
         -- | Represents a load balancer, 
         --   expecting connections from clients;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.XRep'
         | XDealer 
         -- | Represents a router
         --   expecting connections from servers;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.XReq'
         | XRouter 
         -- | Represents a publisher;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.Pub'
         | XPub    
         -- | Represents a subscriber;
         --   should be used with 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.Sub'
         | XSub    
         -- | Represents a Pipe;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.Push'
         | XPipe
         -- | Represents a Puller;
         --   should be used with 'Connect';
         --   corresponds to ZMQ Socket Type 'Z.Pull'
         | XPull
         -- | Represents a Peer;
         --   corresponding peers must use complementing 'LinkType';
         --   corresponds to ZMQ Socket Type 'Z.Pair'
         | XPeer   
    deriving (Eq, Show, Read)

  access :: Z.Context -> AccessType -> LinkType -> 
               String -> String     -> IO Z.Poll
  access ctx a l u t = 
    case a of 
      XServer -> Z.socket ctx Z.Rep  >>= go
      XClient -> Z.socket ctx Z.Req  >>= go
      XDealer -> Z.socket ctx Z.XRep >>= go
      XRouter -> Z.socket ctx Z.XReq >>= go
      XPub    -> Z.socket ctx Z.Pub  >>= go
      XPipe   -> Z.socket ctx Z.Push >>= go
      XPull   -> Z.socket ctx Z.Pull >>= go
      XPeer   -> Z.socket ctx Z.Pair >>= go
      XSub    -> Z.socket ctx Z.Sub  >>= \s -> 
                  Z.subscribe s t >> go s
   where go s = do case l of
                     Bind    -> Z.bind s u
                     Connect -> trycon s u retries
                   return $ Z.S s Z.In

  safeClose :: Z.Socket a -> IO ()
  safeClose s = catch (Z.close s)
                      (\e -> let _ = (e::SomeException)
                              in return ())

  ------------------------------------------------------------------------
  -- | Describes how to access a service;
  --   usually an address and a list of 'Z.SocketOption';
  --   addresses are passed in as strings of the form:
  --
  --   * \"tcp:\/\/*:5555\": for binding the port /5555/ via TCP\/IP
  --                         on all network interfaces;
  --                         an IPv4 address or the operating system
  --                         interface name could be given instead. 
  --
  --   * \"tcp:\/\/localhost:5555\": for connecting to the port /5555/ 
  --                                 on /localhost/ via TCP\/IP;
  --                                 the endpoint may given as /DNS/ name
  --                                 or as an IPv4 address.
  --
  --   * \"ipc:\/\/\/tmp\/queues/0\": for binding and connecting to
  --                                  a local inter-process communication
  --                                  endpoint, in this case created under
  --                                  \/tmp\/queues\/0;
  --                                  only available on UNIX.
  -- 
  --   * \"inproc:\/\/worker\": for binding and connecting to
  --                            the process internal address /worker/
  --
  --   For more options, please refer to the zeromq documentation.
  --   For 'Z.SocketOption', please refer to the ZMQ documentation.
  ------------------------------------------------------------------------
  data AccessPoint = Address {
                       -- | Address string
                       acAdd :: String,
                       -- | Socket options
                       acOs  :: [Z.SocketOption]}
  
  instance Show AccessPoint where
    show (Address s _) = s

  ------------------------------------------------------------------------
  -- | A poll entry describes how to handle an AccessPoint
  ------------------------------------------------------------------------
  data PollEntry = Poll {
                     pollId   :: Identifier,
                     pollAdd  :: String,
                     pollType :: AccessType,
                     pollLink :: LinkType,
                     pollSub  :: String,
                     pollOs   :: [Z.SocketOption]
                   }
    deriving (Show, Read)

  instance Read Z.SocketOption where
    readsPrec _ s = [(Z.Affinity 0,"")]

  instance Eq PollEntry where
    x == y = pollId x == pollId y

  ------------------------------------------------------------------------
  -- | Creates a 'PollEntry';
  --
  --   Parameters:
  --
  --   * 'Identifier': identifies an 'AccessPoint' with a 'String';
  --                    the string shall be unique for one device.
  --
  --   * 'AccessType': the 'AccessType' of this 'AccessPoint'
  --
  --   * 'AccessPoint': the 'AccessPoint' itself
  --
  --   * 'LinkType': how to link to this 'AccessPoint'
  --
  --   * 'String': A subscription topic - 
  --                ignored for all 'AccessPoint', but those
  --                with 'AccessType' 'Sub' 
  ------------------------------------------------------------------------
  pollEntry :: Identifier -> 
               AccessType -> AccessPoint -> LinkType ->
               String     -> PollEntry
  pollEntry i at ac lt sub = Poll {
                               pollId   = i,
                               pollAdd  = acAdd ac,
                               pollType = at,
                               pollLink = lt,
                               pollSub  = sub,
                               pollOs   = acOs ac}

  ------------------------------------------------------------------------
  -- | 'E.Enumerator' to process data segments of type /o/;
  --   receives the 'Z.Context' and an input of type /i/;
  --   'Fetch' is used by 'Server's that receive requests of type /i/
  --   and produce an outgoing stream with segments of type /o/.
  ------------------------------------------------------------------------
  type Fetch       i o = Z.Context -> String -> i -> E.Enumerator o IO ()

  ------------------------------------------------------------------------
  -- | A variant of 'Fetch' without input
  ------------------------------------------------------------------------
  type Fetch_        o = Fetch () o

  ------------------------------------------------------------------------
  -- | A function that may be used with some of the enumerators
  --   defined in 'Enumerator'.
  --   This helper returns 'Nothing' to signal 
  --   that no more data are available
  --   and 'Just' /o/ to continue the stream.
  --   FetchHelpers are typically used with an enumerator
  --   that already defines a given enumerator logic,
  --   /e.g./ 'fetch1' or 'fetchFor'.
  ------------------------------------------------------------------------
  type FetchHelper i o = Z.Context -> String -> i -> IO (Maybe o)

  ------------------------------------------------------------------------
  -- | 'E.Iteratee' to process data segments of type /i/;
  --   receives the 'Z.Context'
  ------------------------------------------------------------------------
  type Dump i = Z.Context -> String -> E.Iteratee i IO ()

  ------------------------------------------------------------------------
  -- | Error handler for 'Server';
  --   receives the 'Criticality' of the error event,
  --   the exception, the server name and the parameter.
  --   If the error handler returns 'Just' a 'B.ByteString'
  --   this value is sent to the client as error message.
  ------------------------------------------------------------------------
  type OnError   = Criticality      -> 
                   SomeException    -> 
                   String -> String -> IO (Maybe B.ByteString)

  ------------------------------------------------------------------------
  -- | Error handler for all services but 'Server';
  --   receives the 'Criticality' of the error event,
  --   the exception and the service name.
  ------------------------------------------------------------------------
  type OnError_  = Criticality      -> 
                   SomeException    -> 
                   String -> String -> IO ()

  -------------------------------------------------------------------------
  -- | Indicates criticality of the error event
  -------------------------------------------------------------------------
  data Criticality = 
                     -- | The current processing 
                     --   (/e.g./ answering a request)
                     --   has not properly ended,
                     --   but the service is able to continue;
                     --   the error may have been caused by a faulty
                     --   request or other temporal conditions.
                     Error 
                     -- | One worker thread is lost ('Server' only)
                   | Critical 
                     -- | The service cannot recover and will terminate
                   | Fatal
    deriving (Eq, Ord, Show, Read)

  -- | Ignore parameters
  noparam :: String
  noparam = ""

  -------------------------------------------------------------------------
  -- | How to link to an 'AccessPoint'
  -------------------------------------------------------------------------
  data LinkType = 
         -- | Bind the address
         Bind 
         -- | Connect to the address
         | Connect
    deriving (Show, Read)

  -------------------------------------------------------------------------
  -- | Safely read 'LinkType';
  --   ignores the case of the input string
  --   and, besides \"bind\" and \"connect\" 
  --   also accepts \"bin\", \"con\" and \"conn\";
  --   intended for use with command line arguments
  -------------------------------------------------------------------------
  parseLink :: String -> Maybe LinkType 
  parseLink s = case map toLower s of
                  "bind"    -> Just Bind
                  "bin"     -> Just Bind
                  "con"     -> Just Connect
                  "conn"    -> Just Connect
                  "connect" -> Just Connect
                  _         -> Nothing

  -------------------------------------------------------------------------
  -- binds or connects to the address
  -------------------------------------------------------------------------
  link :: LinkType -> AccessPoint -> Z.Socket a -> IO ()
  link t ac s = case t of
                  Bind    -> Z.bind s (acAdd ac)
                  Connect -> trycon s (acAdd ac) 10

  ------------------------------------------------------------------------
  -- | Converters are user-defined functions
  --   that convert a 'B.ByteString' to a value of type /a/ ('InBound') or
  --                a value of type /a/ to 'B.ByteString'   ('OutBound'). 
  --   Converters are, hence, similar to /put/ and /get/ in the /Binary/
  --   monad. 
  --   The reason for using explicit, user-defined converters 
  --   instead of /Binary/ /encode/ and /decode/
  --   is that the conversion 
  --   may be more complex, involving reading configurations 
  --   or other 'IO' actions.
  --
  --   The simplest possible in-bound converter for plain strings is:
  --
  --   > let iconv = return . toString
  ------------------------------------------------------------------------
  type InBound  a = B.ByteString -> IO a
  ------------------------------------------------------------------------
  -- | A simple string 'OutBound' converter may be:
  --
  --   > let oconv = return . fromString
  ------------------------------------------------------------------------
  type OutBound a = a -> IO B.ByteString

  ------------------------------------------------------------------------
  -- | 'InBound' 'B.ByteString' -> 'B.ByteString' 
  ------------------------------------------------------------------------
  idIn :: InBound B.ByteString
  idIn = return

  ------------------------------------------------------------------------
  -- | 'OutBound' 'B.ByteString' -> 'B.ByteString' 
  ------------------------------------------------------------------------
  idOut :: OutBound B.ByteString
  idOut = return

  ------------------------------------------------------------------------
  -- | 'OutBound' UTF8 String -> 'B.ByteString' 
  ------------------------------------------------------------------------
  outUTF8 :: OutBound String
  outUTF8 = return . U.fromString

  ------------------------------------------------------------------------
  -- | 'InBound' 'B.ByteString' -> UTF8 String
  ------------------------------------------------------------------------
  inUTF8 :: InBound String
  inUTF8 = return . U.toString

  ------------------------------------------------------------------------
  -- | 'OutBound' String -> 'B.ByteString'
  ------------------------------------------------------------------------
  outString :: OutBound String
  outString = return . B.pack

  ------------------------------------------------------------------------
  -- | 'InBound' 'B.ByteString' -> String
  ------------------------------------------------------------------------
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
  -- some helpers
  ------------------------------------------------------------------------
  retries :: Int
  retries = 10

  trycon :: Z.Socket a -> String -> Int -> IO ()
  trycon sock add i = catch (Z.connect sock add) 
                            (\e -> if i <= 0 
                                     then throwIO (e::SomeException)
                                     else do
                                       threadDelay 1000
                                       trycon sock add (i-1))

  ifLeft :: Either a b -> (a -> c) -> (b -> c) -> c
  ifLeft e l r = either l r e

  ------------------------------------------------------------------------
  -- | Chains IO Actions in an 'E.Enumerator' together;
  --   returns 'E.Error' when an error in the action occurs
  ------------------------------------------------------------------------
  chainIOe :: IO a -> (a -> E.Iteratee b IO c) -> E.Iteratee b IO c
  chainIOe x f = liftIO (try x) >>= \ei ->
                   case ei of
                     Left  e -> E.returnI (E.Error e)
                     Right y -> f y

  ------------------------------------------------------------------------
  -- | Chains IO Actions in an 'E.Enumerator' together;
  --   throws 'SomeException' 
  --   using 'E.throwError'
  --   when an error in the action occurs
  ------------------------------------------------------------------------
  chainIO :: IO a -> (a -> E.Iteratee b IO c) -> E.Iteratee b IO c
  chainIO x f = liftIO (try x) >>= \ei ->
                  case ei of
                    Left  e -> E.throwError (e::SomeException)
                    Right y -> f y

  ------------------------------------------------------------------------
  -- | Executes an IO Actions in an 'E.Iteratee';
  --   throws 'SomeException'
  --   using 'E.throwError' when an error in the action occurs
  ------------------------------------------------------------------------
  tryIO :: IO a -> E.Iteratee i IO a
  tryIO act = 
    liftIO (try act) >>= \ei ->
      case ei of
        Left  e -> E.throwError (e::SomeException)
        Right x -> return x

  ------------------------------------------------------------------------
  -- | Executes an IO Actions in an 'E.Iteratee';
  --   returns 'E.Error' when an error in the action occurs
  ------------------------------------------------------------------------
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

  type Millisecond = Int
  type Identifier  = String
  type OnTimeout   = IO ()
