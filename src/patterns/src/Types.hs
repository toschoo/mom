module Types (
          -- * Service Access Point
          AccessPoint(..), LinkType(..), parseLink, link,
          AccessType(..), access, safeClose, setSockOs,
          -- * PollEntry
          PollEntry(..), pollEntry,
          -- * Enumerators
          Fetch, Fetch_, 
          FetchHelper, FetchHelper', FetchHelper_, FetchHelper_',
          Dump,
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
          retries, trycon, ifLeft, (?>), ouch,
          Timeout, OnTimeout,
          Topic, alltopics, notopic,
          Identifier, Parameter, noparam)

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
  -- | Defines the type of a 'PollEntry';
  --   the names of the constructors are similar to ZMQ socket types
  --   but with some differences to keep the terminology in line
  --   with basic patterns.
  --   The leading \"X\" stands for \"Access\" 
  --   (not for \"eXtended\" as in XRep and XReq).
  ------------------------------------------------------------------------
  data AccessType = 
         -- | Represents a server and expects connections from clients;
         --   should be used with 'Bind';
         --   corresponds to ZMQ Socket Type 'Z.Rep'
         XServer    
         -- | Represents a client and connects to a server;
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

  ------------------------------------------------------------------------
  -- Creates a socket, binds or links it and sets the socket options
  ------------------------------------------------------------------------
  access :: Z.Context -> AccessType -> LinkType -> [Z.SocketOption] ->
               String -> [Topic] -> IO Z.Poll
  access ctx a l os u ts = 
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
                  mapM_ (Z.subscribe s) ts >> go s
   where go s = do setSockOs s os
                   case l of
                     Bind    -> Z.bind s u
                     Connect -> trycon s u retries
                   return $ Z.S s Z.In

  ------------------------------------------------------------------------
  -- Close without throughing an exception
  ------------------------------------------------------------------------
  safeClose :: Z.Socket a -> IO ()
  safeClose s = catch (Z.close s)
                      (\e -> let _ = (e::SomeException)
                              in return ())

  ------------------------------------------------------------------------
  -- | Describes how to access a service;
  --   an 'AccessPoint' usually consists of an address 
  --   and a list of 'Z.SocketOption'.
  --   Addresses are passed in as strings of the form:
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
  --   * \"ipc:\/\/tmp\/queues/0\": for binding and connecting to
  --                                  a local inter-process communication
  --                                  endpoint, in this case created under
  --                                  \/tmp\/queues\/0;
  --                                  only available on UNIX.
  -- 
  --   * \"inproc:\/\/worker\": for binding and connecting to
  --                            the process internal address /worker/
  --
  --   For more options, please refer to the zeromq documentation.
  ------------------------------------------------------------------------
  data AccessPoint = Address {
                       -- | Address string
                       acAdd :: String,
                       -- | Socket options
                       acOs  :: [Z.SocketOption]}
  
  instance Show AccessPoint where
    show (Address s _) = s
  
  ------------------------------------------------------------------------
  -- | A poll entry describes how to handle an 'AccessPoint'
  ------------------------------------------------------------------------
  data PollEntry = Poll {
                     pollId   :: Identifier,
                     pollAdd  :: String,
                     pollType :: AccessType,
                     pollLink :: LinkType,
                     pollSub  :: [Topic],
                     pollOs   :: [Z.SocketOption]
                   }
    deriving (Show, Read)

  instance Eq PollEntry where
    x == y = pollId x == pollId y

  ------------------------------------------------------------------------
  -- | Creates a 'PollEntry';
  --
  --   Parameters:
  --
  --   * 'Identifier': identifies an 'AccessPoint'; 
  --                   the identifier shall be unique within the device.
  --
  --   * 'AccessType': the 'AccessType' of this 'AccessPoint'
  --
  --   * 'AccessPoint': the 'AccessPoint'
  --
  --   * 'LinkType': how to link to this 'AccessPoint'
  --
  --   * ['Topic']: The subscription topics - 
  --                ignored for all poll entries, but those
  --                with 'AccessType' 'XSub' 
  ------------------------------------------------------------------------
  pollEntry :: Identifier -> 
               AccessType -> AccessPoint -> LinkType ->
               [Topic]    -> PollEntry
  pollEntry i at ac lt sub = Poll {
                               pollId   = i,
                               pollAdd  = acAdd ac,
                               pollType = at,
                               pollLink = lt,
                               pollSub  = sub,
                               pollOs   = acOs ac}

  ------------------------------------------------------------------------
  -- | 'E.Enumerator' to process data segments of type /o/;
  --   receives the 'Z.Context', the control parameter 
  --   and an input of type /i/;
  --   'Fetch' is used by 'Server's that receive requests of type /i/
  --   and produce an outgoing stream with segments of type /o/.
  ------------------------------------------------------------------------
  type Fetch       i o = Z.Context -> Parameter -> i -> E.Enumerator o IO ()

  ------------------------------------------------------------------------
  -- | A variant of 'Fetch' without input
  ------------------------------------------------------------------------
  type Fetch_        o = Fetch () o

  ------------------------------------------------------------------------
  -- | A function that may be used with some of the fetchers;
  --   The helper returns 'Nothing' to signal 
  --   that no more data are available
  --   and 'Just' /o/ to continue the stream.
  --   FetchHelpers are used with 'Server's 
  --   that receive requests of type /i/.
  --   The function 
  --   receives the 'Z.Context', the conrol parameter
  --   and an input of type /i/;
  ------------------------------------------------------------------------
  type FetchHelper i o = Z.Context -> Parameter -> i -> IO (Maybe o)

  ------------------------------------------------------------------------
  -- | A variant of 'FetchHelper' that returns type /o/ instead of
  --   'Maybe' /o/.
  --   Please note that /'/ does not mean /strict/, here;
  --   it just means that the result is not a 'Maybe'.
  ------------------------------------------------------------------------
  type FetchHelper' i o = Z.Context -> Parameter -> i -> IO o

  ------------------------------------------------------------------------
  -- | A variant of 'FetchHelper' without input
  ------------------------------------------------------------------------
  type FetchHelper_  o = FetchHelper () o

  ------------------------------------------------------------------------
  -- | A variant of 'FetchHelper_' that returns type /o/ instead of
  --   'Maybe' /o/.
  --   Please note that /'/ does not mean /strict/, here;
  --   it just means that the result is not a 'Maybe'.
  ------------------------------------------------------------------------
  type FetchHelper_' o = FetchHelper' () o

  ------------------------------------------------------------------------
  -- | 'E.Iteratee' to process data segments of type /i/;
  --   receives the 'Z.Context' and the control parameter
  ------------------------------------------------------------------------
  type Dump i = Z.Context -> Parameter -> E.Iteratee i IO ()

  ------------------------------------------------------------------------
  -- | Error handler for servers;
  --   receives the 'Criticality' of the error event,
  --   the exception, the server name and the service control parameter.
  --   If the error handler returns 'Just' a 'B.ByteString'
  --   this 'B.ByteString' is sent to the client as error message.
  --   
  --   A good policy for implementing servers is
  --   to terminate or restart the 'Server'
  --   when a 'Fatal' or 'Critical' error occurs
  --   and to send an error message to the client
  --   on a plain 'Error'.
  --   The error handler, additionally, may log the incident
  --   or inform an administrator.
  ------------------------------------------------------------------------
  type OnError   = Criticality         -> 
                   SomeException       -> 
                   String -> Parameter -> IO (Maybe B.ByteString)

  ------------------------------------------------------------------------
  -- | Error handler for all services but servers;
  --   receives the 'Criticality' of the error event,
  --   the exception, the service name 
  --   and the service control parameter.
  --   
  --   A good policy is
  --   to terminate or restart the service
  --   when a 'Fatal' error occurs
  --   and to continue, if possible,
  --   on a plain 'Error'.
  --   The error handler, additionally, may log the incident
  --   or inform an administrator.
  ------------------------------------------------------------------------
  type OnError_  = Criticality         -> 
                   SomeException       -> 
                   String -> Parameter -> IO ()

  -------------------------------------------------------------------------
  -- | Indicates criticality of the error event
  -------------------------------------------------------------------------
  data Criticality = 
                     -- | The current operation 
                     --   (/e.g./ processing a request)
                     --   has not terminated properly,
                     --   but the service is able to continue;
                     --   the error may have been caused by a faulty
                     --   request or other temporal conditions.
                     --   Note that if an application-defined 'E.Iteratee' or
                     --   'E.Enumerator' results in 'SomeException'
                     --   (by means of 'E.throwError'),
                     --   the incident is classified as 'Error';
                     --   if it throws an IO Error, however,
                     --   the incident is classified as 'Fatal'.
                     Error 
                     -- | One worker thread is lost ('Server' only)
                   | Critical 
                     -- | The service cannot recover and will terminate
                   | Fatal
    deriving (Eq, Ord, Show, Read)

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
  --   and, besides \"bind\" and \"connect\", 
  --   also accepts \"bin\", \"con\" and \"conn\";
  --   intended for use with command line parameters
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
  link t ac s = do setSockOs s (acOs ac) 
                   case t of
                     Bind    -> Z.bind s (acAdd ac)
                     Connect -> trycon s (acAdd ac) 10

  -------------------------------------------------------------------------
  -- Sets Socket Options
  -------------------------------------------------------------------------
  setSockOs :: Z.Socket a -> [Z.SocketOption] -> IO ()
  setSockOs s = mapM_ (Z.setOption s)

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
              Nothing -> return ()
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
  retries = 100

  ------------------------------------------------------------------------
  -- try n times to connect
  -- this is particularly useful for "inproc" sockets:
  -- the socket, in this case, must be bound before we can connect to it.
  ------------------------------------------------------------------------
  trycon :: Z.Socket a -> String -> Int -> IO ()
  trycon sock add i = catch (Z.connect sock add) 
                            (\e -> if i <= 0 
                                     then throwIO (e::SomeException)
                                     else do
                                       threadDelay 1000
                                       trycon sock add (i-1))

  ------------------------------------------------------------------------
  -- either with the arguments flipped 
  ------------------------------------------------------------------------
  ifLeft :: Either a b -> (a -> c) -> (b -> c) -> c
  ifLeft e l r = either l r e

  ------------------------------------------------------------------------
  -- | Chains IO Actions in an 'E.Enumerator' together;
  --   returns 'E.Error' when an error occurs
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
  --   when an error occurs
  ------------------------------------------------------------------------
  chainIO :: IO a -> (a -> E.Iteratee b IO c) -> E.Iteratee b IO c
  chainIO x f = liftIO (try x) >>= \ei ->
                  case ei of
                    Left  e -> E.throwError (e::SomeException)
                    Right y -> f y

  ------------------------------------------------------------------------
  -- | Executes an IO Actions in an 'E.Iteratee';
  --   throws 'SomeException'
  --   using 'E.throwError' when an error occurs
  ------------------------------------------------------------------------
  tryIO :: IO a -> E.Iteratee i IO a
  tryIO act = 
    liftIO (try act) >>= \ei ->
      case ei of
        Left  e -> E.throwError (e::SomeException)
        Right x -> return x

  ------------------------------------------------------------------------
  -- | Executes an IO Actions in an 'E.Iteratee';
  --   returns 'E.Error' when an error occurs
  ------------------------------------------------------------------------
  tryIOe :: IO a -> E.Iteratee i IO a
  tryIOe act = 
    liftIO (try act) >>= \ei ->
      case ei of
        Left  e -> E.returnI (E.Error e)
        Right x -> return x

  ------------------------------------------------------------------------
  -- Ease working with either
  ------------------------------------------------------------------------
  eiCombine :: IO (Either a b) -> (b -> IO (Either a c)) -> IO (Either a c)
  eiCombine x f = x >>= \mbx ->
                  case mbx of
                    Left  e -> return $ Left e
                    Right y -> f y

  infixl 9 ?>
  (?>) :: IO (Either a b) -> (b -> IO (Either a c)) -> IO (Either a c)
  (?>) = eiCombine

  ------------------------------------------------------------------------
  -- Ouch message
  ------------------------------------------------------------------------
  ouch :: String -> a
  ouch s = error $ "Ouch! You hit a bug, please report: " ++ s

  -- | A device identifier is just a plain 'String'
  type Identifier  = String

  -- | A timeout action is just an IO action without arguments
  type OnTimeout   = IO ()

  -- | Control Parameter
  type Parameter = String

  -- | Ignore parameter
  noparam :: Parameter
  noparam = ""

  -- | Subscription Topic
  type Topic = String

  -- | Subscribe to all topics
  alltopics :: [Topic]
  alltopics = [""]

  -- | Subscribe to no topic
  notopic :: [Topic]
  notopic = []
