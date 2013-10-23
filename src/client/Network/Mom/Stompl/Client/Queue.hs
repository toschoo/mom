-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Stompl/Client/Queue.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- The Stomp Protocol specifies message-oriented interoperability.
-- Applications connect to a message broker to send (publish)
-- or receive (subscribe) messages through queues. 
-- Interoperating applications do not know 
-- the location or internal structure of each other.
-- They only see interfaces, /i.e./ the messages
-- published and subscribed through the broker.
-- 
-- The Stompl Client library implements
-- a Stomp client using abstractions like
-- 'Connection', 'Transaction' and
-- queues in terms of 'Reader' and 'Writer'.
-------------------------------------------------------------------------------
module Network.Mom.Stompl.Client.Queue (
                   -- * Connections
                   -- $stomp_con
                   withConnection, 
                   Factory.Con, 
                   F.Heart,
                   Copt(..),
                   -- * Queues
                   -- $stomp_queues
                   Reader, Writer, 
                   newReader, newWriter, 
                   withReader, withWriter, withPair, ReaderDesc, WriterDesc,
                   Qopt(..), F.AckMode(..), 
                   InBound, OutBound, 
                   readQ, 
                   writeQ, writeQWith,
                   writeAdHoc, writeAdHocWith,
                   -- * Messages
                   P.Message, 
                   msgContent, P.msgRaw, 
                   P.msgType, P.msgLen, P.msgHdrs,
                   -- * Receipts
                   -- $stomp_receipts
                   Factory.Rec(..), Receipt,
                   waitReceipt,
                   -- * Transactions
                   -- $stomp_trans
                   Tx,
                   withTransaction,
                   Topt(..), abort, 
                   -- * Acknowledgments
                   -- $stomp_acks
                   ack, ackWith, nack, nackWith,
                   -- * Exceptions
                   module Network.Mom.Stompl.Client.Exception
                   -- * Complete Example
                   -- $stomp_sample
                   )

where
  ----------------------------------------------------------------
  -- todo
  -- -- pass a logger (name) to withConnection
  -- - test/check for deadlocks
  ----------------------------------------------------------------

  import qualified Socket   as S
  import qualified Protocol as P
  import           Factory  
  import           State

  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Client.Exception

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U
  import           Data.List (find)
  import           Data.Time.Clock
  import           Data.Maybe (isJust, fromJust)

  import           Control.Concurrent 
  import           Control.Applicative ((<$>))
  import           Control.Monad
  import           Control.Exception (bracket, finally, catches, onException,
                                      AsyncException(..), Handler(..),
                                      throwIO, SomeException)

  import           Codec.MIME.Type as Mime (Type)
  import           System.Timeout (timeout)

  {- $stomp_con

     The Stomp protocol is connection-oriented and
     usually implemented on top of /TCP/\//IP/.
     The client initialises the connection
     by sending a /connect/ message which is answered
     by the broker by confirming or rejecting the connection.
     The connection is authenticated by user and passcode.
     The authentication mechanism, however, 
     varies among brokers.

     During the connection phase,
     the protocol version and a heartbeat 
     that defines the frequency of /alive/ messages
     exchanged between broker and client
     are negotiated.

     The connection remains active until either the client
     disconnects voluntarily or the broker disconnects
     in consequence of a protocol error.

     The details of the connection, including 
     protocol version and heartbeats are handled
     internally by the Stompl Client library.
  -}

  {- $stomp_queues

     Stomp program interoperability is based on queues.
     Queues are communication channels of arbitrary size
     that may be written by any client 
     currently connected to the broker.
     Messages in the queue are stored in /FIFO/ order.
     The process of adding messages to the queue is called /send/.
     In order to read from a queue, a client has to /subscribe/ to it.
     After having subscribed to a queue, 
     a client will receive all message sent to it.
     Brokers may implement additional selection criteria
     by means of /selectors/ that are expressed in some
     query language, such as /SQL/ or /XPath/.

     There are two different flavours of queues
     distinguished by their communication pattern:
     Queues are either one-to-one channels,
     this is, a message published in this queue
     is sent to exactly one subscriber
     and then removed from it; 
     or queues may be one-to-many,
     /i.e./, a message published in this queue
     is sent to all current subscribers of the queue.
     This type of queues is sometimes called /topic/.
     Which pattern is supported 
     and how patterns are controlled, depends on the broker. 

     From the perspective of the Stomp protocol,
     the content of messages in a queue
     has no format.
     The Protocol describes only those aspects of messages
     that are related to their handling;
     this can be seen as a /syntactic/ level of interoperability.
     Introducing meaning to message contents
     is entirely left to applications.
     Message- or service-oriented frameworks,
     usually, define formats and encodings 
     to describe messages and higher-level
     communication patterns built on top of them,
     to add more /syntactic/ formalism or
     to raise interoperability
     to a /semantic/ or even /pragmatic/ level.

     The Stompl library stresses the importance
     of adding meaning to the message content
     by adding types to queues. 
     From the perspective of the client Haskell program,
     a queue is a communication channel
     that allows sending and receiving messages of a given type.
     This adds type-safety to Stompl queues, which,
     otherwise, would just return plain bytes.
     It is, on the other hand, always possible
     to ignore this feature by declaring queues
     as '()' or 'B.ByteString'.
     In the first case, the /raw/ bytestring
     may be read from the 'P.Message';
     in the second case, the contents of the 'P.Message'
     will be a 'B.ByteString'.

     In the Stompl library, queues 
     are unidirectional communication channels
     either for reading or writing.
     This is captured by implementing queues
     with two different data types,
     a 'Reader' and a 'Writer'.
     On creating a queue a set of parameters can be defined
     to control the behaviour of the queue.
  -}

  {- $stomp_receipts

     Receipts are identifiers unique during the life time
     of an application; receipts can be added to all kinds of
     messages sent to the broker.
     The broker, in its turn, uses receipts to acknowledge received messages.
     Receipts, hence, are useful to make a session more reliable.
     When the broker has confirmed the receipt of a frame sent to it,
     the client application can be sure that it has arrived.
     What kind of additional guarantees are made,
     /e.g./ that the frame is saved to disk or has already been sent
     to the subscriber(s), depends on the broker.

     Receipts are handled internally by the library.
     The application, however, decides where receipts should
     be requested, /i.e./ on subcribing to a queue,
     on sending a message, on sending /acks/ and on
     starting and ending transactions.
     On sending messages, 
     receipt handling can be made explict.
     The function 'writeQWith'
     requests a receipt to the message
     and returns it to the caller.
     The application can then, later,
     explicitly wait for the receipt, using 'waitReceipt'.
     Otherwise, receipt handling remains
     inivisible in the application code.
  -}

  {- $stomp_trans

     Transactions are units of interactions with
     a Stomp broker, including sending messages to queues
     and acknowledging the receipt of messages.
     All messages sent during a transaction
     are buffered in the broker.
     Only when the application terminates the transaction
     with /commit/ the messages will be eventually processed.
     If an error occurs during the transaction,
     it can be /aborted/ by the client. 
     Transactions, in consequence, can be used
     to ensure atomicity,
     /i.e./ either all single steps are performed or 
     no step is performed.

     In the Stompl Client library, transactions
     are sequences of Stompl actions, 
     queue operations as well as nested transactions,
     that are committed at the end or aborted,
     whenever an error condition becomes true.
     Error conditions are uncaught exceptions
     and conditions defined by options passed
     to the transaction, for example
     that all receipts requested during the transaction,
     have been confirmed by the broker.

     To enforce atomicity, 
     threads are not allowed to share transactions.
  -}

  {- $stomp_acks

     Acknowledgements are used by the client to confirm the receipt
     of a message. The Stomp protocol foresees three different
     acknowledgment modes, defined when the client subscribes to a queues.
     A subscription may use 
     /auto mode/, /i.e./ a message is considered acknowledged
     when it has been sent to the subscriber;
     /client mode/, /i.e./ a message is considered acknowledged
     only when an /ack/ message has been sent back from the client.
     Note that client mode is cumulative, that means, 
     the broker will consider all messages acknowledged 
     that have been sent
     from the previous ack up to the acknowledged message;
     or /client-individual mode/, /i.e./ non-cumulative
     client mode.
     
     A message may also be /negatively acknowledged/ (/nack/). 
     How the broker handles a /nack/, however,
     is not further specified by the Stomp protocol.
  -}

  {- $stomp_sample

     > import Network.Mom.Stompl.Client.Queue
     >
     > import System.Environment (getArgs)
     > import Network.Socket (withSocketsDo)
     > import Control.Monad (forever)
     > import Control.Concurrent (threadDelay)
     > import qualified Data.ByteString.UTF8  as U
     > import Data.Char(toUpper)
     > import Codec.MIME.Type (nullType)
     >
     > main :: IO ()
     > main = do
     >   os <- getArgs
     >   case os of
     >     [q] -> withSocketsDo $ ping q
     >     _   -> putStrLn "I need a queue name!"
     >            -- error handling...
     > 
     > data Ping = Ping | Pong
     >   deriving (Show)
     >
     > strToPing :: String -> IO Ping
     > strToPing s = case map toUpper s of
     >                 "PING" -> return Ping
     >                 "PONG" -> return Pong
     >                 _      -> convertError $ "Not a Ping: '" ++ s ++ "'"
     >
     > ping :: String -> IO ()
     > ping qn = 
     >   withConnection "localhost" 61613 [] [] $ \c -> do
     >     let iconv _ _ _ = strToPing . U.toString
     >     let oconv       = return    . U.fromString . show
     >     inQ  <- newReader c "Q-IN"  qn [] [] iconv
     >     outQ <- newWriter c "Q-OUT" qn [] [] oconv
     >     writeQ outQ nullType [] Pong
     >     listen inQ outQ
     >
     > listen  :: Reader Ping -> Writer Ping -> IO ()
     > listen iQ oQ = forever $ do
     >   eiM <- try $ readQ iQ 
     >   case eiM of
     >     Left  e -> do
     >       putStrLn $ "Error: " ++ show e
     >       -- error handling ...
     >     Right m -> do
     >       let p = case msgContent m of
     >                 Ping -> Pong
     >                 Pong -> Ping
     >       putStrLn $ show p
     >       writeQ oQ nullType [] p
     >       threadDelay 10000
  -}

  -- The versions, we support
  vers :: [F.Version]
  vers = [(1,0), (1,1)]

  ------------------------------------------------------------------------
  -- | Initialises a connection and executes an 'IO' action.
  --   The connection life time is the scope of this action.
  --   The connection handle, 'Con', that is passed to the action
  --   should not be returned from 'withConnection'.
  --   Connections, however, can be shared among threads.
  --   In this case, the programmer has to take care
  --   not to terminate the action before all other threads
  --   working on the connection have finished.
  --
  --   Since 'Connection' is a heavy data type,
  --   you should try to reduce the number of connections
  --   to the same broker within the same process - 
  --   there is ideally only one connection per broker
  --   in one process.
  --
  --   Paramter:
  --
  --   * 'String': The broker's hostname or IP-address
  --
  --   * 'Int': The broker's port
  --
  --   * 'Copt': Control options passed to the connection
  --             (including user/password)
  --
  --   * 'Header': List of additional, broker-specific headers
  --
  --   * ('Con' -> 'IO' a): The action to execute.
  --                        The action receives the connection handle
  --                        and returns a value of type /a/ 
  --                        in the 'IO' monad.
  --
  -- 'withConnection' returns the result of the action passed into it.
  --
  -- 'withConnection' will always disconnect from the broker 
  -- when the action has terminated, even if an exception is raised.
  --
  -- Example:
  --
  -- > withConnection "localhost" 61613 [] [] $ \c -> do
  --
  -- This would connect to a broker listening to the loopback interface,
  -- port number 61613.
  -- The action is defined after the /hanging do/.
  --
  -- Internally, connections use concurrent threads;
  -- errors are communicated by throwing exceptions
  -- to the owner of the connection, where
  -- the owner is the thread that created the connection
  -- by calling 'withConnection'.
  -- It is therefore advisable to start different connections
  -- in different threads, so that each thread will receive
  -- only exceptions related to the connection it has opened.
  -- 
  -- Example:
  --
  -- > t <- forkIO $ withConnection "127.0.0.1" 61613 [] [] $ \c -> do
  ------------------------------------------------------------------------
  withConnection :: String -> Int -> [Copt] -> [F.Header] ->
                    (Con -> IO a) -> IO a
  withConnection host port os hs act = do
    let beat  = oHeartBeat os
    let mx    = oMaxRecv   os
    let (u,p) = oAuth      os
    let (ci)  = oCliId     os
    bracket (P.connect host port mx u p ci vers beat hs)
            P.disc -- important: At the end, we close the socket!
            (whenConnected os act)

  whenConnected :: [Copt] -> (Con -> IO a) -> P.Connection -> IO a
  whenConnected os act c = 
    if not $ P.connected c 
      then throwIO $ ConnectException $ P.getErr c
      else do
        cid <- mkUniqueConId  -- connection id
        me  <- myThreadId     -- connection owner
        now <- getCurrentTime -- heartbeat
        finally (do addCon $ mkConnection cid c me now os
                    whenListening cid $ whenBeating cid)
                (rmCon cid)
    where period = snd . P.conBeat 
          whenListening cid = bracket (forkIO $ listen cid) killThread
          whenBeating cid _ = bracket 
            (if period c > 0 
               then Just <$> forkIO (heartBeat cid $ period c)
               else return Nothing)
            (\b -> when (isThrd b) (killThread $ thrd b))
            (\_ -> do r  <- act cid
                      c' <- getCon cid
                      -- wait for receipt on disconnect ----
                      if conWait c' <= 0 then return r
                        else do
                          rc <- mkUniqueRecc 
                          _  <- P.disconnect c (show rc)
                          addRec cid rc
                          waitCon cid rc (conWait c') `onException` rmRec cid rc
                          return r)

  isThrd :: Maybe a -> Bool
  isThrd = isJust

  thrd :: Maybe a -> a
  thrd = fromJust

  ------------------------------------------------------------------------
  -- wait for receipt
  ------------------------------------------------------------------------
  waitCon :: Con -> Receipt -> Int -> IO ()
  waitCon cid rc delay = do
    c <- getCon cid
    case find (==rc) $ conRecs c of
      Nothing -> return ()
      Just _  -> 
        if delay <= 0 
          then throwIO $ ConnectException $ 
                            "No receipt on disconnect (" ++ 
                            show cid ++ ")."
          else do
            threadDelay $ ms 1
            waitCon cid rc (delay - 1)
    
  ------------------------------------------------------------------------
  -- | A Queue for sending messages.
  ------------------------------------------------------------------------
  data Writer a = SendQ {
                   wCon  :: Con,
                   wDest :: String,
                   wName :: String,
                   wRec  :: Bool,
                   wWait :: Bool,
                   wTx   :: Bool,
                   wTo   :: OutBound a}

  ------------------------------------------------------------------------
  -- | A Queue for receiving messages
  ------------------------------------------------------------------------
  data Reader a = RecvQ {
                   rCon  :: Con,
                   rSub  :: Sub,
                   rDest :: String,
                   rName :: String,
                   rMode :: F.AckMode,
                   rAuto :: Bool, -- library creates Ack
                   rRec  :: Bool,
                   rFrom :: InBound a}

  instance Eq (Reader a) where
    q1 == q2 = rName q1 == rName q2

  instance Eq (Writer a) where
    q1 == q2 = wName q1 == wName q2

  ------------------------------------------------------------------------
  -- | Options that may be passed 
  --   to 'newReader' and 'newWriter' and their variants.
  ------------------------------------------------------------------------
  data Qopt = 
            -- | A queue created with 'OWithReceipt' will request a receipt
            --   on all interactions with the broker.
            --   The handling of receipts is usually transparent to applications, 
            --   but, in the case of sending message, may be made visible 
            --   by using 'writeQWith' instead of 'writeQ'.
            --   'writeQWith' return the receipt identifier
            --   and the application can later invoke 'waitReceipt'
            --   to wait for the broker confirming this receipt.
            --   Note that a 'Reader' created with 'OWithReceipt' will 
            --   issue a request for receipt when subscribing to a Stomp queue.
            OWithReceipt 
            -- | A queue created with 'OWaitReceipt' will wait for the receipt
            --   before returning from a call that has issued a request for receipt.
            --   This implies that the current thread will yield the processor.
            --   'writeQ' will internally create a request for receipt and 
            --   wait for the broker to confirm the receipt before returning.
            --   Note that, for 'newReader', there is no difference between
            --   'OWaitReceipt' and 'OWithReceipt'. Either option will cause
            --   the thread to preempt until the receipt is confirmed.
            --
            --   On writing a message, this is not always the preferred
            --   method. You may want to fire and forget - and check 
            --   for the confirmation of the receipt only later.
            --   In this case, you will create the
            --   'Writer' with 'OWithReceipt' only and, later, after having
            --   sent a message with 'writeQWith', wait for the receipt using
            --   'waitReceipt'. Note that 'OWaitReceipt' without 'OWithReceipt'
            --   has no meaning with 'writeQ' and 'writeQWith'. 
            --   If you want to request a receipt with a message
            --   and wait for the broker to confirm it, you have to use 
            --   both options.
            --
            --   It is good practice to use /timeout/ with all calls
            --   that may wait for receipts, 
            --   /ie/ 'newReader' and 'withReader' 
            --   with options 'OWithReceipt' or 'OWaitReceipt',
            --   or 'writeQ' and 'writeQWith' with options 'OWaitReceipt',
            --   or 'ackWith' and 'nackWith'.
            | OWaitReceipt 
            -- | The option defines the 'F.AckMode' of the queue,
            --   which is relevant for 'Reader' only.
            --   'F.AckMode' is one of: 
            --   'F.Auto', 'F.Client', 'F.ClientIndi'.
            --
            --   If 'OMode' is not given, 'F.Auto' is assumed as default.
            --
            --   For more details, see 'F.AckMode'.
            | OMode F.AckMode  
            -- | Expression often used by Ren&#x00e9; Artois.
            --   Furthermore, if 'OMode' is either
            --   'F.Client' or 'F.ClientIndi', then 
            --   this option forces 'readQ' to send an acknowledgment
            --   automatically when a message has been read from the queue. 
            | OAck
            -- | A queue created with 'OForceTx' will throw 
            --   'QueueException' when used outside a 'Transaction'.
            | OForceTx
    deriving (Show, Read, Eq) 

  hasQopt :: Qopt -> [Qopt] -> Bool
  hasQopt o os = o `elem` os

  ackMode :: [Qopt] -> F.AckMode
  ackMode os = case find isMode os of
                 Just (OMode x) -> x
                 _              -> F.Auto
    where isMode x = case x of
                       OMode _ -> True
                       _       -> False

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
  type InBound  a = Mime.Type -> Int -> [F.Header] -> B.ByteString -> IO a
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
  -- | Creates a 'Reader' with the life time of the connection 'Con'.
  --   Creating a receiving queue involves interaction with the broker;
  --   this may result in preempting the calling thread, 
  --   depending on the options ['Qopt'].
  --   
  --   Parameters:
  --
  --   * The connection handle 'Con'
  --
  --   * A queue name that should be unique in your application.
  --     The queue name is useful for debugging, since it appears
  --     in error messages.
  --
  --   * The Stomp destination, /i.e./ the name of the queue
  --     as it is known to the broker and other applications.
  --
  --   * A list of options ('Qopt').
  --
  --   * A list of headers ('F.Header'), 
  --     which will be passed to the broker.
  --     the 'F.Header' parameter is actually a breach in the abstraction
  --     from the Stomp protocol. A header may be, for instance,
  --     a selector that restricts the subscription to this queue,
  --     such that only messages with certain attributes 
  --     (/i.e./ specific headers) are sent to the subscribing client.
  --     Selectors are broker-specific and typically expressed
  --     as SQL or XPath.
  --
  --   * An in-bound converter.
  --
  --   A usage example to create a 'Reader'
  --   with 'Connection' /c/ and the in-bound converter
  --   /iconv/ would be:
  --
  --   > q <- newReader c "TestQ" "/queue/test" [] [] iconv
  --
  --   A call to 'newReader' may result in preemption when
  --   one of the options 'OWaitReceipt' or 'OWithReceipt' are given;
  --   an example for such a call 
  --   with /tmo/ an 'Int' value representing a /timeout/
  --   in microseconds and 
  --   the result /mbQ/ of type 'Maybe' is:
  --
  --   > mbQ <- timeout tmo $ newReader c "TestQ" "/queue/test" [OWaitReceipt] [] oconv
  --   > case mbQ of
  --   >   Nothing -> -- handle error
  --   >   Just q  -> do -- ...
  ------------------------------------------------------------------------
  newReader :: Con -> String -> String -> [Qopt] -> [F.Header] -> 
               InBound a -> IO (Reader a)
  newReader cid qn dst os hs conv = do
    c <- getCon cid
    if not $ P.connected (conCon c)
      then throwIO $ ConnectException $ 
                 "Not connected (" ++ show cid ++ ")"
      else newRecvQ cid c qn dst os hs conv
  
  ------------------------------------------------------------------------
  -- | Creates a 'Writer' with the life time of the connection 'Con'.
  --   Creating a sending queue does not involve interaction with the broker
  --   and will not preempt the calling thread.
  --   
  --   A sending queue may be created like in the following
  --   code fragment, where /oconv/ is 
  --   an already defined out-bound converter:
  --
  --   > q <- newWriter c "TestQ" "/queue/test" [] [] oconv
  ------------------------------------------------------------------------
  newWriter :: Con -> String -> String -> [Qopt] -> [F.Header] -> 
               OutBound a -> IO (Writer a)
  newWriter cid qn dst os _ conv = do
    c <- getCon cid
    if not $ P.connected (conCon c)
      then throwIO $ ConnectException $ 
                 "Not connected (" ++ show cid ++ ")"
      else newSendQ cid qn dst os conv

  ------------------------------------------------------------------------
  -- | Creates a 'Reader' with limited life time. 
  --   The queue will live only in the scope of the action
  --   that is passed as last parameter. 
  --   The function is useful for readers
  --   that are used only temporarly, /e.g./ during initialisation.
  --   When the action terminates, the client unsubscribes from 
  --   the broker queue - even if an exception is raised.
  --
  --   'withReader' returns the result of the action.
  --   Since the life time of the queue is limited to the action,
  --   it should not be returned.
  --   Any operation on a reader created by 'withReader'
  --   outside the action will raise 'QueueException'.
  --
  --   A usage example is: 
  --
  --   > x <- withReader c "TestQ" "/queue/test" [] [] iconv $ \q -> do
  ------------------------------------------------------------------------
  withReader :: Con -> String    -> 
                       String    -> [Qopt] -> [F.Header] -> 
                       InBound i -> (Reader i -> IO r)   -> IO r
  withReader cid qn dst os hs conv act = do
    q <- newReader cid qn dst os hs conv
    act q `finally` unsub q

  ------------------------------------------------------------------------
  -- | Creates a 'Writer' with limited life time. 
  --   The queue will live only in the scope of the action
  --   that is passed as last parameter. 
  --   The function is useful for writers
  --   that are used only temporarly, /e.g./ during initialisation.
  --
  --   'withWriter' returns the result of the action.
  --   Since the life time of the queue is limited to the action,
  --   it should not be returned.
  --   Any operation on a writer created by 'withWriter'
  --   outside the action will raise a 'QueueException'.
  ------------------------------------------------------------------------
  withWriter :: Con -> String     -> 
                       String     -> [Qopt] -> [F.Header] -> 
                       OutBound o -> (Writer o -> IO r)   -> IO r
  withWriter cid qn dst os hs conv act = 
    newWriter cid qn dst os hs conv >>= act

  ------------------------------------------------------------------------
  -- | Creates a pair of ('Reader' i, 'Writer' o) with limited lifetime. 
  --   The pair will live only in the scope of the action
  --   that is passed as last parameter. 
  --   The function is useful for readers\/writers
  --   used in combination, /e.g./ to emulate a client\/server
  --   kind of communication.
  --
  --   'withPair' returns the result of the action passed in.
  --
  --   The parameters are:
  --
  --   * The connection handle 'Con'
  --
  --   * The name of the pair; 
  --     the reader will be identified by a string
  --                with \"_r\" added to this name,
  --     the writer by a string with \"_w\" added to this name.
  --
  --   * The description of the 'Reader', 'ReaderDesc'
  --
  --   * The description of the 'Writer', 'WriterDesc'
  --
  --   * The application-defined action
  --
  --  The reason for introducing the reader and writer description
  --  is to provide error detection at compile time:
  --  It is this way much more difficult to accidently confuse
  --  the writer's and the reader's parameters (/e.g./ 
  --  passing the writer's 'Qopt's to the reader).
  ------------------------------------------------------------------------
  withPair :: Con -> String  ->  ReaderDesc i -> 
                                 WriterDesc o ->
                                 ((Reader i, Writer o) -> IO r) -> IO r
  withPair cid n (rq,ro,rh,iconv)
                 (wq,wo,wh,oconv) act = 
    withReader cid (n ++ "_r") rq ro rh iconv $ \r ->
      withWriter cid (n ++ "_w") wq wo wh oconv $ \w -> act (r,w)

  ------------------------------------------------------------------------
  -- | The 'Reader' parameters of 'withPair':
  --
  --     * The reader's queue name 
  --
  --     * The reader's 'Qopt's
  --
  --     * The reader's 'Header's
  --
  --     * The reader's (inbound) converter
  ------------------------------------------------------------------------
  type ReaderDesc i = (String, [Qopt], [F.Header], InBound  i)

  ------------------------------------------------------------------------
  -- | The 'Writer' parameters of 'withPair'
  --
  --     * The writer's queue name
  --
  --     * The writer's 'Qopt's
  --
  --     * The writer's 'Header's
  --
  --     * The writer's (outbound) converter
  ------------------------------------------------------------------------
  type WriterDesc o = (String, [Qopt], [F.Header], OutBound o)

  ------------------------------------------------------------------------
  -- Creating a SendQ is plain and simple.
  ------------------------------------------------------------------------
  newSendQ :: Con -> String -> String -> [Qopt] -> 
              OutBound a -> IO (Writer a)
  newSendQ cid qn dst os conv = 
    return SendQ {
              wCon  = cid,
              wDest = dst,
              wName = qn,
              wRec  = hasQopt OWithReceipt os, 
              wWait = hasQopt OWaitReceipt os, 
              wTx   = hasQopt OForceTx     os, 
              wTo   = conv}

  ------------------------------------------------------------------------
  -- Creating a ReceivQ, however, involves some more hassle,
  -- in particular 'IO'.
  ------------------------------------------------------------------------
  newRecvQ :: Con        -> Connection -> String -> String -> 
              [Qopt]     -> [F.Header] ->
              InBound a  -> IO (Reader a)
  newRecvQ cid c qn dst os hs conv = do
    let am   = ackMode os
    let au   = hasQopt OAck os
    let with = hasQopt OWithReceipt os || hasQopt OWaitReceipt os
    sid <- mkUniqueSubId
    rc  <- if with then mkUniqueRecc else return NoRec
    logSend cid
    P.subscribe (conCon c) (P.mkSub sid dst am) (show rc) hs
    ch <- newChan 
    addSub  cid (sid, ch) 
    addDest cid (dst, ch) 
    let q = RecvQ {
               rCon  = cid,
               rSub  = sid,
               rDest = dst,
               rName = qn,
               rMode = am,
               rAuto = au,
               rRec  = with,
               rFrom = conv}
    when with $ waitReceipt cid rc
    return q

  ------------------------------------------------------------------------
  -- Unsubscribe a queue
  ------------------------------------------------------------------------
  unsub :: Reader a -> IO ()
  unsub q = do
    let cid = rCon  q
    let sid = rSub  q
    let dst = rDest q
    rc <- if rRec q then mkUniqueRecc else return NoRec
    c  <- getCon cid
    logSend cid
    finally (P.unsubscribe (conCon c) 
                           (P.mkSub sid dst F.Client)
                           (show rc) [])
            (do rmSub  cid sid
                rmDest cid dst)
    when (rRec q) $ waitReceipt cid rc

  ------------------------------------------------------------------------
  -- | Removes the oldest message from the queue
  --   and returns it as 'P.Message'.
  --   The message cannot be read from the queue
  --   by another call to 'readQ' within the same connection.
  --   Wether other connections will receive the message as well
  --   depends on the broker and the queue patterns it implements.
  --   If the queue is currently empty,
  --   the thread will preempt until a message arrives.
  --
  --   If the queue was created with 
  --   'OMode' other than 'F.Auto' 
  --   and with 'OAck', then an /ack/ 
  --   will be automatically sent to the broker;
  --   if 'OAck' was not set,
  --   the message will be registered as pending /ack/.
  --
  --   Note that, when 'readQ' sends an /ack/ internally,
  --   it will not request a receipt from the broker.
  --   The rationale for this design is simplicity.
  --   If the function expected a receipt, 
  --   it would have to either wait for the receipt
  --   or return it.
  --   In the first case, it would be difficult
  --   for the programmer to distinguish, on a timeout, between
  --   /no message available/ and
  --   /no receipt arrived/.
  --   In the second case, the receipt
  --   would need to be returned.
  --   This would unnecessarily blow up the interface.
  --   If you need the reliability of receipts,
  --   you should create the queue without 'OAck'
  --   and use 'ackWith' to acknowledge 
  --   the message explicitly.
  ------------------------------------------------------------------------
  readQ :: Reader a -> IO (P.Message a)
  readQ q = do
    c <- getCon (rCon q)
    if not $ P.connected (conCon c)
      then throwIO $ QueueException $ "Not connected: " ++ show (rCon q)
      else case getSub (rSub q) c of
             Nothing -> throwIO $ QueueException $ 
                           "Unknown queue " ++ rName q
             Just ch -> do
               m <- readChan ch >>= frmToMsg q
               when (rMode q /= F.Auto) $
                 if rAuto q
                   then ack    (rCon q) m
                   else addAck (rCon q) (P.msgId m)
               return m

  ------------------------------------------------------------------------
  -- We do not support this, because of GHC ticket 4154:
  -- deadlock on isEmptyChan with concurrent read
  ------------------------------------------------------------------------
  -- isEmptyQ :: Reader a -> IO Bool

  ------------------------------------------------------------------------
  -- | Adds the value /a/ as message at the end of the queue.
  --   The Mime type as well as the headers 
  --   are added to the message.
  --
  --   If the queue was created with the option
  --   'OWithReceipt',
  --   'writeQ' will request a receipt from the broker.
  --   If the queue was additionally created with
  --   'OWaitReceipt',
  --   'writeQ' will preempt until the receipt is confirmed.
  --
  --   The Stomp headers are useful for brokers
  --   that provide selectors on /subscribe/,
  --   see 'newReader' for details.
  --
  --   A usage example for a 'Writer' /q/ of type 'String'
  --   may be (/nullType/ is defined as /text/\//plain/ in Codec.MIME):
  --
  --   > writeQ q nullType [] "hello world!"
  --
  --   For a 'Writer' that was created 
  --   with 'OWithReceipt' and 'OWaitReceipt',
  --   the function should be called with /timeout/:
  --
  --   > mbR <- timeout tmo $ writeQ q nullType [] "hello world!"
  --   > case mbR of
  --   >   Nothing -> -- error handling
  --   >   Just r  -> do -- ...
  ------------------------------------------------------------------------
  writeQ :: Writer a -> Mime.Type -> [F.Header] -> a -> IO ()
  writeQ q mime hs x =
    writeQWith q mime hs x >>= (\_ -> return ())

  ------------------------------------------------------------------------
  -- | This is a variant of 'writeQ'
  --   that overwrites the destination queue defined in the writer queue.
  --   It can be used for /ad hoc/ communication and
  --   for emulations of client/server-like protocols:
  --   the client would pass the name of the queue
  --   where it expects the server response in a header;
  --   the server would send the resply to the queue
  --   indicated in the header using 'writeAdHoc'.
  --   The additional 'String' parameter contains the destination.
  ------------------------------------------------------------------------
  writeAdHoc :: Writer a -> String -> Mime.Type -> [F.Header] -> a -> IO ()
  writeAdHoc q dest mime hs x =
    writeGeneric q dest mime hs x >>= (\_ -> return ())

  ------------------------------------------------------------------------
  -- | This is a variant of 'writeQ' 
  --   that is particularly useful for queues 
  --   created with 'OWithReceipt', but without 'OWaitReceipt'.
  --   It returns the 'Receipt', so that it can be waited for
  --   later, using 'waitReceipt'.
  --
  --   Note that the behaviour of 'writeQWith', 
  --   besides of returning the receipt, is the same as 'writeQ',
  --   /i.e./, on a queue with 'OWithReceipt' and 'OWaitReceipt'
  --   'writeQWith' will wait for the receipt being confirmed.
  --   In this case, the returned receipt is, in fact, 
  --   of no further use for the application.
  --
  --   The function is used like:
  --
  --   > r <- writeQWith q nullType [] "hello world!"
  ------------------------------------------------------------------------
  writeQWith :: Writer a -> Mime.Type -> [F.Header] -> a -> IO Receipt
  writeQWith q = writeGeneric q (wDest q) 

  ------------------------------------------------------------------------
  -- | This is a variant of 'writeAdHoc' 
  --   that is particularly useful for queues 
  --   created with 'OWithReceipt', but without 'OWaitReceipt'.
  --   It returns the 'Receipt', so that it can be waited for
  --   later, using 'waitReceipt'.
  --   Please refer to 'writeQWith' for more details.
  ------------------------------------------------------------------------
  writeAdHocWith :: Writer a -> String -> Mime.Type -> [F.Header] -> a -> IO Receipt
  writeAdHocWith = writeGeneric 
 
  ------------------------------------------------------------------------
  -- internal work horse
  ------------------------------------------------------------------------
  writeGeneric :: Writer a -> String -> 
                  Mime.Type -> [F.Header] -> a -> IO Receipt
  writeGeneric q dest mime hs x = do
    c <- getCon (wCon q)
    if not $ P.connected (conCon c)
      then throwIO $ ConnectException $
                 "Not connected (" ++ show (wCon q) ++ ")"
      else do
        tx <- getCurTx c >>= (\mbT -> 
                 case mbT of
                   Nothing     -> return NoTx
                   Just  t     -> return t)
        if tx == NoTx && wTx q
          then throwIO $ QueueException $
                 "Queue '" ++ wName q ++ 
                 "' with OForceTx used outside Transaction"
          else do
            let conv = wTo q
            s  <- conv x
            rc <- if wRec q then mkUniqueRecc else return NoRec
            let m = P.mkMessage P.NoMsg NoSub dest
                                mime (B.length s) tx s x
            when (wRec q) $ addRec (wCon q) rc 
            logSend $ wCon q
            P.send (conCon c) m (show rc) hs 
            when (wRec q && wWait q) $ waitReceipt (wCon q) rc 
            return rc

  ------------------------------------------------------------------------
  -- | Acknowledges the arrival of 'P.Message' to the broker.
  --   It is used with a 'Connection' /c/ and a 'P.Message' /x/ like:
  --
  --   > ack c x
  ------------------------------------------------------------------------
  ack :: Con -> P.Message a -> IO ()
  ack cid msg = do
    ack'  cid True False msg
    rmAck cid $ P.msgId msg

  ------------------------------------------------------------------------
  -- | Acknowledges the arrival of 'P.Message' to the broker,
  --   requests a receipt and waits until it is confirmed.
  --   Since it preempts the calling thread,
  --   it is usually used with /timeout/,
  --   for a 'Connection' /c/, a 'P.Message' /x/ 
  --   and a /timeout/ in microseconds /tmo/ like:
  --
  --   > mbR <- timeout tmo $ ackWith c x   
  --   > case mbR of
  --   >   Nothing -> -- error handling
  --   >   Just _  -> do -- ...
  ------------------------------------------------------------------------
  ackWith :: Con -> P.Message a -> IO ()
  ackWith cid msg = do
    ack'  cid True True msg  
    rmAck cid $ P.msgId msg

  ------------------------------------------------------------------------
  -- | Negatively acknowledges the arrival of 'P.Message' to the broker.
  --   For more details see 'ack'.
  ------------------------------------------------------------------------
  nack :: Con -> P.Message a -> IO ()
  nack cid msg = do
    ack' cid False False msg
    rmAck cid $ P.msgId msg

  ------------------------------------------------------------------------
  -- | Negatively acknowledges the arrival of 'P.Message' to the broker,
  --   requests a receipt and waits until it is confirmed.
  --   For more details see 'ackWith'.
  ------------------------------------------------------------------------
  nackWith :: Con -> P.Message a -> IO ()
  nackWith cid msg = do
    ack' cid False True msg
    rmAck cid $ P.msgId msg

  ------------------------------------------------------------------------
  -- Checks for Transaction,
  -- if a transaction is ongoing,
  -- the TxId is added to the message
  -- and calls P.ack on the message.
  -- If called with True for "with receipt"
  -- the function creates a receipt and waits for its confirmation. 
  ------------------------------------------------------------------------
  ack' :: Con -> Bool -> Bool -> P.Message a -> IO ()
  ack' cid ok with msg = do
    c <- getCon cid
    if not $ P.connected (conCon c) 
      then throwIO $ ConnectException $ 
             "Not connected (" ++ show cid ++ ")"
      else if null (show $ P.msgId msg)
           then throwIO $ ProtocolException "No message id in message!"
           else do
             tx <- getCurTx c >>= (\mbT -> 
                       case mbT of
                         Nothing -> return NoTx
                         Just x  -> return x)
             let msg' = msg {P.msgTx = tx}
             rc <- if with then mkUniqueRecc else return NoRec
             when with $ addRec cid rc
             logSend cid
             if ok then P.ack  (conCon c) msg' $ show rc
                   else P.nack (conCon c) msg' $ show rc
             when with $ waitReceipt cid rc 

  ------------------------------------------------------------------------
  -- | Starts a transaction and executes the action
  --   in the last parameter.
  --   After the action has finished, 
  --   the transaction will be either committed or aborted
  --   even if an exception has been raised.
  --   Note that, depending on the options,
  --   the way a transaction is terminated may vary,
  --   refer to 'Topt' for details.
  --
  --   Transactions cannot be shared among threads.
  --   Transactions are internally protected against
  --   access from any thread but the one
  --   that has actually started the transaction.
  --
  --   It is /not/ advisable to use 'withTransaction' with /timeout/.
  --   It is preferred to use /timeout/ on the 
  --   the actions executed within this transaction.
  --   Whether and how much time the transaction itself
  --   shall wait for the completion of on-going interactions with the broker,
  --   in particular pending receipts,
  --   shall be controlled
  --   by the 'OTimeout' option.
  --
  --   'withTransaction' returns the result of the action.
  --
  --   The simplest usage example with a 'Connection' /c/ is:
  --
  --   > r <- withTransaction c [] $ \_ -> do
  --
  --   If the transaction shall use receipts and, before terminating, wait 100/ms/
  --   for all receipts to be confirmed by the broker
  --   'withTransaction' is called like:
  --
  --   > eiR <- try $ withTransaction c [OTimeout 100, OWithReceipts] \_ -> do
  --   > case eiR of
  --   >   Left e  -> -- error handling
  --   >   Right x -> do -- ..
  --
  --   Note that 'try' is used to catch any 'StomplException'.
  ------------------------------------------------------------------------
  withTransaction :: Con -> [Topt] -> (Tx -> IO a) -> IO a
  withTransaction cid os op = do
    tx <- mkUniqueTxId
    let t = mkTrn tx os
    c <- getCon cid
    if not $ P.connected (conCon c)
      then throwIO $ ConnectException $
             "Not connected (" ++ show cid ++ ")"
      else finally (do addTx t cid
                       startTx cid c t 
                       x <- op tx
                       updTxState tx cid TxEnded
                       return x)
                   -- if an exception is raised in terminate
                   -- we at least will remove the transaction
                   -- from our state and then reraise 
                   (terminateTx cid tx `onException` rmThisTx tx cid)
  
  ------------------------------------------------------------------------
  -- | Waits for the 'Receipt' to be confirmed by the broker.
  --   Since the thread will preempt, the call should be protected
  --   with /timeout/, /e.g./:
  --
  --   > mb_ <- timeout tmo $ waitReceipt c r
  --   > case mb_ of
  --   >  Nothing -> -- error handling
  --   >  Just _  -> do -- ...
  ------------------------------------------------------------------------
  waitReceipt :: Con -> Receipt -> IO ()
  waitReceipt cid r =
    case r of
      NoRec -> return ()
      _     -> waitForMe 
    where waitForMe = do
            ok <- checkReceipt cid r
            unless ok $ do
              threadDelay $ ms 1
              waitForMe 

  ------------------------------------------------------------------------
  -- | Aborts the transaction immediately by raising 'AppException'.
  --   The string passed in to 'abort' will be added to the 
  --   exception message.
  ------------------------------------------------------------------------
  abort :: String -> IO ()
  abort e = throwIO $ AppException $
              "Tx aborted by application: " ++ e

  ------------------------------------------------------------------------
  -- Terminate the transaction appropriately
  -- either committing or aborting
  ------------------------------------------------------------------------
  terminateTx :: Con -> Tx -> IO ()
  terminateTx cid tx = do
    c   <- getCon cid
    mbT <- getTx tx c
    case mbT of
      Nothing -> putStrLn "Transaction terminated!" -- return ()
                 -- throwIO $ OuchException $ 
                 --  "Transaction disappeared: " ++ show tx
      Just t | txState t /= TxEnded -> endTx False cid c tx t
             | txReceipts t || txPendingAck t -> do 
                ok <- waitTx tx cid $ txTmo t
                if ok
                  then endTx True cid c tx t
                  else do
                    endTx False cid c tx t
                    let m = if txPendingAck t then "Acks" else "Receipts"
                    throwIO $ TxException $
                       "Transaction aborted: Missing " ++ m
             | otherwise -> endTx True cid c tx t

  -----------------------------------------------------------------------
  -- Send begin frame
  -- We don't wait for the receipt now
  -- we will wait for receipts 
  -- on terminating the transaction anyway
  -----------------------------------------------------------------------
  startTx :: Con -> Connection -> Transaction -> IO ()
  startTx cid c t = do
    rc <- if txAbrtRc t then mkUniqueRecc else return NoRec
    when (txAbrtRc t) $ addRec cid rc 
    logSend cid
    P.begin (conCon c) (show $ txId t) (show rc)

  -----------------------------------------------------------------------
  -- Send commit or abort frame
  -- and, if we work with receipts,
  -- wait for the receipt
  -----------------------------------------------------------------------
  endTx :: Bool -> Con -> Connection -> Tx -> Transaction -> IO ()
  endTx x cid c tx t = do
    let w = txAbrtRc t && txTmo t > 0 
    rc <- if w then mkUniqueRecc else return NoRec
    when w $ addRec cid rc 
    logSend cid
    if x then P.commit (conCon c) (show tx) (show rc)
         else P.abort  (conCon c) (show tx) (show rc)
    mbR <- if w then timeout (ms $ txTmo t) $ waitReceipt cid rc
                else return $ Just ()
    rmTx cid
    case mbR of
      Just _  -> return ()
      Nothing -> throwIO $ TxException $
                   "Transaction in unknown State: " ++
                   "missing receipt for " ++ (if x then "commit!" 
                                                   else "abort!")

  -----------------------------------------------------------------------
  -- Check if there are pending acks,
  -- if so, already wrong,
  -- otherwise wait for receipts
  -----------------------------------------------------------------------
  waitTx :: Tx -> Con -> Int -> IO Bool
  waitTx tx cid delay = do
    c   <- getCon cid
    mbT <- getTx tx c
    case mbT of
      Nothing -> return True
      Just t  | txPendingAck t -> return False
              | txReceipts   t -> 
                  if delay <= 0 then return False
                    else do
                      threadDelay $ ms 1
                      waitTx tx cid (delay - 1)
              | otherwise -> return True

  -----------------------------------------------------------------------
  -- Transform a frame into a message
  -- using the queue's application callback
  -----------------------------------------------------------------------
  frmToMsg :: Reader a -> F.Frame -> IO (P.Message a)
  frmToMsg q f = do
    let b = F.getBody f
    let conv = rFrom q
    let sid  = if  null (F.getSub f) || not (numeric $ F.getSub f)
                 then NoSub else Sub $ read $ F.getSub f
    x <- conv (F.getMime f) (F.getLength f) (F.getHeaders f) b
    let m = P.mkMessage (P.MsgId $ F.getId f) sid
                        (F.getDest   f) 
                        (F.getMime   f)
                        (F.getLength f)
                        NoTx 
                        b -- raw bytestring
                        x -- converted context
    return m {P.msgHdrs = F.getHeaders f}

  -----------------------------------------------------------------------
  -- Connection listener
  -----------------------------------------------------------------------
  listen :: Con -> IO ()
  listen cid = forever $ do
    c   <- getCon cid
    let cc = conCon c
    eiF <- catches (S.receive (P.getRc cc) (P.getSock cc) (P.conMax cc))
                   [Handler (\e -> case e of
                                     ThreadKilled -> throwIO e
                                     _ -> return $ Left $ show e),
                    Handler (\e -> return $ Left $ 
                                        show (e::SomeException))]
    case eiF of
      Left e  -> throwToOwner c $ 
                   ProtocolException $ "Receive Error: " ++ e
      Right f -> do
        logReceive cid
        case F.typeOf f of
          F.Message   -> handleMessage cid f
          F.Error     -> handleError   cid f
          F.Receipt   -> handleReceipt cid f
          F.HeartBeat -> handleBeat    cid f
          _           -> throwToOwner c $ 
                           ProtocolException $ "Unexpected Frame: " ++
                           show (F.typeOf f)

  -----------------------------------------------------------------------
  -- Handle Message Frame
  -----------------------------------------------------------------------
  handleMessage :: Con -> F.Frame -> IO ()
  handleMessage cid f = do
    c <- getCon cid
    case getCh c of
      Nothing -> throwToOwner c $ 
                 ProtocolException $ "Unknown Queue: " ++ show f
      Just ch -> writeChan ch f
    where getCh c = let dst = F.getDest f
                        sid = F.getSub  f
                    in if null sid
                      then getDest dst c
                      else if numeric sid
                             then getSub (Sub $ read sid) c
                             else Nothing 

  -----------------------------------------------------------------------
  -- Handle Error Frame
  -----------------------------------------------------------------------
  handleError :: Con -> F.Frame -> IO ()
  handleError cid f = do
    c <- getCon cid
    let r = if null (F.getReceipt f) then ""
              else " (" ++ F.getReceipt f ++ ")" 
    let e = F.getMsg f ++ r ++ ": " ++ U.toString (F.getBody f)
    throwToOwner c (BrokerException e) 

  -----------------------------------------------------------------------
  -- Throw to owner
  -- important: give the owner some time to react
  --            before continuing and - probably - 
  --            causing another exception
  -----------------------------------------------------------------------
  throwToOwner :: Connection -> StomplException -> IO ()
  throwToOwner c e = do
    throwTo (conOwner c) e
    threadDelay 10000

  -----------------------------------------------------------------------
  -- Handle Receipt Frame
  -----------------------------------------------------------------------
  handleReceipt :: Con -> F.Frame -> IO ()
  handleReceipt cid f = do
    c <- getCon cid
    case parseRec $ F.getReceipt f of
      Just r  -> forceRmRec cid r 
      Nothing -> throwToOwner c $ 
                   ProtocolException $ "Invalid Receipt: " ++ show f

  -----------------------------------------------------------------------
  -- Handle Beat Frame, i.e. ignore them
  -----------------------------------------------------------------------
  handleBeat :: Con -> F.Frame -> IO ()
  handleBeat _ _ = return () -- putStrLn "Beat!"

  -----------------------------------------------------------------------
  -- My Beat 
  -----------------------------------------------------------------------
  heartBeat :: Con -> Int -> IO ()
  heartBeat cid period = forever $ do
    now <- getCurrentTime
    c   <- getCon cid
    let me = myMust  c 
    let he = hisMust c 
    when (now > he) $ throwToOwner c $ 
                         ProtocolException $ 
                           "Missing HeartBeat, last was " ++
                           show (now `diffUTCTime` he)    ++ 
                           " seconds ago!"
    when (now >= me) $ P.sendBeat $ conCon c
    threadDelay $ ms period

  -----------------------------------------------------------------------
  -- When we should have sent last heartbeat
  -----------------------------------------------------------------------
  myMust :: Connection -> UTCTime
  myMust c = let t = conMyBeat c
                 p = snd $ P.conBeat $ conCon c
             in  timeAdd t p

  -----------------------------------------------------------------------
  -- When he should have sent last heartbeat
  -----------------------------------------------------------------------
  hisMust :: Connection -> UTCTime
  hisMust c = let t   = conHisBeat c
                  tol = 4
                  b   = fst $ P.conBeat $ conCon c
                  p   = tol * b
              in  timeAdd t p

  -----------------------------------------------------------------------
  -- Adding a period to a point in time
  -----------------------------------------------------------------------
  timeAdd :: UTCTime -> Int -> UTCTime
  timeAdd t p = ms2nominal p `addUTCTime` t

  -----------------------------------------------------------------------
  -- Convert milliseconds to seconds
  -----------------------------------------------------------------------
  ms2nominal :: Int -> NominalDiffTime
  ms2nominal m = fromIntegral m / (1000::NominalDiffTime)

