-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Stompl/Client/Conduit.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- This module provides Conduit interfaces
-- for the stomp-queue library.
-- The interfaces create or receive
-- streams of messages instead of single messages.
-- This approach aims to simplify the integration
-- of messaging into applications by means of well-defined
-- streaming interfaces. 
-------------------------------------------------------------------------------
module Network.Mom.Stompl.Client.Conduit (
                                  -- * Plain message streams
                                  qSource, qSink, 
                                  -- * Multipart messages as streams
                                  qMultiSource, qMultiSink)
where

  import qualified Data.Conduit as C
  import           Codec.MIME.Type (Type)
  import           Control.Monad.Trans (liftIO)
  import           System.Timeout

  import           Network.Mom.Stompl.Client.Queue
  import           Network.Mom.Stompl.Frame

  ------------------------------------------------------------------------
  -- | Reads from a 'Reader' queue with timeout 
  --   and returns a 'C.Producer' of type 'Message' i.
  --
  --   The function ends iff the timeout expires 
  --   and loops infinitely otherwise.
  --
  --   Parameters:
  --
  --   * 'Reader' i: The input interface, a Stomp 'Reader'. 
  --
  --   * Int: Timeout in microseconds
  ------------------------------------------------------------------------
  qSource :: C.MonadResource m => 
             Reader i -> Int -> C.Producer m (Message i)
  qSource r tmo = go
    where go = liftIO (timeout tmo $ readQ r) >>= mbYield
          mbYield mbX = case mbX of
                          Nothing -> return ()
                          Just x  -> C.yield x >> go

  ------------------------------------------------------------------------
  -- | Writes a stream of messages to a 'Writer' queue
  --   and returns a 'C.Consumer' of type o.
  --   The function terminates, when the stream ends.
  --
  --   Parameters:
  --
  --   * 'Writer' o: The output interface, a Stomp 'Writer'.
  --
  --   * 'Type': The mime type of the message content.
  --
  --   * ['Header']: Headers to add to each message.
  ------------------------------------------------------------------------
  qSink :: C.MonadResource m => 
           Writer o -> Type -> [Header] -> C.Consumer o m ()
  qSink w t hs = C.awaitForever $ \x -> liftIO (writeQ w t hs x)

  -- header to mark the last segment of a multipart message -------------
  lastMHdr :: Header
  lastMHdr = ("__last__", "true")

  ------------------------------------------------------------------------
  -- | Reads from a 'Reader' queue with timeout 
  --   and returns a 'C.Producer' of type 'Message' i.
  --   The function ends when the timeout expires 
  --   or  after having received a segment that is marked as the last one.
  --   Note that multipart messages are not foreseen by the standard.
  --   'qMultiSink' and 'qMultiSource' use a header named \"__last__\"
  --   to label the last segment of a multipart message.
  --
  --   For parameters, please refer to 'qSource'. 
  ------------------------------------------------------------------------
  qMultiSource :: C.MonadResource m =>
                  Reader i -> Int -> C.Producer m (Message i)
  qMultiSource r tmo = loop
    where loop = do
            mbX <- liftIO (timeout tmo $ readQ r)
            case mbX of
              Nothing -> return ()
              Just x  -> case lookup (fst lastMHdr) $ msgHdrs x of
                           Just "true" -> C.yield x
                           _           -> C.yield x >> loop
  

  ------------------------------------------------------------------------
  -- | Writes a multipart message to a 'Writer' queue
  --   and returns a 'C.Consumer' of type o.
  --   The function terminates, when the stream ends.
  --   The last segment is sent with the header (\"__last__\", \"true\").
  --   Note that multipart messages are not foreseen by the standard.
  --
  --   For parameters, please refer to 'qSink'. 
  ------------------------------------------------------------------------
  qMultiSink :: C.MonadResource m =>
                Writer o -> Type -> [Header] -> C.Consumer o m ()
  qMultiSink w t hs = do
    mbX <- C.await
    case mbX of
      Nothing -> return ()
      Just x  -> go x
    where hs'  = lastMHdr:hs
          go x = do
            mbY <- C.await
            case mbY of
              Nothing -> liftIO (writeQ w t hs' x) 
              Just y  -> liftIO (writeQ w t hs  x) >> go y
