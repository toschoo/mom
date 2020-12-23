module Network.Mom.Stompl.Client.Stream
where

  import qualified Data.Conduit as C
  import           Data.Conduit ((.|))

  import           Data.Conduit.Network (AppData)  
  import           Data.Conduit.Network  as N

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U

  import           Network.Mom.Stompl.Parser (stompParser)
  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Client.Exception

  import           Control.Monad (forever)
  import           Control.Monad.Trans (liftIO)
  import           Control.Monad.IO.Class (MonadIO)
  import           Control.Concurrent

  import qualified Data.Attoparsec.ByteString as A 

  ------------------------------------------------------------------------
  -- Error Handler
  ------------------------------------------------------------------------
  type EH = StomplException -> IO ()

  ------------------------------------------------------------------------
  -- A TCP/IP fragment read by the Conduit Client has 4096 bytes.
  -- We allow 1000 fragments = 1024 * 4096 Bytes = 4MB
  ------------------------------------------------------------------------
  maxStep :: Int
  maxStep = 1024

  ------------------------------------------------------------------------
  -- Sender thread: get a Frame from a pipe, convert it into a ByteString
  --                and send it through a socket 
  ------------------------------------------------------------------------
  sender :: AppData -> Chan F.Frame -> IO ()
  sender ad ip =  C.runConduitRes (pipeSource ip .| stream .| N.appSink ad)

  ------------------------------------------------------------------------
  -- Receiver thread: get a ByteStream through a socket,
  --                  parse it to a Frame and send it through a pipe
  ------------------------------------------------------------------------
  receiver :: AppData -> Chan F.Frame -> EH -> IO ()
  receiver ad ip eh = C.runConduitRes (appSource ad .| parseC eh .| pipeSink ip) 

  ------------------------------------------------------------------------
  -- Put a frame into a pipe (a channel)
  ------------------------------------------------------------------------
  pipeSink :: MonadIO m => Chan F.Frame -> C.ConduitT F.Frame C.Void m ()
  pipeSink ch = C.awaitForever (liftIO . writeChan ch)

  ------------------------------------------------------------------------
  -- Read a frame from a pipe (a channel)
  ------------------------------------------------------------------------
  pipeSource :: MonadIO m => Chan F.Frame -> C.ConduitT () F.Frame m ()
  pipeSource ch = forever (liftIO (readChan ch) >>= C.yield)

  ------------------------------------------------------------------------
  -- Convert a frame to a ByteString
  ------------------------------------------------------------------------
  stream :: MonadIO m => C.ConduitT F.Frame B.ByteString m () 
  stream = C.awaitForever (C.yield . F.putFrame)

  ------------------------------------------------------------------------
  -- Parse a Frame from a ByteString
  ------------------------------------------------------------------------
  parseC :: MonadIO m => EH -> C.ConduitT B.ByteString F.Frame m ()
  parseC eh = goOn
    where goOn = go (A.parse stompParser) 0 -- start with a clean parser
          go prs step = do
            mbNew <- C.await
            case mbNew of 
              Nothing -> return () -- socket was closed
              Just s  -> case parseAll prs s of
                           -- parse error: call the error handler ---------
                           Left e -> liftIO (eh $ ProtocolException e)
                                     >> goOn
                           -- we got a result -----------------------------
                           Right (prs', fs) -> do
                             -- Do we have (at least) 1 frame to send? ----
                             step' <- if null fs then return (step+1) 
                                                 else mapM_ C.yield fs >>
                                                      return 0
                             -- Too many fragments ------------------------
                             if step' > maxStep 
                               then liftIO (eh $ ProtocolException 
                                                 "Message too long!") 

                             -- Continue with the current parser ----------
                               else go prs' step'

  ------------------------------------------------------------------------
  -- A parser is something that converts a ByteString into a Frame
  ------------------------------------------------------------------------
  type Parser = B.ByteString -> A.Result F.Frame

  ------------------------------------------------------------------------
  -- Continue parsing until we have a complete frame
  ------------------------------------------------------------------------
  parseAll :: Parser -> B.ByteString -> 
              Either String (Parser, [F.Frame])
  parseAll prs s = case prs s of
                     -- We failed ----------------------------------------
                     A.Fail _ _   e  -> Left $ U.toString s ++ ": " ++ e

                     -- We have a partial result and continue -------------
                     --    feeding this partial result --------------------
                     r@(A.Partial _) -> Right (A.feed r, [])

                     -- We are done ---------------------------------------
                     A.Done s' f     -> 
                       if B.null s' 
                         then Right (A.parse stompParser, [f])
                         -- but there may be a leftover -------------------
                         else case parseAll (A.parse stompParser) s' of
                                Left e           -> Left e
                                Right (prs', fs) -> Right (prs',f:fs)

