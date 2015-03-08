{-# Language CPP #-}
module Stream
where

  import qualified Data.Conduit as C
  import           Data.Conduit (($$),(=$))
  import           Data.Conduit.Network
  import           Data.Conduit.Network.TLS

  import qualified Data.ByteString.Char8 as B
  import qualified Data.ByteString.UTF8  as U

  import           Network.Mom.Stompl.Parser (stompParser)
  import qualified Network.Mom.Stompl.Frame as F
  import           Network.Mom.Stompl.Client.Exception

  import           Control.Concurrent.MVar
  import           Control.Applicative ((<$>))
  import           Control.Monad (unless, when)
  import           Control.Exception (finally, SomeException)
  import qualified Control.Exception as Ex (try)
  import           Control.Monad (forever)
  import           Control.Monad.Trans (liftIO)
  import           Control.Concurrent

  import qualified Data.Attoparsec.ByteString as A 

  type EH = StomplException -> IO ()

  maxStep :: Int
  maxStep = 1000

  sender :: AppData -> Chan F.Frame -> IO ()
  sender ad ip =  pipeSource ip $$ stream =$ appSink ad

  receiver :: AppData -> Chan F.Frame -> EH -> IO ()
  receiver ad ip eh = appSource ad $$ parseC eh =$ pipeSink ip 

  pipeSink :: Chan F.Frame -> C.Sink F.Frame IO ()
  pipeSink ch = C.awaitForever (liftIO . writeChan ch)

  pipeSource :: Chan F.Frame -> C.Source IO F.Frame
  pipeSource ch = forever (liftIO (readChan ch) >>= C.yield)

  stream :: C.ConduitM F.Frame B.ByteString IO ()
  stream = C.awaitForever (C.yield . F.putFrame)

  parseC :: EH -> C.ConduitM B.ByteString F.Frame IO ()
  parseC eh = goOn
    where goOn = go (A.parse stompParser) 0
          go prs step = do
            mbNew <- C.await
            case mbNew of 
              Nothing -> return ()
              Just s  -> case parseAll prs s of
                           Left e -> liftIO (eh $ ProtocolException e)
                                     >> goOn
                           Right (prs', fs) -> do
                             step' <- if null fs then return (step+1) 
                                                 else mapM_ C.yield fs >>
                                                      return 0
                             if step' > maxStep 
                               then liftIO (eh $ ProtocolException 
                                                 "Message too long!") 
                                    >> goOn
                               else go prs' step'

  type Parser = B.ByteString -> A.Result F.Frame

  parseAll :: Parser -> B.ByteString -> 
              Either String (Parser, [F.Frame])
  parseAll prs s = case prs s of
                     A.Fail _ _   e  -> Left $ U.toString s ++ ": " ++ e
                     r@(A.Partial _) -> Right (A.feed r, [])
                     A.Done s' f     -> 
                       if B.null s' 
                         then Right (A.parse stompParser, [f])
                         else case parseAll (A.parse stompParser) s' of
                                Left e           -> Left e
                                Right (prs', fs) -> Right (prs',f:fs)

