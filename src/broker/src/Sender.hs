module Sender (startSender)
where

  import           Types
  import           Config
  import qualified Socket as S
  import           State  
  import           Queue 
  import           Fifo hiding  (empty)
  import qualified Fifo as FIFO (empty) 
  import           Exception

  import qualified Network.Mom.Stompl.Frame  as F
  import qualified Network.Socket as Sock

  import           Control.Concurrent
  import           Control.Exception
  import           Prelude hiding (catch)
  import           Control.Monad
  import           Control.Applicative ((<$>))

  import           Data.List  (insert, delete, find)
  import           Data.Maybe (catMaybes)
  import qualified Data.ByteString           as B

  startSender :: IO ThreadId
  startSender = forkIO (forever $ runSender)

  runSender :: IO ()
  runSender = 
    catch (waitRequest >>= \r -> handleRequest r)
          (\e -> reportSilent EMERGENCY $
                              "Sender down: " ++ show (e::SomeException))

  handleRequest :: SndRequest -> IO ()
  handleRequest r = 
    case r of
      FrameReq cid f c -> handleFrame cid f c
      SendAllReq       -> undefined

  handleFrame :: ConId -> F.Frame -> Con -> IO ()
  handleFrame cid f c = 
    case F.typeOf f of
      F.Subscribe   -> handleSubscribe   cid f c
      F.Unsubscribe -> handleUnsubscribe cid f 
      F.Send        -> handleSend        cid f 
      F.Ack         -> undefined
      F.Nack        -> undefined
      _             -> reportSilent ERROR $ "Unknown internal request: " ++
                                    F.toString f

  handleSubscribe :: ConId -> F.Frame -> Con -> IO ()
  handleSubscribe cid f c 
    | null (F.getDest f) = reportSilent NOTICE $ 
                             "No destination in SUBSCRIBE frame " ++
                             "from connection " ++ show cid ++ 
                             ": " ++ F.toString f
    | otherwise          = 
        let sid = mkSubId cid (F.getId f)
            q   = QName (F.getDest f) 
        in frameToSub sid f c >>= \s -> addSub q s

  handleUnsubscribe :: ConId -> F.Frame -> IO ()
  handleUnsubscribe cid f 
    | null (F.getDest f) &&
      null (F.getId   f)  = reportSilent NOTICE $
                              "No destination in SEND frame " ++
                              "from connection " ++ show cid ++  
                              ": " ++ F.toString f
    | otherwise =
        let sid = mkSubId cid (if null (F.getId f) 
                                 then F.getDest f 
                                 else F.getId   f)
         in rmSub sid

  handleSend :: ConId -> F.Frame -> IO ()
  handleSend cid f 
    | null (F.getDest f) = reportSilent NOTICE $
                             "No destination in SEND frame " ++
                             "from connection " ++ show cid ++  
                             ": " ++ F.toString f
    | otherwise          = withQAdd (sendQ f) $ QName (F.getDest f)

  frameToSub :: SubId -> F.Frame -> Con -> IO Sub
  frameToSub sid f c = do
    fifo <- newFifo
    return (newSub sid f fifo c)
