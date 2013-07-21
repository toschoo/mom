{-# LANGUAGE RankNTypes, Rank2Types #-}
module Network.Mom.Stompl.Client.Basic
where

  import           Network.Mom.Stompl.Client.Queue hiding (Client)
  import           Network.Mom.Stompl.Client.Streams
  import           Network.Mom.Stompl.Frame (Header)
  import           Codec.MIME.Type (Type, nullType)
  import qualified Data.Conduit as C
  import           Data.Conduit (($=), ($$), (=$), (=$=))
  import           Data.Maybe (isNothing)
  import qualified Data.ByteString.Char8 as B
  import           Control.Monad.Trans (liftIO)
  import           Control.Monad       (when)
  import           Control.Exception   (throwIO)
  import           Control.Concurrent                       
  import           Control.Applicative ((<$>))
  import           System.Timeout

  data Client i o = Client {clInQ  :: Reader i,
                            clOutQ :: Writer o}

  withClient :: Con -> String -> String -> String -> 
                InBound i -> OutBound o -> (Client i o -> IO r) -> IO r
  withClient c cName iDest oDest iconv oconv act = do
    inQ  <- newReader c (cName ++ "_in" ) iDest [] [] iconv 
    outQ <- newWriter c (cName ++ "_out") oDest [] [] oconv
    act $ Client inQ outQ

  request :: Client i o -> Int -> Control a -> Producer a o -> Consumer a (Message i) r -> IO r
  request c tmo ctr src snk = do
    C.runResourceT (src $$ sinkQ (clOutQ c))
    C.runResourceT (sourceQ (clInQ c) ctr $$ snk) 

  -- server:
  -- - we need a load balancer
  -- - we need a protocol
  --      to indicate the last message (header - last?)
  --      to indicate the return queue
    
