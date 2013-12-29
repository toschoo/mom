{-# Language BangPatterns #-}
module Network.Mom.Stompl.Patterns.Bridge
where

  import           Types
  import           Network.Mom.Stompl.Patterns.Basic 
  import           Network.Mom.Stompl.Client.Queue 
  import           Codec.MIME.Type (nullType)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO) 

  -----------------------------------------------------------------------
  -- | Forwarder
  -----------------------------------------------------------------------
  withForwarder :: Con   -> Con -> String  -> JobName -> 
                   QName -> QName -> QName -> Int  -> OnError -> 
                   IO r -> IO r
  withForwarder src trg n jn srq ssq trq tmo onErr action = 
     withPub trg n jn trq onErr 
                     ("unknown", [], [], bytesOut) $ \p ->
       withSubThread src n jn srq tmo 
                     (ssq,       [], [], bytesIn) (pub p) onErr action
    where pub p m = publish p nullType (msgHdrs m) $ msgContent m

  -----------------------------------------------------------------------
  -- | TaskBridge
  -----------------------------------------------------------------------
  withTaskBridge :: Con -> Con -> String -> JobName ->
                    QName -> QName ->
                    RegistryDesc -> OnError -> IO r -> IO r
  withTaskBridge src trg n jn srq twq reg onErr action =
    withPusher trg n jn (twq, [], [], bytesOut) $ \p ->
      withTaskThread src n jn (fwd p) 
                        (srq, [], [], bytesIn) reg onErr action
    where fwd p m = let hs = filter ((/= "__job__") . fst) $ msgHdrs m
                     in push p nullType hs $ msgContent m

  -----------------------------------------------------------------------
  -- | ServiceBridge
  -----------------------------------------------------------------------
  withServiceBridge :: Con -> Con -> String -> 
                       JobName -> 
                       QName   -> QName -> QName ->
                       RegistryDesc -> OnError -> IO r -> IO r
  withServiceBridge src trg n jn srq trq twq
                    reg@(_, tmo, _) onErr action =
    withClient trg n jn (trq, [], [], bytesIn)
                        (twq, [], [], bytesOut) $ \c ->
      withServerThread src n jn nullType [] (fwd c)
                       (srq,         [], [], bytesIn) 
                       ("unknown",   [], [], bytesOut)
                       reg onErr action
    where fwd c m  = 
            let hs = filter (clFilter . fst) (msgHdrs m)
             in do mbR <- request c tmo nullType hs $ msgContent m
                   case mbR of
                     Nothing -> throwIO $ TimeoutX "on requesting target"
                     Just r  -> return $ msgContent r
          clFilter x = x /= "__channel__" &&
                       x /= "__job__"
