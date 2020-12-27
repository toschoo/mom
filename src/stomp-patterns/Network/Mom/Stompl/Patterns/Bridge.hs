{-# Language BangPatterns #-}
-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Stompl/Patterns/Bridge.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- Bridges link providers connected to one broker
-- to consumers connected to another broker.
--
-- For publishers and workers, this is quite trivial:
-- the bridge implements 
--            the corresponding consumer on one broker
--            and the corresponding provider on the other.
--
-- For servers, the task is somewhat more complicated:
-- since servers use the client's reply queue to send the result
-- back to the client and this queue only exists on the broker
-- to which the client is connected, the bridge has to remember 
-- the client's reply queue and use its own queue on the server-side broker
-- to finally route the reply back to the original client.
-- With many broker connected by a service bridge,
-- this can result in long chains of clients and servers 
-- sending requests and waiting for replies.
-------------------------------------------------------------------------------
module Network.Mom.Stompl.Patterns.Bridge (
                             -- * Forwarder
                             withForwarder, 
                             -- * TaskBridge
                             withTaskBridge,
                             -- * ServiceBridge
                             withServiceBridge)
where

  import           Types
  import           Network.Mom.Stompl.Patterns.Basic 
  import           Network.Mom.Stompl.Client.Queue 
  import           Codec.MIME.Type (nullType)
  import           Control.Exception (throwIO) 

  -----------------------------------------------------------------------
  -- | Create a forwarder with the lifetime of the application-defined
  --   action passed in and start it in a background thread:
  --
  --   * 'Con': Connection to the source broker
  --            (the one where the original publisher is connected);
  --
  --   * 'Con': Connection to the target broker
  --            (the one where the target subscribers are connected);
  --
  --   * String: Name of the forwarder used for error handling;
  --
  --   * 'JobName': Name of the Topic that is bridged;
  --
  --   * 'QName': Registration queue of the source publisher;
  --
  --   * 'QName': Queue through which the internal subscriber
  --              will receive topic data from the source publisher;
  --
  --   * 'QName': Registration queue of the target publisher;
  --
  --   * Int: Timeout on registering to the source publisher
  --          in microseconds;
  --
  --   * 'OnError': Error handler;
  --
  --   * IO r: Action that defines the lifetime of the forwarder;
  --           its result /r/ is also the result of /withForwarder/.
  --
  --   Note the remarkable similarity to the router pattern ('withRouter').
  --   In fact, a router is but a forwarder where source and target broker
  --   are the same.
  -------------------------------------------------------------------------
  withForwarder :: Con     -> Con   -> String  -> JobName -> 
                   QName   -> QName -> QName   -> Int     -> 
                   OnError -> IO r  -> IO r
  withForwarder src trg n jn srq ssq trq tmo onErr action = 
     withPub trg n jn trq onErr 
                     ("unknown", [], [], bytesOut) $ \p ->
       withSubThread src n jn srq tmo 
                     (ssq,       [], [], bytesIn) (pub p) onErr action
    where pub p m = publish p nullType (msgHdrs m) $ msgContent m

  -----------------------------------------------------------------------
  -- | Create a TaskBridge with the lifetime of the action passed in
  --   and start it on a background thread:
  --
  --   * 'Con': Connection to the source broker
  --            (the one to which the pusher is connected);
  --
  --   * 'Con': Connection to the target broker
  --            (the one to which the worker is connected);
  --
  --   * String: Name of the bridge used for error handling;
  --
  --   * 'JobName': Name of the Task that is bridged;
  --
  --   * 'QName': Queue of the worker on the source side;
  --              (if the worker is connected to a balancer
  --               on the source side, this is an internal queue
  --               only visible in the bridge and in the balancer);
  --
  --   * 'QName': Queue of the worker on the target side
  --              (which may be a balancer's request queue);
  --
  --   * 'RegistryDesc': 'Registry' (/i.e./ balancer)
  --                     to which the bridge is connected
  --                     on the source side;
  --
  --   * 'OnError': Error handler;
  --
  --   * IO r: Action that defines the lifetime of the bridge;
  --           its result /r/ is also the result of /withTaskBridge/.
  -----------------------------------------------------------------------
  withTaskBridge :: Con   -> Con   -> String  -> JobName ->
                    QName -> QName ->
                    RegistryDesc   -> OnError -> IO r    -> IO r
  withTaskBridge src trg n jn srq twq reg onErr action =
    withPusher trg n jn (twq, [], [], bytesOut) $ \p ->
      withTaskThread src n jn (fwd p) 
                        (srq, [], [], bytesIn) reg onErr action
    where fwd p m = let hs = filter ((/= "__job__") . fst) $ msgHdrs m
                     in push p nullType hs $ msgContent m

  -----------------------------------------------------------------------
  -- | Create a ServiceBridge with the lifetime of the action passed in
  --   and start it on a background thread:
  --
  --   * 'Con': Connection to the source broker
  --            (the one to which the client is connected);
  --
  --   * 'Con': Connection to the target broker
  --            (the one to which the server is connected);
  --
  --   * String: Name of the bridge used for error handling;
  --
  --   * 'JobName': Name of the Service that is bridged;
  --
  --   * 'QName': Queue of the server on the source side;
  --              (if the server is connected to a balancer
  --               on the source side, this is an internal queue
  --               only visible in the bridge and in the balancer);
  --
  --   * 'QName': Reader queue of the internal client on the target side;
  --
  --   * 'QName': Queue of the server on the target side
  --              (which may be a balancer's request queue);
  --
  --   * 'RegistryDesc': 'Registry' (/i.e./ balancer)
  --                     to which the bridge is connected
  --                     on the source side;
  --
  --   * 'OnError': Error handler;
  --
  --   * IO r: Action that defines the lifetime of the bridge;
  --           its result /r/ is also the result of /withServiceBridge/.
  -----------------------------------------------------------------------
  withServiceBridge :: Con   -> Con   -> String  ->  JobName -> 
                       QName -> QName -> QName   ->
                       RegistryDesc   -> OnError -> IO r     -> IO r
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
          clFilter = not . (`elem` ["__channel__", "__job__"])
