{-# Language BangPatterns #-}
-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Stompl/Patterns/Balancer.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- This module provides a balancer for services and tasks
-- and a topic router.
-- Balancers for services and tasks improve scalability and reliability
-- of servers and workers. Workers should always be used with a balancer
-- (since balancing workload is the main idea of workers);
-- servers can very well be used without a balancer, but won't scale
-- with increasing numbers of clients.
--
-- A balancer consists of a registry to which 
-- servers and workers may connect;
-- servers and workers are maintained in lists 
-- according to the job they provide.
-- Client and pushers send requests to the balancer,
-- which then forwards the request to the a server or worker.
-- The client will receive the reply not through the balancer,
-- but directly from the server (to which the reply queue
-- was forwarded as part of the request message -- 
-- see 'ClientA' for details).
-- 
-- If servers and workers send heartbeats to the registry,
-- it is highly probable that the request will be forwarded
-- to a working provider. This is much more reliable
-- than pushing a job to only one worker or 
-- requesting only one server.
-- 
-- A router is a forwarder of a topic.
-- A router is very similar to a publisher ('PubA')
-- with the difference that the router
-- does not create new topic data, 
-- but uses topic data received from a publisher
-- (a router, hence, is a subscriber and a publisher).
-- Routers can be used to balance the workload of publishers:
-- Instead of one publisher serving thousands of subscribers,
-- the initial publisher would serve thousands of routers,
-- which, in their turn, serve thousands of subscribers 
-- (or even other routers).
-------------------------------------------------------------------------------
module Network.Mom.Stompl.Patterns.Balancer (
                                      -- * Balancer
                                      withBalancer,
                                      -- * Router 
                                      withRouter)
where

  import           Registry
  import           Types
  import           Network.Mom.Stompl.Client.Queue 
  import           Network.Mom.Stompl.Patterns.Basic
  import           Codec.MIME.Type (nullType)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, catches)
  import           Control.Monad (forever, unless)

  -----------------------------------------------------------------------
  -- | Create a Service and Task Balancer with the lifetime
  --   of the application-defined action passed in
  --   and start it in a background thread:
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * String: Name of the balancer, used for error handling;
  --
  --   * 'QName': Registration queue -- this queue is used
  --              by providers to connect to the registry,
  --              it is not used for consumer requests;
  --
  --   * (Int, Int): Heartbeat range of the 'Registry' 
  --                 (see 'withRegistry' for details);
  --  
  --   * 'QName': Request queue -- this queue is used
  --              for consumer requests;
  --
  --   * 'OnError': Error handling;
  --
  --   * IO r: Action that defines the lifetime of the balancer;
  --           the result /r/ is also the result of /withBalancer/.
  -----------------------------------------------------------------------
  withBalancer :: Con     -> String  -> QName -> (Int, Int) -> 
                  QName   -> OnError -> IO r  -> IO r
  withBalancer c n qn (mn,mx) rq onErr action =
    withRegistry c n qn (mn,mx) onErr $ \reg -> 
      withThread (balance reg) action
    where balance reg = 
            withPair c n (rq,        [], [], bytesIn)
                         ("unknown", [], [], bytesOut) $ \(r,w) -> 
              forever $ catches (do
                m  <- readQ r
                jn <- getJobName m
                t  <- mapR reg jn (send2Prov w m)
                unless t $ throwIO $ NoProviderX jn)
              (ignoreHandler n onErr)
          send2Prov w m p = writeAdHoc w (prvQ p) nullType 
                                         (msgHdrs m) $ msgContent m

  -----------------------------------------------------------------------
  -- | Create a router with the lifetime of the 
  --   application-defined action passed in
  --   and start it in a background thread:
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * String: Name of the router, used for error handling;
  --
  --   * 'JobName': Routed topic;
  --
  --   * 'QName': Registration queue of the source publisher;
  --
  --   * 'QName': Queue through which the internal subscriber
  --              will receive the topic data from the source publisher;
  --
  --   * 'QName': Registration queue of the target publisher
  --              to which subscribers will connect;
  --
  --   * Int: Registration timeout 
  --          (timeout to register at the source publisher);
  --  
  --   * 'QName': Request queue -- this queue is used
  --              for consumer requests;
  --
  --   * 'OnError': Error handling;
  --
  --   * IO r: Action that defines the lifetime of the router;
  --           the result /r/ is also the result of /withRouter/.
  -----------------------------------------------------------------------
  withRouter :: Con   -> String  -> JobName -> 
                QName -> QName   -> QName   -> 
                Int   -> OnError -> IO r    -> IO r
  withRouter c n jn srq ssq trq tmo onErr action = 
     withPub c n jn trq onErr 
                     ("unknown", [], [], bytesOut) $ \p ->
       withSubThread c n jn srq tmo 
                     (ssq,       [], [], bytesIn) (pub p) onErr action
    where pub p m = publish p nullType (msgHdrs m) $ msgContent m
