{-# Language BangPatterns #-}
module Network.Mom.Stompl.Patterns.Balancer (
                                      withBalancer,
                                      withRouter)
where

  import           Types
  import           Registry
  import           Network.Mom.Stompl.Client.Queue 
  import           Network.Mom.Stompl.Patterns.Basic
  import           Codec.MIME.Type (nullType)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, catches)
  import           Control.Monad (forever, unless)

  -----------------------------------------------------------------------
  -- | Balancer
  -----------------------------------------------------------------------
  withBalancer :: Con -> String -> QName -> (Int, Int) -> OnError -> 
                  QName -> IO r -> IO r
  withBalancer c n qn (mn,mx) onErr rq action =
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
              (ignoreHandler Error n onErr)
          send2Prov w m p = writeAdHoc w (prvQ p) nullType 
                                         (msgHdrs m) $ msgContent m

  -----------------------------------------------------------------------
  -- | Router
  -----------------------------------------------------------------------
  withRouter :: Con -> String  -> JobName -> 
                QName -> QName -> QName -> Int  -> OnError -> 
                IO r -> IO r
  withRouter c n jn srq ssq trq tmo onErr action = 
     withPub c n jn trq onErr 
                     ("unknown", [], [], bytesOut) $ \p ->
       withSubThread c n jn srq tmo 
                     (ssq,       [], [], bytesIn) (pub p) onErr action
    where pub p m = publish p nullType (msgHdrs m) $ msgContent m


