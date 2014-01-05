{-# Language BangPatterns #-}
-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Stompl/Patterns/Desk.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- A Desk is a server that supplies information
-- about job providers. A client requests providers
-- for a specific job (service, task or topic)
-- and the desk will reply with a list of queue names
-- of providers of the enquired job.
-- 
-- The desk is not statically configured,
-- but uses a registry to which providers connect.
-- Providers that cease to work can disconnect or,
-- if heartbeats are required, will be removed from the
-- list of available providers internally when no more heartbeats
-- are sent. This way, the information provided by a desk is 
-- always up-to-date.
--
-- Desk balances providers, /i.e./ providers rotate in a list
-- from which always the first /n/ providers are handed out
-- to requesting consumers (where /n/ corresponds to the number of 
-- providers requested by the consumer.) 
--
-- Since providers are managed dynamically, 
-- the result of two consecutive calls is probably not the same.
-- Desk is thus not idempotent in the strict sense.
-- But, since the call itself does only cause 
-- a change of the order of providers
-- (and since it should be irrelevant for the consumer
--  which provider is actually used),
-- two consecutive calls will have the same effect
-- -- if not all providers disconnect between the two calls.
--
-- Internally, the Desk protocol uses the following headers:
--
-- * __jobs__: Comma-separated list of providers;
--
-- * __redundancy__: Requested number of providers.
-------------------------------------------------------------------------------
module Network.Mom.Stompl.Patterns.Desk (
                            withDesk, requestProvider)
where

  import           Registry
  import           Types
  import           Network.Mom.Stompl.Patterns.Basic 
  import           Network.Mom.Stompl.Client.Queue 
  import           Data.Char (isDigit)
  import           Data.List (intercalate)
  import           Data.List.Split (endBy)
  import           Codec.MIME.Type (nullType)
  import           Prelude hiding (catch)
  import           Control.Exception (throwIO, catches)
  import           Control.Monad (forever)
  import           Control.Applicative ((<$>))

  -----------------------------------------------------------------------
  -- | Creates a desk with the lifetime of the application-defined
  --   action:
  --
  --   * 'Con': Connection to a Stomp broker;
  --
  --   * String: Name of the desk, used for error handling;
  --
  --   * 'QName': Registration queue -- this queue is used
  --              by providers to connect to the registry,
  --              it is not used for consumer requests;
  --
  --   * (Int, Int): Heartbeat range of the 'Registry' 
  --                 (see 'withRegistry' for details);
  --
  --   * 'OnError': Error handling;
  --
  --   * 'QName': Request queue -- this queue is used
  --              by consumers to request information about
  --              available providers;
  --
  --   * IO r: Action that defines the lifetime of the desk;
  --           the result is also the result of /withDesk/.
  -----------------------------------------------------------------------
  withDesk :: Con -> String -> QName -> (Int, Int) -> OnError -> 
              QName -> IO r -> IO r
  withDesk c n qn (mn,mx) onErr rq action =
    withRegistry c n qn (mn,mx) onErr $ \reg -> 
      withThread (doDesk reg) action
    where doDesk reg = 
            withPair   c n (rq,        [], [], ignorebody)
                           ("unknown", [], [],     nobody) $ \(r,w) -> 
              forever $ catches (do
                m  <- readQ r
                j  <- getJobName m
                q  <- getChannel m
                i  <- getRedundancy m
                ps <- (intercalate "," . map prvQ) <$> getProvider reg j i
                case length ps of
                  0 -> let hs = [("__sc__", show NotFound),
                                 ("__jobs__",          ""),
                                 ("__redundancy__",   "0")]
                        in writeAdHoc w q nullType hs ()
                  x -> let hs = [("__sc__",        show OK),
                                 ("__redundancy__", show x),
                                 ("__jobs__",           ps)]
                        in writeAdHoc w q nullType hs ())
              (ignoreHandler n onErr)

  -----------------------------------------------------------------------
  -- | Function used by consumer to request provider information 
  --   from a desk:
  --
  --   * 'ClientA' () (): The request to the desk is sent
  --                      through a client of type () ().
  --                      This client must be created by 
  --                      the application beforehand
  --                      (/e.g./: the client could be created
  --                               once during initialisation
  --                               and then be used repeatedly
  --                               to obtain or update information 
  --                               on providers according to the
  --                               application needs);
  --
  --   * Int: Timeout in milliseconds;
  --
  --   * 'JobName': Name of the job for which the consumer
  --                needs providers;
  --
  --   * Int: Number of providers needed by the consumer.
  --          This can be used for redundancy:
  --          if one provider fails, 
  --          the consumer passes to the next.
  --          Be aware, however, that the information, 
  --          at the point in time, when a provider fails, 
  --          may already be outdated.
  --          Therefore, the redundant providers should be used
  --          immediately and, when the main provider fails later,
  --          the information should be updated by requesting
  --          new providers from the desk.
  --
  --  The result is a tuple of ('StatusCode', ['QName']).
  --  If the 'StatusCode' is not 'OK', 
  --  the list of 'QName' will be empty;
  --  otherwise, it will contain at least one provider
  --  and maximum /n/ providers (where /n/ is the number of providers
  --  requested). If fewer providers than requested are available,
  --  the list will contain less than /n/ providers. 
  --  But note that this, as long as there is at least one provider,
  --  does not count as an error, /i.e./ the 'StatusCode' is still 'OK'.
  -----------------------------------------------------------------------
  requestProvider :: ClientA () ()  ->  Int -> 
                     JobName        ->  Int -> IO (StatusCode, [QName])
  requestProvider c tmo jn r = do
    mbR <- request c tmo nullType [("__job__",            jn),  
                                   ("__redundancy__", show r)] ()
    case mbR of
      Nothing -> return (Timeout, [])
      Just m  -> do
        eiSC <- getSC   m
        case eiSC of
          Left  sc -> throwIO $ BadStatusCodeX sc
          Right OK -> do qs <- getJobs m
                         return (OK, qs)
          Right sc -> return (sc, [])
    
  getJobs :: Message i -> IO [QName]
  getJobs m = do
    x <- getHeader "__jobs__"
                   "no jobs in header" m
    return $ endBy "," x

  getRedundancy :: Message i -> IO Int
  getRedundancy m = do
    x <- getHeader "__redundancy__" 
                   "No redundancy level in headers" m
    if all isDigit x then return $ read x 
                     else throwIO $ HeaderX "__redundancy" $
                                      "Redundancy level not numberic: " ++ x
