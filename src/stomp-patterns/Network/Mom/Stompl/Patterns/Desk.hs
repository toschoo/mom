{-# Language BangPatterns #-}
module Network.Mom.Stompl.Patterns.Desk (
                            withDesk, requestProvider)
where

  import           Types
  import           Registry
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
  -- | Desk
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
              (ignoreHandler Error n onErr)

  -----------------------------------------------------------------------
  -- | Desk
  -----------------------------------------------------------------------
  requestProvider :: ClientA () ()  -> 
                     Int -> JobName -> Int -> IO (StatusCode, [QName])
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
