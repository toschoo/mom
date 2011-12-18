module Types (SndRequest(..), LogRequest(..),
              Con(..), newCon,
              ConId(..), QName(..), Pattern(..), MsgId(..), SndId(..),
              SubId(..), mkSubId, parseSubId, delId,
              Log(..), LogType(..), isLogType,
              LogFormat(..), isLogFormat,
              L.Priority(..), parsePrio, isPrio)
where

  import qualified Socket as S
  import qualified Network.Mom.Stompl.Frame as F
  import           Control.Concurrent
  import           System.Log.Logger as L (Priority(..)) 
  import           System.IO (Handle)
  import           Data.Char (toLower)

  parsePrio :: String -> Priority
  parsePrio s = case (map toLower) s of
                  "debug"     -> DEBUG
                  "info"      -> INFO
                  "notice"    -> NOTICE 
                  "warning"   -> WARNING
                  "error"     -> ERROR
                  "critical"  -> CRITICAL
                  "alert"     -> ALERT
                  "emergency" -> EMERGENCY
                  _           -> error $ "Not a Priority: " ++ s

  isPrio :: String -> Bool
  isPrio s = if (map toLower) s `elem` ps then True else False
    where ps = ["debug", "info",     "notice", "warning",
                "error", "critical", "alert",  "emergency"] 

  data Log  = StdLog  Handle
            | FileLog FilePath
            | SrvLog  String Int

  data LogType = StdErr | StdOut | File | Server
    deriving (Show, Eq)

  instance Read LogType where
    readsPrec _ s = case (map toLower) s of
                      "stderr" -> [(StdErr, "")]
                      "stdout" -> [(StdOut, "")]
                      "file"   -> [(File, "")]
                      "server" -> [(Server, "")]
                      _        -> error $ "Not a LogType: '" ++ s ++ "'"

  isLogType :: String -> Bool
  isLogType s = if (map toLower) s `elem` ts then True else False
    where ts = ["stderr", "stdout", "file", "server"] 

  data LogFormat = Native | Log4j
    deriving (Show, Eq)

  instance Read LogFormat where
    readsPrec _ s = case (map toLower) s of
                      "native" -> [(Native, "")]
                      "log4j"  -> [(Log4j , "")]
                      _        -> error $ "Not a LogFormat: '" ++ s ++ "'"

  isLogFormat :: String -> Bool
  isLogFormat s = if (map toLower) s `elem` fs then True else False
    where fs = ["native", "log4j"]

  data SndRequest = FrameReq ConId F.Frame Con
                  | SendAllReq 
                  -- CfgReq
                  -- ...

  data LogRequest = LogReq {
                      logName :: String,
                      logPrio :: Priority,
                      logMsg  :: String
                    }
                   | LogCfgReq 

  data Pattern = One | Many
    deriving (Eq, Ord, Show)

  instance Read Pattern where
    readsPrec _ s = 
      case (map toLower) s of
        "one"  -> [(One, "")]
        "many" -> [(Many, "")]
        _      -> error $ "Cannot parse Pattern '" ++ s ++ "'"
  

  data Con = NewCon {
               conSock :: S.Socket,
               conWr   :: S.Writer
             }

  newCon :: S.Socket -> S.Writer -> Con
  newCon s w = NewCon s w

  newtype ConId = ConId String

  instance Show ConId where
    show (ConId s) = s

  newtype QName = QName String
    deriving (Eq, Ord)

  instance Show QName where
    show (QName s) = s

  newtype MsgId = MsgId String
    deriving (Eq, Ord)

  instance Show MsgId where
    show (MsgId s) = s

  newtype SndId = SndId String
    deriving (Eq, Ord)

  instance Show SndId where
    show (SndId s) = s

  newtype SubId = SubId String
    deriving (Eq, Ord)

  instance Show SubId where
    show (SubId s) = s

  mkSubId :: ConId -> String -> SubId 
  mkSubId cid s = SubId (show cid ++ ":" ++ s)

  parseSubId :: SubId -> (ConId, SubId)
  parseSubId s = (ConId $ getCid s, SubId $ getSid s)
    where getCid =          takeWhile (/= ':') . show
          getSid = drop 1 . dropWhile (/= ':') . show

  delId :: Eq b => b -> (a -> b) -> [a] -> [a]
  delId _ _ [] = []
  delId i f (s:ss) | i == f s   =     delId i f ss
                   | otherwise  = s : delId i f ss

