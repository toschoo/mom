module Types
where

  import qualified Socket as S
  import qualified Network.Mom.Stompl.Frame as F
  import           Control.Concurrent

  data SndRequest = FrameReq ConId F.Frame Con
                  | SendAllReq 
                  -- CfgReq
                  -- ...

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

