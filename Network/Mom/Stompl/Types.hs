module Types 
where

  import Network.Mom.Stompl.Frame
  import System.Log.Logger (Priority)
  import Network.Socket
  import Data.List (foldl')

  data LogMsg = LogMsg {
                  logName :: String,
                  logPrio :: Priority,
                  logMsg  :: String
                  }
                | CfgLogMsg {
                  logName :: String,
                  logFile :: String,
                  logPrio :: Priority}

  data SubMsg = FrameMsg {
                  msgCid   :: Int,
                  msgFrame :: Frame
                  }
                | SockMsg {
                  msgCid   :: Int,
                  msgSock  :: Socket,
                  msgState :: SockState}
                | RegMsg {
                    msgCid  :: Int,
                    msgSock :: Socket
                  }
                | UnRegMsg {
                    msgCid :: Int}
                | CfgSndMsg {
                    msgName :: String}

  data SockState = Up | Down 
    deriving (Eq, Show)

  -- some general routines --
  dropNothing :: [(Maybe a, b)] -> [(a, b)]
  dropNothing = foldl' (\x y -> case fst y of
                                  Nothing -> x
                                  Just c  -> (c, snd y) : x) []

