-------------------------------------------------------------------------------
-- |
-- Module     : Heartbeat.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: non-portable
-- 
-- Heartbeat
-------------------------------------------------------------------------------
module Heartbeat
where

  import Network.Mom.Patterns.Types (Msec)

  import Data.Time.Clock
  import Control.Concurrent.MVar

  -------------------------------------------------------------------------
  -- Tolerance for foreign heartbeat -
  -- Period until disconnect = tolerance * heartbeat period
  -------------------------------------------------------------------------
  tolerance :: Int
  tolerance = 10

  -------------------------------------------------------------------------
  -- Helper 
  -------------------------------------------------------------------------
  hbPeriodReached :: MVar UTCTime -> Msec -> IO Bool
  hbPeriodReached m tmo = modifyMVar m $ \t -> do
                            now <- getCurrentTime
                            if t `timeAdd` tmo <= now
                              then return (now `timeAdd` tmo, True )
                              else return (                t, False)

  ------------------------------------------------------------------------
  -- Heartbeat descriptor
  ------------------------------------------------------------------------
  data Heartbeat = Heart {
                     hbNextMe     :: UTCTime,
                     hbNextHe     :: UTCTime,
                     hbPeriod     :: Msec
                   }

  ------------------------------------------------------------------------
  -- Create a new heartbeat descriptor
  ------------------------------------------------------------------------
  newHeartbeat :: Msec -> IO Heartbeat
  newHeartbeat period = do
    now <- getCurrentTime
    return Heart {
               hbNextHe = timeAdd now (tolerance * period),
               hbNextMe = timeAdd now period,
               hbPeriod = period}

  ------------------------------------------------------------------------
  -- Update heartbeat descriptor
  -- - my next heartbeat
  -- - his next heartbeat
  ------------------------------------------------------------------------
  updMe :: UTCTime -> Heartbeat -> Heartbeat
  updMe now hb = hb {hbNextMe = now `timeAdd` (hbPeriod hb)}

  updHim :: UTCTime -> Heartbeat -> Heartbeat
  updHim now hb = hb {hbNextHe = timeAdd now $ tolerance * (hbPeriod hb)}

  ------------------------------------------------------------------------
  -- Check me, him
  ------------------------------------------------------------------------
  checkMe :: UTCTime -> Heartbeat -> Bool
  checkMe now hb | now <= hbNextMe hb = False
                 | otherwise          = True

  alive :: UTCTime -> Heartbeat -> Bool
  alive now hb | now <= hbNextHe hb = True
               | otherwise          = False

  -----------------------------------------------------------------------
  -- Adding period to time
  -----------------------------------------------------------------------
  timeAdd :: UTCTime -> Msec -> UTCTime
  timeAdd t p = ms2nominal p `addUTCTime` t

  -----------------------------------------------------------------------
  -- Convert milliseconds to seconds
  -----------------------------------------------------------------------
  ms2nominal :: Msec -> NominalDiffTime
  ms2nominal m = fromIntegral m / (1000::NominalDiffTime)
    
