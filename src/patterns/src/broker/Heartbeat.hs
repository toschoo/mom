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

  import Data.Time.Clock

  tolerance :: Int
  tolerance = 5

  -- Milliseconds
  type Msec = Int

  ------------------------------------------------------------------------
  -- Heartbeat descriptor
  ------------------------------------------------------------------------
  data Heartbeat = Heart {
                     hbNextHB     :: UTCTime,
                     hbPeriod     :: Msec,
                     hbBeat       :: Bool
                   }

  ------------------------------------------------------------------------
  -- HbState:
  --   OK  : Heartbeat sent and response received
  --   Send: Heartbeat to be sent and nothing yet received
  --   Dead: Heartbeat sent and nothing received after tolerance
  ------------------------------------------------------------------------
  data HbState = HbOK | HbSend | HbDead
    deriving (Eq, Show)

  ------------------------------------------------------------------------
  -- Create a new heartbeat descriptor
  ------------------------------------------------------------------------
  newHeartbeat :: Msec -> IO Heartbeat
  newHeartbeat period = do
    now <- getCurrentTime
    return Heart {
               hbNextHB = timeAdd now (tolerance * period),
               hbPeriod = period,
               hbBeat   = False}

  ------------------------------------------------------------------------
  -- Update a heartbeat descriptor
  ------------------------------------------------------------------------
  updAction :: UTCTime -> Heartbeat -> Heartbeat
  updAction now hb = hb {hbNextHB     = moveNext now hb,
                         hbBeat       = False}

  ------------------------------------------------------------------------
  -- Check heartbeat descriptor:
  --    if next heartbeat still in the future: ok
  --       otherwise if state is "beat sent" : dead
  --                 otherwise: send heartbeat now
  --                                 "beat sent" (we expect the caller
  --                                              to send a hb *now*)
  --                                 move next heartbeat
  ------------------------------------------------------------------------
  checkHB :: UTCTime -> Heartbeat -> (HbState, Heartbeat)
  checkHB now hb | now <= hbNextHB hb = (HbOK, hb)
                 | hbBeat hb          = (HbDead, hb)
                 | otherwise          = (HbSend, hb {
                                           hbNextHB = moveNext now hb,
                                           hbBeat   = True})

  ------------------------------------------------------------------------
  -- Test heartbeat according to the same logic as check,
  -- but without updating it
  ------------------------------------------------------------------------
  testHB :: UTCTime -> Heartbeat -> HbState
  testHB now hb | now <= hbNextHB hb = HbOK
                | hbBeat hb          = HbDead
                | otherwise          = HbSend

  ------------------------------------------------------------------------
  -- Add tolerance * period
  ------------------------------------------------------------------------
  moveNext :: UTCTime -> Heartbeat -> UTCTime
  moveNext t hb = timeAdd t (tolerance * hbPeriod hb)

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
    
