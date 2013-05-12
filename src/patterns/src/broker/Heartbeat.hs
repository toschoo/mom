module Heartbeat
where

  import Data.Time.Clock

  tolerance :: Int
  tolerance = 5

  type Msec = Int

  data Heartbeat = Heart {
                     hbNextHB     :: UTCTime,
                     hbPeriod     :: Msec,
                     hbBeat       :: Bool
                   }

  data HbState = HbOK | HbSend | HbDead
    deriving (Eq, Show)

  newHeartbeat :: Msec -> IO Heartbeat
  newHeartbeat period = do
    now <- getCurrentTime
    return $ Heart {
               hbNextHB = timeAdd now (tolerance * period),
               hbPeriod = period,
               hbBeat   = False}

  updAction :: UTCTime -> Heartbeat -> Heartbeat
  updAction now hb = hb {hbNextHB     = moveNext now hb,
                         hbBeat       = False}

  checkHB :: UTCTime -> Heartbeat -> (HbState, Heartbeat)
  checkHB now hb | now <= hbNextHB hb = (HbOK, hb)
                 | hbBeat hb          = (HbDead, hb)
                 | otherwise          = (HbSend, hb {
                                           hbNextHB = moveNext now hb,
                                           hbBeat   = True})

  testHB :: UTCTime -> Heartbeat -> HbState
  testHB now hb | now <= hbNextHB hb = HbOK
                | hbBeat hb          = HbDead
                | otherwise          = HbSend

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
    
