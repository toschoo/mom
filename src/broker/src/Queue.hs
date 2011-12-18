module Queue (
         Queue, QName(..), Pattern(..),
         newQueue, qByName, qLength, qName, subsInQ,
         addSubToQ, rmSubFromQ, updSubInQ, 
         sendQ, sendQAll, sendQOne,
         Sub, SubId(..), mkSubId, newSub, subId,
         Con, newCon)
where

  import           Types
  import           Factory
  import           Fifo hiding (empty)
  import qualified Fifo (empty)
  import qualified Socket as S -- common!
  import           Exception
  import qualified Network.Mom.Stompl.Frame as F

  import qualified Data.ByteString.Char8 as B
  import           Data.Maybe (catMaybes, fromJust)
  import           Data.List  (delete)

  import           Control.Applicative
  import           Control.Monad
  import           Control.Exception
  import           Prelude hiding (catch)
  import           Control.Concurrent.ParallelIO.Local

  data Queue = NewQueue {
                 qName :: QName,
                 qPat  :: Pattern,
                 qSubs :: [Sub],
                 qMsgs :: Fifo F.Frame
                 -- ...
               }

  instance Eq Queue where
    x == y = qName x == qName y

  newQueue :: QName -> Pattern -> Maybe Sub -> Fifo F.Frame -> Queue
  newQueue n p mbS f = NewQueue {
                         qName = n,
                         qPat  = p,
                         qSubs = catMaybes [mbS],
                         qMsgs = f
                       }

  qByName :: QName -> Queue -> Bool
  qByName n q = n == qName q

  subsInQ :: Queue -> [Sub]
  subsInQ = qSubs

  addSubToQ :: Sub -> Queue -> Queue
  addSubToQ s q = 
    if s `elem` qSubs q then q else q {qSubs = s : qSubs q}

  rmSubFromQ :: SubId -> Queue -> Queue
  rmSubFromQ s q = q {qSubs = delId s subId $ qSubs q}

  updSubInQ :: Sub -> Queue -> Queue
  updSubInQ s q = addSubToQ s (rmSubFromQ (subId s) q) 

  qLength :: Queue -> IO Int
  qLength q = do
    s <- sum <$> mapM subLength (qSubs q)
    m <- count $ qMsgs q
    return (s + m) 

  mapOneOrMany :: Pattern -> (Sub -> IO (Maybe Sub)) -> [Sub] -> IO [Maybe Sub]
  mapOneOrMany Many f ss = mapPar f ss 
  mapOneOrMany One  _ [] = return []
  mapOneOrMany One  f (s:ss) = f s >>= \mbS ->
    case mbS of
      Nothing -> mapOneOrMany One f ss
      Just s' -> return (Just s' : map Just ss)

  mapPar :: (Sub -> IO (Maybe Sub)) -> [Sub] -> IO [Maybe Sub]
  mapPar f ss   = withPool 2 act -- from config
    where act p = parallel p (map f ss)
    
  -- ingenius or oversimplified?
  sendQ :: F.Frame -> Queue -> IO Queue -- save q f
  sendQ f q = Fifo.empty (qMsgs q) >>= \w -> 
    case w of
      False -> do report (mkQLog $ qName q) DEBUG $ "Adding message to queue"
                  push  (qMsgs q) f >> return q
                  -- add to database
      True  -> do
        sbs <- catMaybes <$> mapOneOrMany (qPat q) (sendSub f) 
                                                   (qSubs   q) 
        if null sbs 
          then push (qMsgs q) f >> return q {qSubs = []}
               -- add to database
          else return q {qSubs = sbs}

  sendQAll :: Queue -> IO Queue
  sendQAll = sendQX True

  sendQOne :: Queue -> IO Queue
  sendQOne = sendQX False

  sendQX :: Bool -> Queue -> IO Queue
  sendQX sndall q 
    | null (qSubs q) = return q
    | otherwise      = do
        sbs  <- sendXInQ sndall q (qSubs q)
        sbs' <- catMaybes <$> mapOneOrMany (qPat q) sendSubAll sbs
        return q {qSubs = sbs'}

  sendXInQ :: Bool -> Queue -> [Sub] -> IO [Sub]
  sendXInQ sndall q sbs = do
        mbF <- pop (qMsgs q)
        case mbF of
          Nothing -> return sbs
          Just f  -> do
            sbs'  <- catMaybes <$> mapM (sendSub f) (qSubs q) 
            if null sbs'
              then push (qMsgs q) f >> return []
              else do
                sbs'' <- if sndall 
                           then sendXInQ True q sbs'
                           else return sbs'
                return sbs''
                           
  data Sub = NewSub {
               subId   :: SubId,
               subMode :: F.AckMode,
               subCon  :: Con,
               subMsgs :: Fifo F.Frame
             }

  instance Eq Sub where
    x == y = subId x == subId y

  newSub :: SubId -> F.Frame -> Fifo F.Frame -> Con -> Sub
  newSub sid f fifo c = NewSub {
                          subId   = sid,
                          subMode = F.getAcknow f,
                          subCon  = c,
                          subMsgs = fifo
                        }

  subLength :: Sub -> IO Int
  subLength = count . subMsgs

  sendSub :: F.Frame -> Sub -> IO (Maybe Sub)
  sendSub f s = do 
    msg <- newMsgId
    let (_, sid)  = parseSubId (subId s)
    case F.sndToMsg (show msg) (show sid) f of
      Nothing -> return $ Just s -- protocol error
      Just f' -> do
        w <- Fifo.empty (subMsgs s)
        if w 
          then send f' s
          else do report (mkSubLog (F.getDest f) $ subId s) DEBUG $
                         "Adding message " ++ show (F.getId f') ++
                         " to queue"
                  push (subMsgs s) f' >> (return $ Just s)
                  -- add to database

  sendSubOne :: Sub -> IO (Maybe Sub)
  sendSubOne s = do
    mbF <- pop (subMsgs s)
    case mbF of
      Nothing -> return $ Just s
      Just f  -> send f s

  sendSubAll :: Sub -> IO (Maybe Sub)
  sendSubAll s = do
    mbS <- sendSubOne s
    case mbS of 
      Nothing -> return Nothing
      Just s' -> do
        s'' <- sendSubAll s
        return s''

  send :: F.Frame -> Sub -> IO (Maybe Sub)
  send f s = catch (do report (mkSubLog  (F.getDest f) $ subId s) DEBUG $
                              "Sending message " ++ show (F.getId f)
                       S.send (conWr   $ subCon s)
                              (conSock $ subCon s) f
                       -- remove from database 
                       return $ Just s)
                   (\e -> do report (mkSubLog (F.getDest f) $ subId s) NOTICE $
                                    "Cannot send to Socket: " ++
                                    show (e::SomeException) 
                             return Nothing)

  mkQLog :: QName -> String
  mkQLog q = "Queue " ++ show q

  mkSubLog :: String -> SubId -> String
  mkSubLog q s = "Sub " ++ q ++ "." ++ show s

