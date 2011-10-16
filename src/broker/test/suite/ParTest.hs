module FrameTest
where

  type Test     = (TestDesc, FilePath)
  type Msg      = String
  data TestDesc = TDesc {
                    dscDesc  :: String,
                    dscType  :: FrameType,
                    dscRes   :: TestResult,
                    dscHdrs  :: [(String, String)]}
    deriving (Eq, Show, Read)

  data TestResult = Fail | Pass
    deriving (Eq, Show, Read)

  select :: FrameType -> [Test] -> [Test]
  select t = filter (hasType t)

  hasType :: FrameType -> Test -> Bool
  hasType t c = (t == (dscType . fst) c) 

  frmOk :: FrameType -> TestDesc -> Bool
  frmOk f d = f == dscType d

  headerOk :: String -> Frame -> TestDesc -> Bool
  headerOk k f d = 
    if acc f == value then True else False
    where acc   = getAccess k
          value = case lookup k $ dscHdrs d of
                    Nothing -> ""
                    Just v  -> v

  getValue :: String -> Frame -> String
  getValue s f = acc f
    where acc = getAccess s

  getAccess :: String -> (Frame -> String)
  getAccess k =  
    case k of
      "login"          -> getLogin
      "passcode"       -> getPasscode
      "destination"    -> getDest
      "content-length" -> show . getLength
      "content-type"   -> getMime
      "transaction"    -> getTrans
      "id"             -> getId
      "message-id"     -> getId
      "message"        -> getMsg
      "receipt"        -> getReceipt
      "heart-beat"     -> beatToVal . getBeat
      "accept-version" -> versToVal . getVersions
      "version"        -> verToVal  . getVersion
      "host"           -> getHost
      _                -> (\_ -> "unknown")

