module Main
where

  import           System.Environment
  import           Data.Char
  import           Text.Parsec 
  import           Text.Parsec.ByteString as BP
  import qualified Data.ByteString.Char8  as B
  import qualified Data.ByteString        as S

  data Msg = ConMsg {
               conLogin :: String,
               conPass  :: String}
           | SndMsg {
               sndDest  :: String,
               sndTrans :: String,
               sndRec   :: String,
               sndLen   :: Int,
               sndBody  :: B.ByteString}
           | DisMsg {
               disRec   :: String}
    deriving (Show, Eq)

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      []  -> error "I need a file name"
      [f] -> do 
        c <- parseFromFile parseStomp f -- readFile "con.txt"
        case c of
          Left  e  -> error $ "Error: " ++ (show e)
          Right m -> putStrLn $ show m 
      _ -> error "I don't know what to do with all the arguments!"   


  -- parseMsg :: B.ByteString -> Msg
  -- parseMsg s = case parse parseStomp "Stomp" s of
  --               Left e  -> error $ "Error: " ++ (show e)
  --               Right m -> m

  type Header = (String, String)

  hdrLog  = "login"
  hdrPass = "passcode"
  hdrDest = "destination"
  hdrLen  = "content-length"
  hdrTrn  = "transaction"
  hdrRec  = "receipt"

  parseStomp :: Parser Msg
  parseStomp = do
    t <- msgType
    case t of
      "CONNECT"    -> connect
      "DISCONNECT" -> disconnect
      "SEND"       -> send
      _            -> fail $ "Unknown message type: '" ++ t ++ "'"

  connect :: Parser Msg
  connect = do
    hs <- headers
    ignoreBody 
    case getConnect hs of
      Nothing    -> fail $ "Incomplete connect headers"
      Just (l,p) -> return $ ConMsg l p

  disconnect :: Parser Msg
  disconnect = do
    r <- option (hdrRec, "") $ header 
    ignoreBody 
    if fst r /= hdrRec 
      then fail $ "Unknown Header in Disconnect Message: " ++ (fst r)
      else return $ DisMsg (snd r)

  send :: Parser Msg
  send = do
    hs <- headers
    case getLen hs of
      Left  e -> fail e
      Right l -> do
        b  <- body l
        case makeSndMsg hs l b of
          Left  e -> fail e
          Right m -> return m

  getConnect :: [Header] -> Maybe (String, String)
  getConnect hs = do
    l <- lookup hdrLog  hs
    p <- lookup hdrPass hs
    return (l,p)

  getLen :: [Header] -> Either String Int
  getLen hs = 
    case lookup hdrLen hs of
      Nothing -> Right (-1)
      Just l  -> if numeric l then Right $ read l 
                   else Left $ "content-length is not numeric: " ++ l

  numeric :: String -> Bool
  numeric = and . map isDigit

  makeSndMsg :: [Header] -> Int -> B.ByteString -> Either String Msg
  makeSndMsg hs l b = 
    case lookup hdrDest hs of
      Nothing -> Left "No destination header in SEND Message"
      Just d  -> Right $ SndMsg {
                           sndDest = d,
                           sndLen  = l,
                           sndTrans = case lookup hdrTrn hs of
                                        Nothing -> ""
                                        Just t  ->  t,
                           sndRec   = case lookup hdrRec hs of
                                        Nothing -> ""
                                        Just r  -> r,
                           sndBody  = b
                         }

  headers :: Parser [Header]
  headers = do
    hs <- header `endBy` char '\n'
    return hs 

  header :: Parser Header
  header = do
    k <- many1 (noneOf " :\n\x00")
    keyValSep
    v <- many1 (noneOf " \n\x00")
    skipWhite
    return (k,v)

  keyValSep :: Parser ()
  keyValSep = do
    skipWhite
    char ':'
    skipWhite

  terminal :: Parser ()
  terminal = do
    _ <- char '\n'
    return ()

  skipLine :: Parser ()
  skipLine = do
    skipWhite
    terminal

  msgType :: Parser String
  msgType = do
    t <- many1 (noneOf " \n\x00")
    skipWhite
    terminal
    return t

  body :: Int -> Parser B.ByteString
  body l = do
    skipWhite
    skipLine
    inp <- getInput
    let (b, r) = B.breakEnd (== '\x00') inp
    if B.null b 
      then
        fail "Missing NUL in Body"
      else do
        let (b',r') = B.breakEnd (/= '\x00') b
        if l < 0 || l == B.length b' then return b'
          else fail "Actual content does not equal content-length"

  ignoreBody :: Parser ()
  ignoreBody = skipMany (noneOf "\x00")

  skipWhite :: Parser ()
  skipWhite = do
    optional $ skipMany1 (char ' ')
