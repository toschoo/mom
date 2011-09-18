module Network.Mom.Stompl.Parser (
                        startParsing,
                        continueParsing,
                        stompParser,
                        stompAtOnce
                      )
where

  import           Data.Attoparsec hiding (take, takeWhile, takeTill)
  import qualified Data.Attoparsec as A   (takeWhile, takeTill)
  import qualified Data.ByteString as B
  import qualified Data.ByteString.UTF8 as U
  import           Data.Word 

  import           Control.Applicative ((<|>))
  import           Network.Mom.Stompl.Frame

  startParsing :: B.ByteString -> Either String (Result Frame)
  startParsing m = case parse stompParser m  of
                        Fail _ _ e -> Left e
                        r          -> Right r

  continueParsing :: Result Frame -> B.ByteString -> Either String (Result Frame)
  continueParsing r m = case feed r m of
                          Fail _ _ e -> Left e
                          r'         -> Right r'

  stompAtOnce :: B.ByteString -> Either String Frame
  stompAtOnce s = parseOnly stompParser s

  stompParser :: Parser Frame
  stompParser = do
    t <- msgType
    case t of
      "CONNECT"     -> connect
      "DISCONNECT"  -> disconnect
      "SEND"        -> send
      "SUBSCRIBE"   -> subscribe
      "UNSUBSCRIBE" -> usubscribe
      "BEGIN"       -> begin
      "COMMIT"      -> commit 
      "ABORT"       -> abort
      "ACK"         -> ack
      "MESSAGE"     -> message
      "RECEIPT"     -> receipt
      "ERROR"       -> prsError
      _             -> fail $ "Unknown message type: '" ++ t ++ "'"

  msgType :: Parser String
  msgType = do
    t <- A.takeTill (endAny)
    skipWhite
    terminal
    return $ U.toString t

  send :: Parser Frame
  send = bodyFrame mkSndFrame

  message :: Parser Frame
  message = bodyFrame mkMsgFrame

  prsError :: Parser Frame
  prsError = bodyFrame mkErrFrame

  bodyFrame :: ([Header] -> Int -> Body -> Either String Frame) -> Parser Frame
  bodyFrame mk = do
    hs <- headers
    case getLen hs of
      Left  e -> fail e
      Right l -> do
        b  <- body l
        case mk hs l b of
          Left  e -> fail e
          Right m -> return m

  connect :: Parser Frame
  connect = genericFrame mkConFrame

  disconnect :: Parser Frame
  disconnect = genericFrame mkDisFrame

  subscribe :: Parser Frame
  subscribe = genericFrame mkSubFrame 

  usubscribe :: Parser Frame
  usubscribe = genericFrame mkUSubFrame

  begin :: Parser Frame
  begin = genericFrame mkBgnFrame

  commit :: Parser Frame
  commit = genericFrame mkCmtFrame

  abort :: Parser Frame
  abort = genericFrame mkAbrtFrame

  ack :: Parser Frame
  ack = genericFrame mkAckFrame
  
  receipt :: Parser Frame
  receipt = genericFrame mkRecFrame

  genericFrame :: ([Header] -> Either String Frame) -> Parser Frame
  genericFrame mk = do
    hs <- headers
    ignoreBody
    case mk hs of
      Left e  -> fail e
      Right m -> return m

  headers :: Parser [Header]
  headers = headers' []

  headers' :: [Header] -> Parser [Header]
  headers' hs = do
    skipWhite
    endHeaders hs <|> getHeader hs 

  endHeaders :: [Header] -> Parser [Header]
  endHeaders hs = do
    terminal
    return hs

  getHeader :: [Header] -> Parser [Header]
  getHeader hs = do
    h <- header
    headers' (h:hs)

  header :: Parser Header
  header = do
    k <- A.takeTill endAny
    keyValSep 
    v <- A.takeTill endLine
    terminal
    return (U.toString k, U.toString v)

  keyValSep :: Parser ()
  keyValSep = do
    skipWhite
    _ <- takeWhile1 (== col)
    skipWhite

  terminal :: Parser ()
  terminal = do
    skipWhite
    _ <- word8 eol 
    skipWhite

  body :: Int -> Parser B.ByteString
  body x = body' x B.empty
    where 
      body' l i = do
        n <- A.takeTill (== nul)
        let b = i >|< n 
        if l < 0 || l == B.length b
          then do
            _ <- word8 nul
            return b
          else 
            if l < B.length b 
              then failBodyLen 
              else do
                _ <- word8 nul
                body' l (b |> '\x00') 

  ignoreBody :: Parser ()
  ignoreBody = do 
    _ <- A.takeTill (== nul)
    _ <- word8 nul
    return ()

  skipWhite :: Parser ()
  skipWhite = do
    _ <- A.takeWhile (== spc)
    return ()

  endAny :: Word8 -> Bool
  endAny w = (w == col || w == eol || w == spc || w == nul)

  endLine :: Word8 -> Bool
  endLine = (== eol)

  nul, eol, spc, col :: Word8
  nul  = 0
  eol  = 10
  spc  = 32
  col  = 58
  
  failBodyLen :: Parser a
  failBodyLen = fail "Body longer than indicated by content-length" 
  
