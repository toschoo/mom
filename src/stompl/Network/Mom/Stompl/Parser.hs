-------------------------------------------------------------------------------
-- |
-- Module     : Network/Mom/Stompl/Frame.hs
-- Copyright  : (c) Tobias Schoofs
-- License    : LGPL 
-- Stability  : experimental
-- Portability: portable
--
-- Stomp Parser based on Attoparsec
-------------------------------------------------------------------------------
module Network.Mom.Stompl.Parser (
                        stompParser,
                        stompAtOnce
                      )
where

  import           Data.Attoparsec hiding (take, takeWhile, takeTill)
  import qualified Data.Attoparsec as A   (takeWhile, takeTill)
  import qualified Data.ByteString as B
  import qualified Data.ByteString.UTF8 as U
  import           Data.Word 

  import           Control.Applicative ((<|>), (<$>))
  import           Control.Monad (void)
  import           Network.Mom.Stompl.Frame

  ------------------------------------------------------------------------
  -- | Parses a ByteString at once with Attoparsec 'parseOnly'.
  --   May fail or conclude.
  ------------------------------------------------------------------------
  stompAtOnce :: B.ByteString -> Either String Frame
  stompAtOnce = parseOnly stompParser 

  ------------------------------------------------------------------------
  -- | The Stomp Parser
  ------------------------------------------------------------------------
  stompParser :: Parser Frame
  stompParser = do
    t <- msgType
    case t of
      ""            -> beat
      "CONNECT"     -> connect
      "STOMP"       -> stomp
      "CONNECTED"   -> connected
      "DISCONNECT"  -> disconnect
      "SEND"        -> send
      "SUBSCRIBE"   -> subscribe
      "UNSUBSCRIBE" -> usubscribe
      "BEGIN"       -> begin
      "COMMIT"      -> commit 
      "ABORT"       -> abort
      "ACK"         -> ack
      "NACK"        -> nack
      "MESSAGE"     -> message
      "RECEIPT"     -> receipt
      "ERROR"       -> prsError
      _             -> fail $ "Unknown message type: '" ++ t ++ "'"

  msgType :: Parser String
  msgType = do
    skipWhite
    t <- A.takeTill (`elem` [cr, eol, spc])
    skipWhite
    terminal
    return $ U.toString t

  beat :: Parser Frame
  beat = return mkBeat

  send :: Parser Frame
  send = bodyFrame mkSndFrame

  message :: Parser Frame
  message = bodyFrame mkMsgFrame

  prsError :: Parser Frame
  prsError = bodyFrame mkErrFrame

  connect :: Parser Frame
  connect = connectFrame mkConFrame

  stomp   :: Parser Frame
  stomp   = genericFrame mkStmpFrame

  connected :: Parser Frame
  connected = connectFrame mkCondFrame

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

  nack :: Parser Frame
  nack = genericFrame mkNackFrame
  
  receipt :: Parser Frame
  receipt = genericFrame mkRecFrame

  ------------------------------------------------------------------------
  -- Frame with body
  ------------------------------------------------------------------------
  bodyFrame :: ([Header] -> Body -> Either String Frame) -> Parser Frame
  bodyFrame mk = do
    hs <- headers True
    b <- body
    case mk hs b of
      Left  e -> fail e
      Right m -> return m

  ------------------------------------------------------------------------
  -- Frame without body and without escaping headers,
  -- i.e. connect and connected
  ------------------------------------------------------------------------
  connectFrame :: ([Header] -> Either String Frame) -> Parser Frame
  connectFrame mk = do
    hs <- headers False
    ignoreBody
    case mk hs of
      Left e  -> fail e
      Right m -> return m

  ------------------------------------------------------------------------
  -- Frame without body
  ------------------------------------------------------------------------
  genericFrame :: ([Header] -> Either String Frame) -> Parser Frame
  genericFrame mk = do
    hs <- headers True
    ignoreBody
    case mk hs of
      Left e  -> fail e
      Right m -> return m

  ------------------------------------------------------------------------
  -- we add each next header found to the head of the list 
  -- of headers already parsed and therefore 
  -- reverse the list of all headers
  ------------------------------------------------------------------------
  headers :: Bool -> Parser [Header]
  headers t = reverse <$> headers' t []

  headers' :: Bool -> [Header] -> Parser [Header]
  headers' t hs = do
    skipWhite
    endHeaders hs <|> getHeader t hs 

  endHeaders :: [Header] -> Parser [Header]
  endHeaders hs = do
    terminal
    return hs

  getHeader :: Bool -> [Header] -> Parser [Header]
  getHeader t hs = do
    h <- header t
    headers' t (h:hs)

  header :: Bool -> Parser Header
  header t = do
    k <- escText t [col] 
    keyValSep
    v <- escText t [cr, eol]
    terminal
    return (U.toString k, U.toString v)

  keyValSep :: Parser ()
  keyValSep = void $ word8 col

  ------------------------------------------------------------------------
  -- end-of-line: either lf or cr ++ lf
  ------------------------------------------------------------------------
  terminal :: Parser ()
  terminal = do
    c <- anyWord8
    case c of
      10 -> return ()
      13 -> void $ word8 eol
      _  -> fail $ "Expecting end-of-line: " ++ show c

  ------------------------------------------------------------------------
  -- read text until null or,
  -- if text length is given,
  -- until text length
  ------------------------------------------------------------------------
  body :: Parser B.ByteString
  body = body' B.empty
    where 
      body' i = do
        _ <- word8 nul
        n <- A.takeTill (== nul)
        let b = i >|< n 
        body' (b |> '\x00')

  ------------------------------------------------------------------------
  -- escape header key and value;
  -- we don't do this for connect and connected frames,
  -- this is controlled by the Bool parameter.
  ------------------------------------------------------------------------
  escText :: Bool -> [Word8] -> Parser B.ByteString
  escText tt stps = go B.empty
    where go t = do
            let stps' | tt        = esc:stps
                      | otherwise =     stps
            n   <- A.takeTill (`elem` stps')
            mbB <- peekWord8
            case mbB of
              Nothing -> fail $ "end reached, expected: " ++ show stps
              Just b  ->
                if b `elem` stps then return (t >|< n)
                  else do 
                    _ <- word8 esc
                    x <- anyWord8
                    c <- case x of
                           92  -> return '\\'
                           99  -> return ':'
                           110 -> return '\n'
                           114 -> return '\r'
                           _   -> fail $ "Unknown escape sequence: " ++ show x
                    go (t >|< n |> c)

  ignoreBody :: Parser ()
  ignoreBody = do 
    _ <- A.takeTill (== nul)
    _ <- word8 nul
    return ()

  skipWhite :: Parser ()
  skipWhite = void $ A.takeWhile (== spc)

  nul, eol, cr, spc, col, esc, _c, _r, _n  :: Word8
  nul  =   0
  eol  =  10
  cr   =  13
  spc  =  32
  col  =  58
  esc  =  92
  _c   =  99
  _r   = 114
  _n   = 110
  
  failBodyLen :: Int -> Int -> Parser a
  failBodyLen l1 l2 = 
    fail $ "Body longer than indicated by content-length: " ++
           show l1 ++ " - " ++ show l2
  
