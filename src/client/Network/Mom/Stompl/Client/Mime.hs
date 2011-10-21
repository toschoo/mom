module Mime
where

  import Codec.MIME.Type as Mime
  import Text.ParserCombinators.Parsec
  import Control.Applicative ((<$>))
  import Data.Char (toLower)

  type Param = (String, String)

  mime :: Parser Mime.Type
  mime = do
    m <- mimeTyp
    p <- params <|> (eof >> return [])
    return $ Mime.Type m p

  mimeTyp :: Parser MIMEType
  mimeTyp = do
    t <- many1 $ noneOf (white ++ specials)
    _ <- char '/'
    s <- many1 $ noneOf (white ++ specials)
    let m = case map toLower t of
              "application" -> Mime.Application s
              "audio"       -> Mime.Audio       s
              "image"       -> Mime.Image       s
              "message"     -> Mime.Message     s
              "model"       -> Mime.Model       s
              -- "MULTIPART"   -> Mime.Type (Mime.Multipart   sub) []
              "text"        -> Mime.Text        s
              "video"       -> Mime.Video       s
              _             -> Mime.Other t     s
    return m

  params :: Parser [Param]
  params = do
    ch <- lookAhead anyChar
    if ch == ';'
      then many1 param
      else skipWhite >> eof >> return []

  param :: Parser Param
  param = do
    _   <- char ';'
    at  <- many1 $ noneOf (white ++ specials)
    _   <- char '='
    val <- value
    if null val 
      then fail "Parameter without value"
      else return (at, val)
  
  value :: Parser String
  value = do
    ch <- anyChar
    if ch == ';' then return ""
      else if ch == '"'
           then do
             ch1 <- anyChar
             ch2 <- char '"'
             (ch1:) <$> valueOrEof
           else do
             (ch:) <$> valueOrEof

  valueOrEof :: Parser String
  valueOrEof = (eof >> return "") <|> value

  skipWhite :: Parser ()
  skipWhite = optional $ skipMany1 $ oneOf white
  
  specials, white :: String                    
  specials = "()<>@,;:\\\"/[]?="
  white = "\n\t "

  showParam :: Param -> String
  showParam (a, v) = ';' : (a ++ "=" ++ showVal v)

  showParams :: [Param] -> String
  showParams ps = foldr (\x xs -> showParam x ++ xs) [] ps

  showVal :: String -> String
  showVal [] = []
  showVal (x:xs) = if x `elem` specials 
                     then ['"', x, '"'] ++ showVal xs
                     else x : showVal xs

