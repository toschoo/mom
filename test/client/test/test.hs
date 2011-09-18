module Main where
      import Data.Char
      import Text.ParserCombinators.Parsec
      -- import Data.ByteString.Lex.Double
      -- import qualified Data.ByteString.Char8 as B
      csvFile = endBy line eol
      line = sepBy cell (char ',')
      cell = quotedCell <|> many (noneOf ",\n\r")
       
      quotedCell =
          do char '"'
             content <- many quotedChar
             char '"' <?> "quote at end of cell"
             return content
       
      quotedChar =
              noneOf "\""
          <|> try (string "\"\"" >> return '"')
       
      eol =   try (string "\n\r")
         <|> try (string "\r\n")
         <|> string "\n"
         <|> string "\r"
         <?> "end of line"
       
      parseCSV :: String -> Either ParseError [[String]]
      parseCSV input = parse csvFile "(unknown)" input
       
      printList = mapM_ (\y -> putStr y)
       
      mkFarmItem [x,y] = "t1=5&c=4&\n03.12.2010 3:33:50\n03.12.2010 3:53:00\n01:00:00\nnonstop\n" ++ x ++ "," ++ y ++ "\n" ++ x ++"__"++ y ++ "\n"
       
      mkFarm = map (\x -> mkFarmItem x)
       
      main = do
       x <- readFile "42.csv"
       case (parseCSV x) of
                 Left e -> do putStrLn "Error parsing input:"
                              print e
                 Right r -> do printList $ mkFarm r

