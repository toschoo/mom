module Main
where 
  
  -- import Network.Mom.Stompl.Parser
  import           Data.Attoparsec
  import qualified Data.ByteString.Char8 as B
  import           Data.Word

  t1 :: B.ByteString
  t1 = B.pack "hello \nworld\\c this is \\\\ a test:"


  main :: IO ()
  main = 
    let eiR = parseOnly (escText [58]) t1
     in case eiR of
          Left  e -> putStrLn e
          Right r -> putStrLn $ B.unpack r

  escText :: [Word8] -> Parser B.ByteString
  escText stps = go B.empty
    where go t = do
            n <- takeTill (`elem` esc:stps)
            b <- anyWord8
            if b `elem` stps then return (t >|< n)
              else do 
                x <- anyWord8
                c <- case x of
                       13 -> return '\x13'
                       10 -> return '\n'
                       92 -> return '\\'
                       99 -> return ':'
                       _  -> fail $ "Unknown escape sequence: " ++ show x
                go (t >|< n |> c)

  esc :: Word8
  esc = 92

  (>|<) = B.append
  (|>)  = B.snoc

