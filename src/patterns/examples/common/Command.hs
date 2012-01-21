module Command
where

  import           Network.Mom.Patterns
  import qualified Data.ByteString.Char8 as B

  data Command = AddPort Identifier LinkType Int
               | RemPort Identifier
               | Tmo     Timeout
               | Stop
               | Empty
    deriving (Show, Read)

  data Result = OK | Err String
    deriving (Show, Read)

  toCmd :: InBound Command
  toCmd = return . read . B.unpack

  fromCmd :: OutBound Command
  fromCmd = return . B.pack . show

  fromResult :: OutBound Result
  fromResult = return . B.pack . show

  toResult :: InBound Result
  toResult = return . read . B.unpack

