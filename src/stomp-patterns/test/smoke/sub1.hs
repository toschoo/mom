module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)
  import System.Environment
  import System.Exit

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q] -> withSocketsDo $ tstSub q 
      _   -> do
        putStrLn "I need a queue and nothing else."
        exitFailure


  tstSub :: QName -> IO ()
  tstSub q = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withSub c "Test" "Pub1" q 500000 
              ("/q/sub1", [], [], iconv) $ \s -> forever $ do
        mbM <- checkIssue s (-1)
        case mbM of
          Nothing -> error "Nothing with timeout -1!!!"
          Just m  -> print $ msgContent m
    where iconv :: InBound Int
          iconv _ _ _ = return . read . B.unpack 

