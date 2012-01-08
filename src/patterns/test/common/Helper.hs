module Helper
where

  import qualified Data.Enumerator        as E
  import           Data.Enumerator (($$))
  import qualified Data.Enumerator.Binary as EB
  import qualified Data.Enumerator.List   as EL
  import qualified Data.ByteString.Char8  as B
  import qualified Database.HDBC          as SQL
  import           Control.Monad.Trans
  import           Network.Mom.Patterns
  import qualified System.IO              as IO
  import           System.Environment

  getOs :: IO (LinkType, Int, [String])
  getOs = do
    os <- getArgs
    case os of
      [t, p]  -> return (read t, read p, [])
      (t:p:r) -> return (read t, read p, r)
      _       -> usage 

  getPorts :: IO (Int, Int, [String])
  getPorts = do
    os <- getArgs
    case os of
      [s, t]  -> return (read s, read t, [])
      (s:t:r) -> return (read s, read t, r)
      _       -> usage 

  usage :: IO a
  usage = error "<program> 'bind' | 'connect' <port>"

  address :: LinkType         -> 
             String -> String -> Int -> 
             [SocketOption]   -> AccessPoint
  address t prot add port os = 
    case t of
      Bind    -> Address (prot ++ "://*:"             ++ show port) os
      Connect -> Address (prot ++ "://" ++ add ++ ":" ++ show port) os

  onErr :: OnError
  onErr e n = do
    putStrLn $ "Error in " ++ n ++ ": " ++ show e
    return Nothing 

  onErr_ :: OnError_
  onErr_ e n = putStrLn $ "Error in " ++ n ++ ": " ++ show e

  dbExec :: SQL.Statement -> [SQL.SqlValue] -> IO ()
  dbExec s ps = SQL.execute s ps >>= \_ -> return ()

  dbFetcher :: SQL.Statement -> Fetch [SQL.SqlValue] String
  dbFetcher s _ _ stp = tryIO (SQL.execute s []) >>= \_ -> go stp
    where go step = 
            case step of
              E.Continue k -> do
                mbR <- tryIO $ SQL.fetchRow s
                case mbR of
                  Nothing -> E.continue k
                  Just r  -> go $$ k (E.Chunks [convRow r])
              _ -> E.returnI step
          convRow :: [SQL.SqlValue] -> String
          convRow [sqlId, sqlName] =
              show idf ++ ": " ++ name
            where idf  = (SQL.fromSql sqlId)::Int
                  name = case SQL.fromSql sqlName of
                           Nothing -> "NN"
                           Just r  -> r
          convRow _ = undefined

  fileFetcher :: IO.Handle -> Fetch_ B.ByteString 
  fileFetcher h c _ = handleFetcher 4096 h c ()

  handleFetcher :: Integer -> IO.Handle -> Fetch_ B.ByteString
  handleFetcher bufSize h _ _ = EB.enumHandle bufSize h

  output :: Dump String
  output c = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> liftIO (putStrLn i) >> output c

  outit :: E.Iteratee String IO ()
  outit = do
    mbi <- EL.head
    case mbi of
      Nothing -> return ()
      Just i  -> liftIO (putStrLn i) >> outit


