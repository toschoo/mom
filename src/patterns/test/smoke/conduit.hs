module Main 
where

  import           Control.Monad.Trans
  import           Data.Conduit (($$), ($=), (=$=))
  import qualified Data.Conduit          as C
  import qualified Data.Conduit.Binary   as CB
  import qualified Data.Conduit.Text     as CT
  import qualified Data.Conduit.List     as CL
  import qualified Data.ByteString.Char8 as B
  

  main :: IO ()
  main = sink out -- C.runResourceT (list [1..10] $$ out)

  sink :: C.Sink Int (C.ResourceT IO) () -> IO ()
  sink s = C.runResourceT $ list [1..10] $$ s

  list :: C.MonadResource m => [Int] -> C.Source m Int
  list [] = return ()
  list (i:is) = C.yield i >> list is

  out :: C.Sink Int (C.ResourceT IO) ()
  out = C.awaitForever $ \i -> liftIO $ print i
