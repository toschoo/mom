module Main 
where
 
  ------------------------------------------------------------------------
  -- pushes the contents of a file to the workers
  ------------------------------------------------------------------------

  import           Helper (getOs, address)
  import           Network.Mom.Patterns
  import qualified Data.Enumerator.Binary as EB

  main :: IO ()
  main = do
    (l, p, xs) <- getOs
    case xs of
      [x] -> doit l p x
      _   -> error "I need a file name"
    
  doit :: LinkType -> Int -> FilePath -> IO ()
  doit l p f = withContext 1 $ \ctx -> do
    let ap = address l "tcp" "localhost" p []
    withPipe ctx ap return $ \pu ->
      push pu (EB.enumFile f)
