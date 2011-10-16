module Main
where

  import Network.Mom.Stompl.Frame
  import Network.Mom.Stompl.Parser
  import System.FilePath ((</>))
  import System.Exit
  import System.Environment
  import Data.ByteString as B (readFile)

  testSndToMsg :: String -> Frame -> (Bool, String)
  testSndToMsg i f = 
    case sndToMsg i f of
      Nothing -> (False, "Not a SendFrame")
      Just m  -> 
        let p1 = getHeaders m == getHeaders f
            p2 = getDest    m == getDest f
            p3 = getLength  m == getLength  f
            p4 = getMime    m == getMime f
            p5 = getBody    m == getBody f
        in if p1 && p2 && p3 && p4 && p5 
             then (True, "")
             else 
               let e = if not p1 
                         then "Headers differ"
                         else if not p2 
                                then "Destinations differ"
                                else if not p3 
                                       then "Lengths differ"
                                       else if not p4 
                                              then "Mime Types differ"
                                              else if not p5 
                                                     then "Bodies differ"
                                                     else "unknown error"
               in (False, e)

  applyTests :: FilePath -> IO ()
  applyTests d = do
    t <- B.readFile (d </> "send1-1.1.txt")
    case stompAtOnce t of
      Left e -> do
        putStrLn $ "Can't parse: " ++ e ++ "\n"
        exitFailure
      Right s -> do
        putStrLn $ show $ getHeaders s
        let (v, e) = testSndToMsg "1" s
        if v
          then do
            putStrLn "Ok. Test passed."
            exitSuccess
          else do
            putStrLn $ "Bad. Test failed: " ++ e
            exitFailure
    
  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [d] -> applyTests d
      _   -> do
        putStrLn "I need the directory where I can find the samples!"
        exitFailure
  
    
