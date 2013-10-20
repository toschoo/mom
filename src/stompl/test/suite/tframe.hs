module Main
where

  import Network.Mom.Stompl.Frame
  import Network.Mom.Stompl.Parser
  import System.FilePath ((</>))
  import System.Exit
  import System.Environment
  import Data.ByteString as B (readFile)

  testSndToMsg :: String -> String -> Frame -> (Bool, String)
  testSndToMsg i sub f = 
    case sndToMsg i sub f of
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
               let e = ifList [(p1, "Headers differ"),
                               (p2, "Destinations differ"),
                               (p3, "Lengths differ"),
                               (p4, "MIME Types differ"),
                               (p5, "Bodies differ")]
               in (False, e)

  ifList :: [(Bool, String)] -> String
  ifList [] = "Unknown error"
  ifList ((p, s):xs) | p = ifList xs
                     | otherwise = s 

  applyTests :: FilePath -> IO ()
  applyTests d = do
    t <- B.readFile (d </> "send1-1.1.txt")
    case stompAtOnce t of
      Left e -> do
        putStrLn $ "Can't parse: " ++ e ++ "\n"
        exitFailure
      Right s -> do
        putStrLn $ show $ getHeaders s
        let (v, e) = testSndToMsg "1" "/queue/test" s
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
  
    
