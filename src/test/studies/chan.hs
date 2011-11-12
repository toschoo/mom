
import Control.Concurrent

main :: IO ()
main = do
    todo <- newChan
    forkIO $ readChan todo
    putStrLn "Before isEmptyChan"
    b <- isEmptyChan todo
    putStrLn "After isEmptyChan"
    writeChan todo ()

