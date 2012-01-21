module Main
where

  import Types
  import Service
  import Helper

  main :: IO ()
  main = do
    let add = DEVICE (ADD $ pollEntry "Test" XSub 
                             (address Connect "tcp" "localhost" 5555 [])
                             Connect "zipcode") 
    let rmv = DEVICE (REM "Test")
    let tmo = DEVICE (TMO 500)

    let adds = show add
    let rmvs = show rmv
    let tmos = show tmo

    putStrLn adds
    putStrLn rmvs
    putStrLn tmos

    let add' = read adds :: Command
    let rmv' = read rmvs :: Command
    let tmo' = read tmos :: Command

    putStrLn $ show add'
    putStrLn $ show rmv'
    putStrLn $ show tmo'
    
