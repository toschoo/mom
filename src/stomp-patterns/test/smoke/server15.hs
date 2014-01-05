module Main
where

  import Network.Mom.Stompl.Client.Queue
  import Network.Mom.Stompl.Patterns.Basic
  import qualified Data.ByteString.Char8 as B
  import Network.Socket
  import Control.Monad (forever)
  import Codec.MIME.Type (nullType)
  import System.Environment
  import System.Exit

  main :: IO ()
  main = do
    os <- getArgs
    case os of
      [q] -> withSocketsDo $ tstReply q
      _   -> do
        putStrLn "I need a queue and nothing else."
        exitFailure


  tstReply :: QName -> IO ()
  tstReply q = 
    withConnection "127.0.0.1" 61613 [] [] $ \c -> 
      withServer c "Test" (q,            [], [], iconv)
                          ("unknown",    [], [], oconv) $ \s -> do
        (sc,me) <- if null reg 
                     then return (OK, 0)
                     else register c jn Service reg rn tmo best
        case sc of
          OK -> 
            if me < mn || me > mx
              then do void $ unRegister c jn wn rn tmo
                      throwIO $ UnacceptableHbX me

              else do hb <- mkHB me
                      m  <- newMVar hb 
                      let p = if me <= 0 then (-1) else 1000 * me 
                      withWriter c "HB" reg [] [] nobody $ \w -> 
                      finally (forever $
                        reply s p t hs transform >> heartbeat m w jn rn) (do
                        sc <- unRegister c jn wn rn tmo
                              unless (sc == OK) $ 
                                throwIO $ NotOKX sc "on unregister")

    where iconv _ _ _ = return . B.unpack
          oconv       = return . B.pack
          createReply = return . reverse . msgContent

