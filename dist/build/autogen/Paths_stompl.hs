module Paths_stompl (
    version,
    getBinDir, getLibDir, getDataDir, getLibexecDir,
    getDataFileName
  ) where

import Data.Version (Version(..))
import System.Environment (getEnv)

version :: Version
version = Version {versionBranch = [0,0,1], versionTags = []}

bindir, libdir, datadir, libexecdir :: FilePath

bindir     = "/home/ts/.cabal/bin"
libdir     = "/home/ts/.cabal/lib/stompl-0.0.1/ghc-6.12.1"
datadir    = "/home/ts/.cabal/share/stompl-0.0.1"
libexecdir = "/home/ts/.cabal/libexec"

getBinDir, getLibDir, getDataDir, getLibexecDir :: IO FilePath
getBinDir = catch (getEnv "stompl_bindir") (\_ -> return bindir)
getLibDir = catch (getEnv "stompl_libdir") (\_ -> return libdir)
getDataDir = catch (getEnv "stompl_datadir") (\_ -> return datadir)
getLibexecDir = catch (getEnv "stompl_libexecdir") (\_ -> return libexecdir)

getDataFileName :: FilePath -> IO FilePath
getDataFileName name = do
  dir <- getDataDir
  return (dir ++ "/" ++ name)
