{-# LANGUAGE BangPatterns, FlexibleContexts, GeneralizedNewtypeDeriving, OverloadedStrings, TypeOperators #-}
import Prelude hiding (readFile)
import Control.Applicative
import Control.Monad (forM, mzero)
import Data.Aeson
import Data.ByteString.Lazy (readFile)
import Data.IntMap (IntMap)
import Data.Hashable
import Data.List
import Data.Maybe (catMaybes, fromMaybe)
import Data.Monoid
import Data.HashSet (HashSet)
import Data.Text (Text)
import Data.UUID
import Snap.Core
import Snap.Http.Server
import System.Directory
import System.FilePath
import qualified Data.IntMap as IntMap
import qualified Data.HashSet as HashSet
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Encoding

data ChangeSet = ChangeSet { artistChanges :: !(HashSet MBID)
                           , labelChanges :: !(HashSet MBID)
                           , recordingChanges :: !(HashSet MBID)
                           , releaseChanges :: !(HashSet MBID)
                           , releaseGroupChanges :: !(HashSet MBID)
                           , workChanges :: !(HashSet MBID)
                           }

newtype MBID = MBID Text
  deriving (Eq, Show, Ord, Hashable, ToJSON)

instance Monoid ChangeSet where
  mempty = ChangeSet mempty mempty mempty mempty mempty mempty
  a `mappend` b = ChangeSet { artistChanges = artistChanges a `mappend` artistChanges b
                            , labelChanges = labelChanges a `mappend` labelChanges b
                            , recordingChanges = recordingChanges a `mappend` recordingChanges b
                            , releaseGroupChanges = releaseGroupChanges a `mappend` releaseGroupChanges b
                            , releaseChanges = releaseChanges a `mappend` releaseChanges b
                            , workChanges = workChanges a `mappend` workChanges b
                            }
  mconcat as = ChangeSet { artistChanges = HashSet.unions (map artistChanges as)
                         , labelChanges = HashSet.unions (map labelChanges as)
                         , recordingChanges = HashSet.unions (map recordingChanges as)
                         , releaseChanges = HashSet.unions (map releaseChanges as)
                         , releaseGroupChanges = HashSet.unions (map releaseGroupChanges as)
                         , workChanges = HashSet.unions (map workChanges as)
                         }


instance FromJSON ChangeSet where
  parseJSON (Object j) = j .: "data" >>= go
    where go o = ChangeSet <$> (HashSet.fromList <$> o .: "artist")
                           <*> (HashSet.fromList <$> o .: "label")
                           <*> (HashSet.fromList <$> o .: "recording")
                           <*> (HashSet.fromList <$> o .: "release")
                           <*> (HashSet.fromList <$> o .: "release_group")
                           <*> (HashSet.fromList <$> o .: "work")
  parseJSON _ = mzero

instance ToJSON ChangeSet where
  toJSON c =
    object [
      "data" .= object
        [ "artist" .= artistChanges c
        , "label" .= labelChanges c
        , "recording" .= recordingChanges c
        , "release" .= releaseChanges c
        , "release_group" .= releaseGroupChanges c
        , "work" .= workChanges c
        ]
      ]

instance FromJSON MBID where
  parseJSON (String s) = maybe mzero (const $ return (MBID s)) $ fromString (Text.unpack s)
  parseJSON _ = mzero

loadChangeSets :: FilePath -> IO (IntMap ChangeSet)
loadChangeSets d = do
  changeSetFiles <- filter (isSuffixOf ".json") <$> getDirectoryContents d
  fmap IntMap.fromList $ forM changeSetFiles $ \f -> do
    let csId = read $ reverse $ drop 5 $ reverse f
    changeSet <- fromMaybe (error "Failed to decode") . decode' <$> readFile (d </> f)
    return (csId, changeSet)

main :: IO ()
main = do
  changeSets <- loadChangeSets "change-sets"
  quickHttpServe $ route [("/since/:x", since changeSets)]
 where
   since changeSets = do
     Just csId <- fmap (read . Text.unpack . Encoding.decodeUtf8) <$> getParam "x"
     let (_, !cs, !futureCs) = IntMap.splitLookup csId changeSets
     writeLBS $ encode $ mconcat $ catMaybes $ map Just (IntMap.elems futureCs) ++ [ cs ]
