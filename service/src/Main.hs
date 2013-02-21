{-# LANGUAGE BangPatterns, FlexibleContexts, GeneralizedNewtypeDeriving, OverloadedStrings, TupleSections, TypeOperators #-}
import Prelude hiding (readFile)

import Control.Applicative
import Control.Monad (forM, join, mzero)
import Data.Aeson
import Data.ByteString.Lazy (readFile)
import Data.HashSet (HashSet)
import Data.Hashable
import Data.List (isSuffixOf)
import Data.Maybe (catMaybes)
import Data.Monoid
import Data.Text (Text)
import Data.Time (UTCTime, readTime, parseTime)
import Data.UUID (fromString)
import Snap.Core
import Snap.Http.Server
import Snap.Util.GZip (withCompression)
import System.Directory
import System.FilePath
import System.Locale (defaultTimeLocale)
import qualified Data.HashSet as HashSet
import qualified Data.IntMap as IntMap
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Encoding

--------------------------------------------------------------------------------
-- | A 'ChangePacket' is a set of changed MBIDs, up to a specific point in time.
data ChangePacket = ChangePacket { packetChangeset :: ChangeSet
                                 , packetTimestamp :: UTCTime
                                 }


--------------------------------------------------------------------------------
-- | A 'ChangeSet' consists of a set of 'MBID's that have changed in some way,
-- partitioned into the space those MBIDs exist in (artist, label, etc).
data ChangeSet = ChangeSet { artistChanges :: !(HashSet MBID)
                           , labelChanges :: !(HashSet MBID)
                           , recordingChanges :: !(HashSet MBID)
                           , releaseChanges :: !(HashSet MBID)
                           , releaseGroupChanges :: !(HashSet MBID)
                           , workChanges :: !(HashSet MBID)
                           }


intersect :: ChangeSet -> ChangeSet -> ChangeSet
intersect a b = ChangeSet
  { artistChanges = artistChanges a `HashSet.intersection` artistChanges b
  , labelChanges = labelChanges a `HashSet.intersection` labelChanges b
  , recordingChanges = recordingChanges a `HashSet.intersection` recordingChanges b
  , releaseChanges = releaseChanges a `HashSet.intersection` releaseChanges b
  , releaseGroupChanges = releaseGroupChanges a `HashSet.intersection` releaseGroupChanges b
  , workChanges = workChanges a `HashSet.intersection` workChanges b
  }


--------------------------------------------------------------------------------
-- | An 'MBID' is the plain text, base 16, representation of a UUID. This is
-- a @newtype@ around 'Text' for speed, but also so you don't start accidently
-- modifying them.
newtype MBID = MBID Text
  deriving (Eq, Show, Ord, Hashable, ToJSON)


--------------------------------------------------------------------------------
-- The 'ChangeSet' 'Monoid' simply unions all the MBIDs together, respecting
-- the partitions they come from
instance Monoid ChangeSet where
  mempty = ChangeSet mempty mempty mempty mempty mempty mempty

  a `mappend` b = ChangeSet
    { artistChanges = artistChanges a `mappend` artistChanges b
    , labelChanges = labelChanges a `mappend` labelChanges b
    , recordingChanges = recordingChanges a `mappend` recordingChanges b
    , releaseGroupChanges = releaseGroupChanges a `mappend` releaseGroupChanges b
    , releaseChanges = releaseChanges a `mappend` releaseChanges b
    , workChanges = workChanges a `mappend` workChanges b
    }

  mconcat as = ChangeSet
    { artistChanges = HashSet.unions (map artistChanges as)
    , labelChanges = HashSet.unions (map labelChanges as)
    , recordingChanges = HashSet.unions (map recordingChanges as)
    , releaseChanges = HashSet.unions (map releaseChanges as)
    , releaseGroupChanges = HashSet.unions (map releaseGroupChanges as)
    , workChanges = HashSet.unions (map workChanges as)
    }


--------------------------------------------------------------------------------
-- We parse 'ChangePacket's from JSON by reading out the timestamp, and turning
-- each list of MBIDs into a 'HashSet'.
instance FromJSON ChangePacket where
  parseJSON (Object j) =
      ChangePacket <$> j .: "data"
                   <*> (readTime defaultTimeLocale "%Y-%m-%d %H:%M:%S%Q+00:00" <$> j .: "timestamp")

  parseJSON _ = mzero


instance FromJSON ChangeSet where
  parseJSON (Object o) =
    ChangeSet <$> (HashSet.fromList <$> o .: "artist" <|> pure mempty)
              <*> (HashSet.fromList <$> o .: "label" <|> pure mempty)
              <*> (HashSet.fromList <$> o .: "recording" <|> pure mempty)
              <*> (HashSet.fromList <$> o .: "release" <|> pure mempty)
              <*> (HashSet.fromList <$> o .: "release_group" <|> pure mempty)
              <*> (HashSet.fromList <$> o .: "work" <|> pure mempty)
  parseJSON _ = mzero


--------------------------------------------------------------------------------
-- We turn 'ChangePacket's back to JSON is *almost* the same way, except we use
-- the @latest@ key for the timestamp, to point to the latest known change
-- packet.
instance ToJSON ChangePacket where
  toJSON (ChangePacket c ts) =
    object
      [ "latest" .= ts
      , "data" .= object
          [ "artist" .= artistChanges c
          , "label" .= labelChanges c
          , "recording" .= recordingChanges c
          , "release" .= releaseChanges c
          , "release_group" .= releaseGroupChanges c
          , "work" .= workChanges c
          ]
        ]


--------------------------------------------------------------------------------
instance FromJSON MBID where
  parseJSON (String s) = maybe mzero (const $ return (MBID s)) $ fromString (Text.unpack s)
  parseJSON _ = mzero


--------------------------------------------------------------------------------
-- | Given a directory with JSON files name xxx.json, where xxx is the sequence
-- index, this will return a list of associations of sequence index to
-- 'ChangePacket'. Any 'ChangePacket's that cannot be parsed will be skipped.
loadChangeSets :: FilePath -> IO [(Int, ChangePacket)]
loadChangeSets d = do
  changeSetFiles <- filter (isSuffixOf ".json") <$> getDirectoryContents d
  fmap catMaybes $ forM changeSetFiles $ \f -> do
    let csId = read $ reverse $ drop 5 $ reverse f
    fmap (csId, ) . decode' <$> readFile (d </> f)


--------------------------------------------------------------------------------
main :: IO ()
main = do
  changePackets <- loadChangeSets "change-sets"

  -- changeSets is an 'IntMap' to 'ChangeSet's.
  let changeSets =
        IntMap.fromList $ map (\(i, ChangePacket c _) -> (i, c)) changePackets

  -- dateMapper is a map from a 'ChangePacket's timestamp to its sequence index
  let dateMapper =
        Map.fromList $ map (\(i, ChangePacket _ ts) -> (ts, i)) changePackets

  quickHttpServe $ route
    [("/since/:x", if null changePackets
                     then emptyServer
                     else withCompression $ since changeSets dateMapper)]

 where

   -- Main request handler
   since changeSets dateMapper = do
     t' <- join . fmap parseTimeParameter <$> getParam "x"
     modifyResponse (setContentType "application/json")
     case t' of
       Nothing -> clientError "Timestamp could not be parsed" []
       Just t -> do
         -- The format of the timestamp was valid, but we need to make sure
         -- its in our domain
         case Map.lookupGE t dateMapper of
           Nothing ->
             clientError "Specified timestamp is later than the latest known changes"
               [ "latest" .= Map.findMax dateMapper ]
           Just (latestTime, csId) -> do
             -- The time was in our domain, so we round *up* to the nearest
             -- change set, and then take change set and all subsequent
             -- change sets. We then emit the union of these.
             let (_, !cs, !futureCs) = IntMap.splitLookup csId changeSets
             let changeSet = mconcat $ catMaybes $ map Just (IntMap.elems futureCs) ++ [ cs ]

             finalChangeSet' <- method GET (pure $ Just changeSet)
                            <|> (method POST $ fmap (intersect changeSet) . decode' <$> readRequestBody 10485760)

             case finalChangeSet' of
               Just finalChangeSet ->
                 writeLBS $ encode $ ChangePacket { packetChangeset = finalChangeSet
                                                  , packetTimestamp = latestTime
                                                  }
               Nothing -> clientError "Unable to parse request body" []

   parseTimeParameter = parseTime defaultTimeLocale "%Y-%m-%dT%H:%M:%SZ" .
     Text.unpack . Encoding.decodeUtf8

   clientError t extra =
     writeError 400 $ [ "error" .= (t :: Text) ] ++ extra

   emptyServer =
     writeError 500 [ "error" .= ("The server has no change sets"::Text) ]

   writeError code e = do
     writeLBS $ encode $ object e
     modifyResponse (setResponseCode code)
