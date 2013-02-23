{-# LANGUAGE BangPatterns, FlexibleContexts, GeneralizedNewtypeDeriving, OverloadedStrings, TupleSections, TypeOperators #-}
import Prelude hiding (readFile)

import Control.Applicative
import Control.Concurrent.STM
import Control.Monad (forM, join, mzero, void)
import Control.Monad.IO.Class (liftIO)
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
import System.Posix.Signals
import qualified Data.Aeson.Types as Aeson
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
-- | A 'UnionPacket' is just like a 'ChangePacket', except the timestamp is
-- the timestamp of the latest change packet the server knows about.
newtype UnionPacket = UnionPacket ChangePacket

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
instance ToJSON UnionPacket where
  toJSON (UnionPacket (ChangePacket c ts)) =
    object [ "latest" .= ts, "data" .= c]


instance ToJSON ChangeSet where
  toJSON c = object
    [ "artist" .= artistChanges c
    , "label" .= labelChanges c
    , "recording" .= recordingChanges c
    , "release" .= releaseChanges c
    , "release_group" .= releaseGroupChanges c
    , "work" .= workChanges c
    ]

--------------------------------------------------------------------------------
instance FromJSON MBID where
  parseJSON (String s) = maybe mzero (const $ return (MBID s)) $ fromString (Text.unpack s)
  parseJSON _ = mzero


--------------------------------------------------------------------------------
-- | Given a directory with JSON files name xxx.json, where xxx is the sequence
-- index, this will return a list of associations of sequence index to
-- 'ChangePacket'. Any 'ChangePacket's that cannot be parsed will be skipped.
loadChangeSets :: FilePath -> IO (IntMap.IntMap ChangeSet, Map.Map UTCTime IntMap.Key)
loadChangeSets d = do
  changeSetFiles <- filter (isSuffixOf ".json") <$> getDirectoryContents d

  changePackets <- fmap catMaybes $ forM changeSetFiles $ \f -> do
    let csId = read $ reverse $ drop 5 $ reverse f
    fmap (csId, ) . decode' <$> readFile (d </> f)

  -- changeSets is an 'IntMap' to 'ChangeSet's.
  let changeSets =
        IntMap.fromList $ map (\(i, ChangePacket c _) -> (i, c)) changePackets

  -- dateMapper is a map from a 'ChangePacket's timestamp to its sequence index
  let dateMapper =
        Map.fromList $ map (\(i, ChangePacket _ ts) -> (ts, i)) changePackets

  return (changeSets, dateMapper)


--------------------------------------------------------------------------------
main :: IO ()
main = do
  (changeSetsTVar, dateMapperTVar) <- liftIO $ atomically $
    (,) <$> newTVar mempty <*> newTVar mempty

  let goReload = reload changeSetsTVar dateMapperTVar
  goReload

  void $ installHandler sigHUP (Catch goReload) Nothing

  quickHttpServe $ withCompression $ do
    modifyResponse (setContentType "application/json")
    route
      [ ("/since/:timeStamp", since changeSetsTVar dateMapperTVar)
      , ("/on/:timeStamp", changesOn changeSetsTVar dateMapperTVar)
      , ("/latest", redirectToLatest changeSetsTVar)
      ]


--------------------------------------------------------------------------------
reload :: TVar (IntMap.IntMap ChangeSet) -> TVar (Map.Map UTCTime IntMap.Key) -> IO ()
reload changeSetsTVar dateMapperTVar = do
   (changeSets, dateMapper) <- loadChangeSets "change-sets"
   atomically $ do
     readTVar changeSetsTVar *> writeTVar changeSetsTVar changeSets
     readTVar dateMapperTVar *> writeTVar dateMapperTVar dateMapper


--------------------------------------------------------------------------------
-- | Main request handler
since :: MonadSnap m => TVar (IntMap.IntMap ChangeSet) -> TVar (Map.Map UTCTime IntMap.Key) -> m ()
since changeSetsTVar dateMapperTVar = do
  t' <- parseTimeParameter
  case t' of
    Nothing -> clientError "Timestamp could not be parsed" []
    Just t -> do
      -- The format of the timestamp was valid, but we need to make sure its
      -- in our domain. It is at this point we read the current known change
      -- sets/date mapper. Hence a reload during this request will have no
      -- effect.
      (dateMapper, changeSets) <- liftIO $ atomically $
        (,) <$> readTVar dateMapperTVar <*> readTVar changeSetsTVar

      if IntMap.null changeSets
        then emptyServer
        else do
          let (latestTime, _) = Map.findMax dateMapper
          case Map.lookupGE t dateMapper of
            Nothing ->
              clientError "Specified timestamp is later than the latest known changes"
                [ "latest" .= latestTime ]
            Just (_, csId) -> do
              -- The time was in our domain, so we round *up* to the nearest
              -- change set, and then take change set and all subsequent
              -- change sets. We then emit the union of these.
              let (_, !cs, !futureCs) = IntMap.splitLookup csId changeSets
              let changeSet = mconcat $ catMaybes $ map Just (IntMap.elems futureCs) ++ [ cs ]

              withFilteredChangeSet changeSet $ \c -> writeJSON $
                UnionPacket ChangePacket { packetChangeset = c
                                         , packetTimestamp = latestTime
                                         }


--------------------------------------------------------------------------------
-- | Main request handler
changesOn :: MonadSnap m => TVar (IntMap.IntMap ChangeSet) -> TVar (Map.Map UTCTime IntMap.Key) -> m ()
changesOn changeSetsTVar dateMapperTVar = do
  t' <- parseTimeParameter
  case t' of
    Nothing -> clientError "Timestamp could not be parsed" []
    Just t -> do
      -- The format of the timestamp was valid, but we need to make sure its
      -- in our domain. It is at this point we read the current known change
      -- sets/date mapper. Hence a reload during this request will have no
      -- effect.
      (dateMapper, changeSets) <- liftIO $ atomically $
        (,) <$> readTVar dateMapperTVar <*> readTVar changeSetsTVar

      if IntMap.null changeSets
        then emptyServer
        else do
          case Map.lookupGE t dateMapper of
            Nothing ->
              clientError "Specified timestamp is later than the latest known changes"
                [ "latest" .= fst (Map.findMax dateMapper) ]
            Just (_, csId) -> redirectTo csId


--------------------------------------------------------------------------------
redirectTo :: MonadSnap m => Int -> m ()
redirectTo csId = redirect $ Encoding.encodeUtf8 $ Text.pack $
  "http://changed-mbids.musicbrainz.org/pub/musicbrainz/data/changed-mbids/changed-ids-" ++
    (show csId) ++ ".json.gz"


--------------------------------------------------------------------------------
redirectToLatest :: MonadSnap m => TVar (IntMap.IntMap ChangeSet) -> m ()
redirectToLatest changeSetsTVar = do
  changeSets <- liftIO $ atomically $ readTVar changeSetsTVar
  if IntMap.null changeSets
    then emptyServer
    else redirectTo (fst $ IntMap.findMax changeSets)


--------------------------------------------------------------------------------
withFilteredChangeSet :: MonadSnap m => ChangeSet -> (ChangeSet -> m ()) -> m ()
withFilteredChangeSet changeSet a = do
  finalChangeSet' <- method GET (pure $ Just changeSet)
                 <|> (method POST $
                        fmap (intersect changeSet) . decode'
                          <$> readRequestBody 10485760)

  maybe noParse a finalChangeSet'

  where
    noParse = clientError "Unable to parse request body" []

writeJSON :: (MonadSnap m, ToJSON a) => a -> m ()
writeJSON = writeLBS . encode

--------------------------------------------------------------------------------
parseTimeParameter :: MonadSnap m => m (Maybe UTCTime)
parseTimeParameter = join . fmap parser <$> getParam "timeStamp"
  where parser = parseTime defaultTimeLocale "%Y-%m-%dT%H:%M:%SZ" .
          Text.unpack . Encoding.decodeUtf8


--------------------------------------------------------------------------------
clientError :: MonadSnap m => Text -> [Aeson.Pair] -> m ()
clientError e extra =
  writeError 400 $ [ "error" .= e ] ++ extra


--------------------------------------------------------------------------------
serverError :: MonadSnap m => Text -> m ()
serverError e =
  writeError 500 $ [ "error" .= e ]


--------------------------------------------------------------------------------
emptyServer :: MonadSnap m => m ()
emptyServer = serverError "The server has no change sets"


--------------------------------------------------------------------------------
writeError :: MonadSnap m => Int -> [Aeson.Pair] -> m ()
writeError code e = do
  writeLBS $ encode $ object e
  modifyResponse (setResponseCode code)
