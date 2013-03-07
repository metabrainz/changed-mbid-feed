{-# LANGUAGE BangPatterns, FlexibleContexts, GeneralizedNewtypeDeriving, OverloadedStrings, TupleSections, TypeOperators #-}
import Prelude hiding (readFile)

import Control.Applicative
import Control.Concurrent.STM
import Control.Error (note)
import Control.Monad (forM, join, mzero, void)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Either
import Data.Aeson
import Data.Configurator (Worth(..), autoReload, autoConfig, require)
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
import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashSet as HashSet
import qualified Data.IntMap as IntMap
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.Text.IO as Text
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


totalSize :: ChangeSet -> Int
totalSize (ChangeSet artist label recording release rg work) =
  sum $ map HashSet.size [ artist, label, recording, release, rg, work]

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
    fmap (csId, ) . decode' . LBS.fromChunks . pure . Encoding.encodeUtf8
      <$> Text.readFile (d </> f)

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

  (cfg, _) <- autoReload autoConfig [ Required "service.config" ]

  void $ installHandler sigHUP (Catch goReload) Nothing

  quickHttpServe $ withCompression $ do
    modifyResponse (setContentType "application/json")

    (dateMapper, changeSets) <- liftIO $ atomically $
      (,) <$> readTVar dateMapperTVar <*> readTVar changeSetsTVar

    maxRequestSize <- liftIO (require cfg "max-request-size")

    eitherT id return $ do
      assertNotEmpty changeSets
      lift $ route
        [ ("/since/:timeStamp", since changeSets dateMapper maxRequestSize)
        , ("/on/:timeStamp", changesOn dateMapper)
        , ("/latest", redirectToLatest changeSets)
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
since :: MonadSnap m => IntMap.IntMap ChangeSet -> Map.Map UTCTime IntMap.Key -> Int -> m ()
since changeSets dateMapper maxRequestSize = eitherT id return $ do
  t <- parseTimeParameter

  csId <- nearestChangeSet t dateMapper
  let (_, !cs, !futureCs) = IntMap.splitLookup csId changeSets
  let changeSet = mconcat $ catMaybes $ map Just (IntMap.elems futureCs) ++ [ cs ]

  withFilteredChangeSet changeSet maxRequestSize $ \c -> writeJSON $
    UnionPacket ChangePacket { packetChangeset = c
                             , packetTimestamp = fst $ Map.findMax dateMapper
                             }

--------------------------------------------------------------------------------
-- | Main request handler
changesOn :: MonadSnap m => Map.Map UTCTime IntMap.Key -> m ()
changesOn dateMapper = eitherT id return $ do
  t <- parseTimeParameter

  case Map.lookupGE t dateMapper of
    Nothing -> EitherT $ return $ Left $
      clientError "Specified timestamp is later than the latest known changes"
        [ "latest" .= fst (Map.findMax dateMapper) ]
    Just (_, csId) -> lift (redirectTo csId)


--------------------------------------------------------------------------------
nearestChangeSet :: MonadSnap m => UTCTime -> Map.Map UTCTime IntMap.Key -> EitherT (m ()) m IntMap.Key
nearestChangeSet t dateMapper = case Map.lookupGE t dateMapper of
  Nothing -> EitherT $ return $ Left $
    clientError "Specified timestamp is later than the latest known changes"
      [ "latest" .= fst (Map.findMax dateMapper) ]
  Just (_, csId) -> return csId


--------------------------------------------------------------------------------
redirectToLatest :: MonadSnap m => IntMap.IntMap ChangeSet -> m ()
redirectToLatest changeSets = redirectTo (fst $ IntMap.findMax changeSets)


--------------------------------------------------------------------------------
redirectTo :: MonadSnap m => Int -> m ()
redirectTo csId = redirect $ Encoding.encodeUtf8 $ Text.pack $
  "http://changed-mbids.musicbrainz.org/pub/musicbrainz/data/changed-mbids/changed-ids-" ++
    (show csId) ++ ".json.gz"


--------------------------------------------------------------------------------
withFilteredChangeSet :: MonadSnap m => ChangeSet -> Int -> (ChangeSet -> m ()) -> EitherT (m ()) m ()
withFilteredChangeSet changeSet maximumRequestSize a = do
  filters <- EitherT $ note noParse . decode' <$> readRequestBody 10485760

  if totalSize filters > maximumRequestSize
    then left $ clientError "Request too large" [ "max-size" .= maximumRequestSize]
    else lift $ a (intersect changeSet filters)

  where
    noParse = clientError "Unable to parse request body" []


--------------------------------------------------------------------------------
writeJSON :: (MonadSnap m, ToJSON a) => a -> m ()
writeJSON = writeLBS . encode


--------------------------------------------------------------------------------
parseTimeParameter :: MonadSnap m => EitherT (m ()) m UTCTime
parseTimeParameter = EitherT $
  note (clientError "Timestamp could not be parsed" []) . join . fmap parser
    <$> getParam "timeStamp"
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
writeError :: MonadSnap m => Int -> [Aeson.Pair] -> m ()
writeError code e = do
  writeLBS $ encode $ object e
  modifyResponse (setResponseCode code)

--------------------------------------------------------------------------------
assertNotEmpty :: MonadSnap m => IntMap.IntMap a -> EitherT (m ()) m ()
assertNotEmpty x =
  if IntMap.null x
    then EitherT $ return (Left $ serverError "The server has no change sets")
    else return ()
