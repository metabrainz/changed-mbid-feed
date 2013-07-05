import qualified Data.Set as Set

data Vertex = Area | Artist | Label | Recording | Release | ReleaseGroup | Url | Work
  deriving (Bounded, Enum, Eq, Ord, Show)

data Edge
  = ArtistIs
  | ArtistArtistRelationship
  | ArtistLabelRelationship
  | ArtistRecordingRelationship
  | ArtistReleaseRelationship
  | ArtistReleaseGroupRelationship
  | ArtistUrlRelationship
  | ArtistWorkRelationship

  | LabelLabelRelationship
  | LabelRecordingRelationship
  | LabelReleaseRelationship
  | LabelReleaseGroupRelationship
  | LabelUrlRelationship
  | LabelWorkRelationship
  | LabelIs

  | ReleaseIs
  | ReleaseArtist
  | ReleaseLabel
  | ReleaseReleaseGroup

  | ReleaseGroupIs
  deriving (Bounded, Enum, Eq, Ord, Show)

propagate :: Edge -> [Edge]
propagate edge = case edge of
  ReleaseIs -> [ReleaseArtist, ReleaseLabel, ReleaseReleaseGroup]
  _ -> []

visualise :: [Edge] -> String
visualise edges =
  let touched = edges
      untouched = Set.toList $ Set.fromList [minBound..maxBound] `Set.difference` Set.fromList edges
  in unlines $
      [ prelude ] ++
      map (statement . vertex) [minBound..maxBound] ++
      map (toGraphViz "solid") touched ++
      map (toGraphViz "dashed") untouched ++
      [ "}" ]

  where

    prelude = "graph {"

toGraphViz :: String -> Edge -> String
toGraphViz style e = case e of
  ArtistIs -> edge style Artist Artist "is"
  ArtistArtistRelationship -> edge style Artist Artist "artist-artist relationship"
  ArtistLabelRelationship -> edge style Artist Label "artist-label relationship"
  ArtistRecordingRelationship -> edge style Artist Recording "artist-recording relationship"
  ArtistReleaseRelationship -> edge style Artist Release "artist-release relationship"
  ArtistReleaseGroupRelationship -> edge style Artist ReleaseGroup "artist-release group relationship"
  ArtistUrlRelationship -> edge style Artist Url "artist-url relationship"
  ArtistWorkRelationship -> edge style Artist Work "artist-work relationship"
  LabelLabelRelationship -> edge style Artist Label "label-label relationship"
  LabelRecordingRelationship -> edge style Artist Recording "label-recording relationship"
  LabelReleaseRelationship -> edge style Artist Release "label-release relationship"
  LabelReleaseGroupRelationship -> edge style Artist ReleaseGroup "label-release group relationship"
  LabelUrlRelationship -> edge style Artist Url "label-url relationship"
  LabelWorkRelationship -> edge style Artist Work "label-work relationship"
  LabelIs -> edge style Label Label "is"
  ReleaseArtist -> edge style Release Artist "release artist"
  ReleaseIs -> edge style Release Release "is"
  ReleaseLabel -> edge style Release Label "release label"
  ReleaseReleaseGroup -> edge style Release ReleaseGroup "release group"
  ReleaseGroupIs -> edge style ReleaseGroup ReleaseGroup "is"

type Label = String

statement :: String -> String
statement s = "  " ++ s ++ ";"

vertex :: Vertex -> String
vertex Area = "area"
vertex Artist = "artist"
vertex Label = "label"
vertex Recording = "recording"
vertex Release = "release"
vertex ReleaseGroup = "release_group"
vertex Url = "url"
vertex Work = "work"

edge :: String -> Vertex -> Vertex -> Label -> String
edge style a b label =
  statement $ unwords [ vertex a, "--", vertex b, "[label=\"" ++ label ++ "\", style=" ++ style ++ "]" ]

fix :: [Edge] -> [Edge]
fix edges = go (Set.fromList edges) edges
  where
    go processed []    = Set.toList processed
    go processed edges =
      let propagations = Set.fromList $ concatMap propagate edges
      in go (processed `Set.union` propagations)
            (Set.toList $ propagations `Set.difference` processed)

main = putStrLn $ visualise [ ReleaseIs ]
