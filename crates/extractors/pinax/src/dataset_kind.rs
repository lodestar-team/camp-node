//! Pinax dataset kind: a Firehose→Parquet (S3) source materialised by camp-node.

/// The canonical string identifier for Pinax datasets.
const DATASET_KIND: &str = "pinax";

/// Zero-sized type identifying the "pinax" dataset kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PinaxDatasetKind;

impl PinaxDatasetKind {
    /// Canonical string identifier used in manifests/config.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        DATASET_KIND
    }
}

impl std::str::FromStr for PinaxDatasetKind {
    type Err = PinaxDatasetKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s != DATASET_KIND {
            return Err(PinaxDatasetKindError(s.to_string()));
        }
        Ok(PinaxDatasetKind)
    }
}

impl std::fmt::Display for PinaxDatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        DATASET_KIND.fmt(f)
    }
}

impl serde::Serialize for PinaxDatasetKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(DATASET_KIND)
    }
}

impl<'de> serde::Deserialize<'de> for PinaxDatasetKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl PartialEq<str> for PinaxDatasetKind {
    fn eq(&self, other: &str) -> bool {
        DATASET_KIND == other
    }
}

impl PartialEq<PinaxDatasetKind> for str {
    fn eq(&self, _other: &PinaxDatasetKind) -> bool {
        self == DATASET_KIND
    }
}

impl PartialEq<&str> for PinaxDatasetKind {
    fn eq(&self, other: &&str) -> bool {
        DATASET_KIND == *other
    }
}

impl PartialEq<PinaxDatasetKind> for &str {
    fn eq(&self, _other: &PinaxDatasetKind) -> bool {
        *self == DATASET_KIND
    }
}

impl PartialEq<String> for PinaxDatasetKind {
    fn eq(&self, other: &String) -> bool {
        DATASET_KIND == other.as_str()
    }
}

impl PartialEq<PinaxDatasetKind> for String {
    fn eq(&self, _other: &PinaxDatasetKind) -> bool {
        self.as_str() == DATASET_KIND
    }
}

/// Error returned when parsing an invalid Pinax dataset kind string.
#[derive(Debug, thiserror::Error)]
#[error("invalid dataset kind: {}, expected: {}", .0, DATASET_KIND)]
pub struct PinaxDatasetKindError(String);
