use derive_more::{Display, From};
use rkyv::{Archive, Deserialize, Serialize};

/// Represents space's identifier.
#[derive(
    Archive,
    Copy,
    Clone,
    Deserialize,
    Debug,
    Display,
    Eq,
    From,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub struct Id(u32);
