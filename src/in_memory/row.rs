use std::sync::atomic::AtomicBool;

use rkyv::{Archive, Deserialize, Serialize};

/// General `Row` wrapper that is used to append general data for every `Inner`
/// `Row`.
#[derive(Archive, Deserialize, Debug, Serialize)]
pub struct GeneralRow<Inner> {
    /// Inner generic `Row`.
    pub inner: Inner,

    /// Indicator for deleted rows.
    pub deleted: AtomicBool,
}

impl<Inner> GeneralRow<Inner> {
    /// Creates new [`GeneralRow`] from `Inner`.
    pub fn from_inner(inner: Inner) -> Self {
        Self {
            inner,
            deleted: AtomicBool::new(false)
        }
    }
}


/// Common trait for the `Row`s that can be stored on the [`Data`] page.
///
/// [`Data`]: crate::in_memory::page::Data
pub trait StorableRow {
    /// Indicator if `Row` is sized.
    fn is_sized() -> bool;
}