use std::fmt::Debug;
use std::sync::atomic::AtomicBool;

use rkyv::with::{AtomicLoad, Relaxed};
use rkyv::{Archive, Deserialize, Serialize};

/// Common trait for the `Row`s that can be stored on the [`Data`] page.
///
/// [`Data`]: crate::in_memory::data::Data
pub trait StorableRow {
    type WrappedRow: Archive + Debug;
}

pub trait RowWrapper<Inner> {
    fn get_inner(self) -> Inner;

    fn from_inner(inner: Inner) -> Self;
}

/// General `Row` wrapper that is used to append general data for every `Inner`
/// `Row`.
#[derive(Archive, Deserialize, Debug, Serialize)]
pub struct GeneralRow<Inner> {
    /// Inner generic `Row`.
    pub inner: Inner,

    /// Indicator for deleted rows.
    #[rkyv(with = AtomicLoad<Relaxed>)]
    pub deleted: AtomicBool,
}

impl<Inner> RowWrapper<Inner> for GeneralRow<Inner> {
    fn get_inner(self) -> Inner {
        self.inner
    }

    /// Creates new [`GeneralRow`] from `Inner`.
    fn from_inner(inner: Inner) -> Self {
        Self {
            inner,
            deleted: AtomicBool::new(false),
        }
    }
}
