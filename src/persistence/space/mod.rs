mod data;
mod index;

use std::fs::{File, OpenOptions};
use std::path::Path;

use data_bucket::{GeneralPage, Link, SpaceInfoPage};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;

pub use data::SpaceData;
pub use index::{map_index_pages_to_toc_and_general, IndexTableOfContents, SpaceIndex};

pub trait SpaceDataOps<PkGenState> {
    fn from_table_files_path<S: AsRef<str>>(path: S) -> eyre::Result<Self>
    where
        Self: Sized;
    fn bootstrap(file: &mut File, table_name: String) -> eyre::Result<()>;
    fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()>;
    fn get_mut_info(&mut self) -> &mut GeneralPage<SpaceInfoPage<PkGenState>>;
    fn save_info(&mut self) -> eyre::Result<()>;
}

pub trait SpaceIndexOps<T>
where
    T: Ord,
{
    fn primary_from_table_files_path<S: AsRef<str>>(path: S) -> eyre::Result<Self>
    where
        Self: Sized;
    fn secondary_from_table_files_path<S1: AsRef<str>, S2: AsRef<str>>(
        path: S1,
        name: S2,
    ) -> eyre::Result<Self>
    where
        Self: Sized;
    fn bootstrap(file: &mut File, table_name: String) -> eyre::Result<()>;
    fn process_change_event(&mut self, event: ChangeEvent<Pair<T, Link>>) -> eyre::Result<()>;
}

pub trait SpaceSecondaryIndexOps<SecondaryIndexEvents> {
    fn from_table_files_path<S: AsRef<str>>(path: S) -> eyre::Result<Self>
    where
        Self: Sized;
    fn process_change_events(&mut self, events: SecondaryIndexEvents) -> eyre::Result<()>;
}

pub fn open_or_create_file<S: AsRef<str>>(path: S) -> eyre::Result<File> {
    let path = Path::new(path.as_ref());
    Ok(OpenOptions::new()
        .write(true)
        .read(true)
        .create(!path.exists())
        .open(path)?)
}
