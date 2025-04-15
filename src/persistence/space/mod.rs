mod data;
mod index;

use std::future::Future;
use std::path::Path;

use data_bucket::{GeneralPage, Link, SpaceInfoPage};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use tokio::fs::{File, OpenOptions};

pub use data::SpaceData;
pub use index::{
    map_index_pages_to_toc_and_general, map_unsized_index_pages_to_toc_and_general,
    IndexTableOfContents, SpaceIndex, SpaceIndexUnsized,
};

pub trait SpaceDataOps<PkGenState> {
    fn from_table_files_path<S: AsRef<str> + Send>(
        path: S,
    ) -> impl Future<Output = eyre::Result<Self>> + Send
    where
        Self: Sized;
    fn bootstrap(
        file: &mut File,
        table_name: String,
    ) -> impl Future<Output = eyre::Result<()>> + Send;
    fn save_data(
        &mut self,
        link: Link,
        bytes: &[u8],
    ) -> impl Future<Output = eyre::Result<()>> + Send;
    fn get_mut_info(&mut self) -> &mut GeneralPage<SpaceInfoPage<PkGenState>>;
    fn save_info(&mut self) -> impl Future<Output = eyre::Result<()>> + Send;
}

pub trait SpaceIndexOps<T>
where
    T: Ord,
{
    fn primary_from_table_files_path<S: AsRef<str> + Send>(
        path: S,
    ) -> impl Future<Output = eyre::Result<Self>> + Send
    where
        Self: Sized;
    fn secondary_from_table_files_path<S1: AsRef<str> + Send, S2: AsRef<str> + Send>(
        path: S1,
        name: S2,
    ) -> impl Future<Output = eyre::Result<Self>> + Send
    where
        Self: Sized;
    fn bootstrap(
        file: &mut File,
        table_name: String,
    ) -> impl Future<Output = eyre::Result<()>> + Send;
    fn process_change_event(
        &mut self,
        event: ChangeEvent<Pair<T, Link>>,
    ) -> impl Future<Output = eyre::Result<()>> + Send;
}

pub trait SpaceSecondaryIndexOps<SecondaryIndexEvents> {
    fn from_table_files_path<S: AsRef<str> + Send>(
        path: S,
    ) -> impl Future<Output = eyre::Result<Self>> + Send
    where
        Self: Sized;
    fn process_change_events(
        &mut self,
        events: SecondaryIndexEvents,
    ) -> impl Future<Output = eyre::Result<()>> + Send;
}

pub async fn open_or_create_file<S: AsRef<str>>(path: S) -> eyre::Result<File> {
    let path = Path::new(path.as_ref());
    Ok(OpenOptions::new()
        .write(true)
        .read(true)
        .create(!path.exists())
        .open(path)
        .await?)
}
