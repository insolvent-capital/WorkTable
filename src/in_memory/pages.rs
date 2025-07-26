use std::{
    fmt::Debug,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
    sync::{Arc, RwLock},
};

use data_bucket::page::PageId;
use derive_more::{Display, Error, From};
use lockfree::stack::Stack;
#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;
use rkyv::{
    api::high::HighDeserializer,
    rancor::Strategy,
    ser::{allocator::ArenaHandle, sharing::Share, Serializer},
    util::AlignedVec,
    Archive, Deserialize, Portable, Serialize,
};

use crate::{
    in_memory::{
        row::{RowWrapper, StorableRow},
        Data, DataExecutionError, DATA_INNER_LENGTH,
    },
    prelude::Link,
};

fn page_id_mapper(page_id: usize) -> usize {
    page_id - 1usize
}

#[derive(Debug)]
pub struct DataPages<Row, const DATA_LENGTH: usize = DATA_INNER_LENGTH>
where
    Row: StorableRow,
{
    /// Pages vector. Currently, not lock free.
    pages: RwLock<Vec<Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>>>,

    /// Stack with empty [`Link`]s. It stores [`Link`]s of rows that was deleted.
    empty_links: Stack<Link>,

    /// Count of saved rows.
    row_count: AtomicU64,

    last_page_id: AtomicU32,

    current_page_id: AtomicU32,
}

impl<Row, const DATA_LENGTH: usize> Default for DataPages<Row, DATA_LENGTH>
where
    Row: StorableRow,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Row, const DATA_LENGTH: usize> DataPages<Row, DATA_LENGTH>
where
    Row: StorableRow,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    pub fn new() -> Self {
        Self {
            // We are starting ID's from `1` because `0`'s page in file is info page.
            pages: RwLock::new(vec![Arc::new(Data::new(1.into()))]),
            empty_links: Stack::new(),
            row_count: AtomicU64::new(0),
            last_page_id: AtomicU32::new(1),
            current_page_id: AtomicU32::new(1),
        }
    }

    pub fn from_data(vec: Vec<Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>>) -> Self {
        // TODO: Add row_count persistence.
        if vec.is_empty() {
            Self::new()
        } else {
            let last_page_id = vec.len();
            Self {
                pages: RwLock::new(vec),
                empty_links: Stack::new(),
                row_count: AtomicU64::new(0),
                last_page_id: AtomicU32::new(last_page_id as u32),
                current_page_id: AtomicU32::new(last_page_id as u32),
            }
        }
    }

    pub fn insert(&self, row: Row) -> Result<Link, ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <Row as StorableRow>::WrappedRow: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
    {
        let general_row = <Row as StorableRow>::WrappedRow::from_inner(row);

        if let Some(link) = self.empty_links.pop() {
            let pages = self.pages.read().unwrap();
            let current_page: usize = page_id_mapper(link.page_id.into());
            let page = &pages[current_page];

            if let Err(e) = unsafe { page.save_row_by_link(&general_row, link) } {
                match e {
                    DataExecutionError::InvalidLink => {
                        self.empty_links.push(link);
                    }
                    DataExecutionError::PageIsFull { .. }
                    | DataExecutionError::SerializeError
                    | DataExecutionError::DeserializeError => return Err(e.into()),
                }
            } else {
                return Ok(link);
            };
        }

        loop {
            let (link, tried_page) = {
                let pages = self.pages.read().unwrap();
                let current_page =
                    page_id_mapper(self.current_page_id.load(Ordering::Acquire) as usize);
                let page = &pages[current_page];

                (page.save_row(&general_row), current_page)
            };
            match link {
                Ok(link) => {
                    self.row_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(link);
                }
                Err(e) => match e {
                    DataExecutionError::PageIsFull { .. } => {
                        if tried_page
                            == page_id_mapper(self.current_page_id.load(Ordering::Relaxed) as usize)
                        {
                            self.add_next_page(tried_page);
                        }
                    }
                    DataExecutionError::SerializeError
                    | DataExecutionError::DeserializeError
                    | DataExecutionError::InvalidLink => return Err(e.into()),
                },
            };
        }
    }

    pub fn insert_cdc(&self, row: Row) -> Result<(Link, Vec<u8>), ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            > + Clone,
        <Row as StorableRow>::WrappedRow: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
    {
        let link = self.insert(row.clone())?;
        let general_row = <Row as StorableRow>::WrappedRow::from_inner(row);
        let bytes = rkyv::to_bytes(&general_row)
            .expect("should be ok as insert not failed")
            .into_vec();
        Ok((link, bytes))
    }

    fn add_next_page(&self, tried_page: usize) {
        let mut pages = self.pages.write().expect("lock should be not poisoned");
        if tried_page == page_id_mapper(self.current_page_id.load(Ordering::Acquire) as usize) {
            let index = self.last_page_id.fetch_add(1, Ordering::AcqRel) + 1;

            pages.push(Arc::new(Data::new(index.into())));
            self.current_page_id.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataPages")
    )]
    pub fn select(&self, link: Link) -> Result<Row, ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Portable
            + Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let pages = self.pages.read().unwrap();
        let page = pages
            // - 1 is used because page ids are starting from 1.
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = page.get_row(link).map_err(ExecutionError::DataPageError)?;
        Ok(gen_row.get_inner())
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataPages")
    )]
    pub fn with_ref<Op, Res>(&self, link: Link, op: Op) -> Result<Res, ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        Op: Fn(&<<Row as StorableRow>::WrappedRow as Archive>::Archived) -> Res,
    {
        let pages = self.pages.read().unwrap();
        let page = pages
            .get::<usize>(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = page
            .get_row_ref(link)
            .map_err(ExecutionError::DataPageError)?;
        let res = op(gen_row);
        Ok(res)
    }

    #[allow(clippy::missing_safety_doc)]
    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataPages")
    )]
    pub unsafe fn with_mut_ref<Op, Res>(
        &self,
        link: Link,
        mut op: Op,
    ) -> Result<Res, ExecutionError>
    where
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Portable,
        Op: FnMut(&mut <<Row as StorableRow>::WrappedRow as Archive>::Archived) -> Res,
    {
        let pages = self.pages.read().unwrap();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = unsafe {
            page.get_mut_row_ref(link)
                .map_err(ExecutionError::DataPageError)?
                .unseal_unchecked()
        };
        let res = op(gen_row);
        Ok(res)
    }

    /// # Safety
    /// This function is `unsafe` because it modifies archived memory directly.
    /// The caller must ensure that:
    /// - The `link` is valid and points to a properly initialized row.
    /// - No other references to the same row exist during modification.
    /// - The operation does not cause data races or memory corruption.
    pub unsafe fn update<const N: usize>(
        &self,
        row: Row,
        link: Link,
    ) -> Result<Link, ExecutionError>
    where
        Row: Archive,
        <Row as StorableRow>::WrappedRow: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
    {
        let pages = self.pages.read().unwrap();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = <Row as StorableRow>::WrappedRow::from_inner(row);
        unsafe {
            page.save_row_by_link(&gen_row, link)
                .map_err(ExecutionError::DataPageError)
        }
    }

    pub fn delete(&self, link: Link) -> Result<(), ExecutionError> {
        self.empty_links.push(link);
        Ok(())
    }

    pub fn select_raw(&self, link: Link) -> Result<Vec<u8>, ExecutionError> {
        let pages = self.pages.read().unwrap();
        let page = pages
            .get(page_id_mapper(link.page_id.into()))
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        page.get_raw_row(link)
            .map_err(ExecutionError::DataPageError)
    }

    pub fn get_bytes(&self) -> Vec<([u8; DATA_LENGTH], u32)> {
        let pages = self.pages.read().unwrap();
        pages
            .iter()
            .map(|p| (p.get_bytes(), p.free_offset.load(Ordering::Relaxed)))
            .collect()
    }

    pub fn get_page_count(&self) -> usize {
        self.pages.read().unwrap().len()
    }

    pub fn get_empty_links(&self) -> Vec<Link> {
        let mut res = vec![];
        for l in self.empty_links.pop_iter() {
            res.push(l)
        }

        res
    }

    pub fn with_empty_links(mut self, links: Vec<Link>) -> Self {
        let stack = Stack::new();
        for l in links {
            stack.push(l)
        }
        self.empty_links = stack;

        self
    }
}

#[derive(Debug, Display, Error, From)]
pub enum ExecutionError {
    DataPageError(DataExecutionError),

    PageNotFound(#[error(not(source))] PageId),

    Locked,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::Instant;

    use crate::in_memory::pages::DataPages;
    use crate::in_memory::row::GeneralRow;
    use crate::in_memory::StorableRow;
    use rkyv::{Archive, Deserialize, Serialize};

    #[derive(
        Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct TestRow {
        a: u64,
        b: u64,
    }

    impl StorableRow for TestRow {
        type WrappedRow = GeneralRow<TestRow>;
    }

    #[test]
    fn insert() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();

        assert_eq!(link.page_id, 1.into());
        assert_eq!(link.length, 24);
        assert_eq!(link.offset, 0);

        assert_eq!(pages.row_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn insert_many() {
        let pages = DataPages::<TestRow>::new();

        for _ in 0..10_000 {
            let row = TestRow { a: 10, b: 20 };
            pages.insert(row).unwrap();
        }

        assert_eq!(pages.row_count.load(Ordering::Relaxed), 10_000);
        assert!(pages.current_page_id.load(Ordering::Relaxed) > 2);
    }

    #[test]
    fn select() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();
        let res = pages.select(link).unwrap();

        assert_eq!(res, row)
    }

    #[test]
    fn update() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();
        let res = pages.select(link).unwrap();

        assert_eq!(res, row)
    }

    #[test]
    fn delete() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();
        pages.delete(link).unwrap();

        assert_eq!(pages.empty_links.pop(), Some(link));
        pages.empty_links.push(link);

        let row = TestRow { a: 20, b: 20 };
        let new_link = pages.insert(row).unwrap();
        assert_eq!(new_link, link)
    }

    #[test]
    fn insert_on_empty() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert(row).unwrap();
        let _ = pages.delete(link);
        let link_new = pages.insert(row).unwrap();

        assert_eq!(link, link_new);
        assert_eq!(pages.select(link).unwrap(), TestRow { a: 10, b: 20 })
    }

    //#[test]
    fn _bench() {
        let pages = Arc::new(DataPages::<TestRow>::new());

        let mut v = Vec::new();

        let now = Instant::now();

        for j in 0..10 {
            let pages_shared = pages.clone();
            let h = thread::spawn(move || {
                for i in 0..1000 {
                    let row = TestRow { a: i, b: j * i + 1 };

                    pages_shared.insert(row).unwrap();
                }
            });

            v.push(h)
        }

        for h in v {
            h.join().unwrap()
        }

        let elapsed = now.elapsed();

        println!("wt2 {elapsed:?}")
    }

    #[test]
    fn bench_set() {
        let pages = Arc::new(RwLock::new(HashSet::new()));

        let mut v = Vec::new();

        let now = Instant::now();

        for j in 0..10 {
            let pages_shared = pages.clone();
            let h = thread::spawn(move || {
                for i in 0..1000 {
                    let row = TestRow { a: i, b: j * i + 1 };

                    let mut pages = pages_shared.write().unwrap();
                    pages.insert(row);
                }
            });

            v.push(h)
        }

        for h in v {
            h.join().unwrap()
        }

        let elapsed = now.elapsed();

        println!("set {elapsed:?}")
    }

    #[test]
    fn bench_vec() {
        let pages = Arc::new(RwLock::new(Vec::new()));

        let mut v = Vec::new();

        let now = Instant::now();

        for j in 0..10 {
            let pages_shared = pages.clone();
            let h = thread::spawn(move || {
                for i in 0..1000 {
                    let row = TestRow { a: i, b: j * i + 1 };

                    let mut pages = pages_shared.write().unwrap();
                    pages.push(row);
                }
            });

            v.push(h)
        }

        for h in v {
            h.join().unwrap()
        }

        let elapsed = now.elapsed();

        println!("vec {elapsed:?}")
    }
}
