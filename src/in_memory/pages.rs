use std::fmt::Debug;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use derive_more::{Display, Error, From};
use lockfree::stack::Stack;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::{Archive, Deserialize, Serialize};

use crate::in_memory::page;
use crate::in_memory::page::{DataExecutionError, Link, DATA_INNER_LENGTH};
use crate::in_memory::row::{GeneralRow, RowWrapper, StorableRow};
#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;
use crate::prelude::ArchivedRow;

#[derive(Debug)]
pub struct DataPages<Row, const DATA_LENGTH: usize = DATA_INNER_LENGTH>
where
    Row: StorableRow,
{
    /// Pages vector. Currently, not lock free.
    pages: RwLock<Vec<Arc<page::Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>>>,

    /// Stack with empty [`Link`]s. It stores [`Link`]s of rows that was deleted.
    empty_links: Stack<Link>,

    /// Hint of `Row` size. Available for sized `Row`s.
    row_size_hint: AtomicI32,

    /// Count of saved rows.
    row_count: AtomicU64,

    last_page_id: AtomicU32,

    current_page: AtomicU32,
}

impl<Row, const DATA_LENGTH: usize> DataPages<Row, DATA_LENGTH>
where
    Row: StorableRow,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    pub fn new() -> Self {
        Self {
            pages: RwLock::new(vec![Arc::new(page::Data::new(0.into()))]),
            empty_links: Stack::new(),
            row_size_hint: AtomicI32::new(-1),
            row_count: AtomicU64::new(0),
            last_page_id: AtomicU32::new(0),
            current_page: AtomicU32::new(0),
        }
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataPages")
    )]
    pub fn insert<const N: usize>(&self, row: Row) -> Result<Link, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
        <Row as StorableRow>::WrappedRow: Archive + Serialize<AllocSerializer<N>>,
    {
        if let Some(link) = self.empty_links.pop() {
            todo!()
        }

        let general_row = <Row as StorableRow>::WrappedRow::from_inner(row);

        let (link, tried_page) = {
            let pages = self.pages.read().unwrap();
            let current_page = self.current_page.load(Ordering::Relaxed);
            let page = &pages[current_page as usize];

            (page.save_row::<N>(&general_row), current_page)
        };
        let res = match link {
            Ok(link) => {
                self.row_count.fetch_add(1, Ordering::Relaxed);
                link
            }
            Err(e) => {
                return if let DataExecutionError::PageIsFull { .. } = e {
                    if tried_page == self.current_page.load(Ordering::Relaxed) {
                        self.add_next_page(tried_page);
                    }
                    self.retry_insert(general_row)
                } else {
                    Err(e.into())
                }
            }
        };

        Ok(res)
    }

    fn retry_insert<const N: usize>(
        &self,
        general_row: <Row as StorableRow>::WrappedRow,
    ) -> Result<Link, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
        <Row as StorableRow>::WrappedRow: Archive + Serialize<AllocSerializer<N>>,
    {
        let pages = self.pages.read().unwrap();
        let current_page = self.current_page.load(Ordering::Relaxed);
        let page = &pages[current_page as usize];

        let res = page
            .save_row::<N>(&general_row)
            .map_err(ExecutionError::DataPageError);
        if let Ok(link) = res {
            self.row_count.fetch_add(1, Ordering::Relaxed);
            Ok(link)
        } else {
            res
        }
    }

    fn add_next_page(&self, tried_page: u32) {
        let mut pages = self.pages.write().unwrap();
        if tried_page == self.current_page.load(Ordering::Relaxed) {
            let index = self.last_page_id.fetch_add(1, Ordering::Relaxed) + 1;

            pages.push(Arc::new(page::Data::new(index.into())));
            self.current_page.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataPages")
    )]
    pub fn select(&self, link: Link) -> Result<Row, ExecutionError>
    where
        Row: Archive,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Deserialize<
            <Row as StorableRow>::WrappedRow,
            rkyv::de::deserializers::SharedDeserializeMap,
        >,
    {
        let pages = self.pages.read().unwrap();
        let page = pages
            .get::<usize>(link.page_id.into())
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
        Row: Archive,
        Op: Fn(&<<Row as StorableRow>::WrappedRow as Archive>::Archived) -> Res
    {
        let pages = self.pages.read().unwrap();
        let page = pages
            .get::<usize>(link.page_id.into())
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = page.get_row_ref(link).map_err(ExecutionError::DataPageError)?;
        let res = op(gen_row);
        Ok(res)
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataPages")
    )]
    pub unsafe fn with_mut_ref<Op, Res>(&self, link: Link, mut op: Op) -> Result<Res, ExecutionError>
    where
        Row: Archive,
        Op: FnMut(&mut <<Row as StorableRow>::WrappedRow as Archive>::Archived) -> Res
    {
        let pages = self.pages.read().unwrap();
        let page = pages
            .get::<usize>(link.page_id.into())
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = page.get_mut_row_ref(link)
            .map_err(ExecutionError::DataPageError)?
            .get_unchecked_mut();
        let res = op(gen_row);
        Ok(res)
    }

    pub unsafe fn update<const N: usize>(
        &self,
        row: Row,
        link: Link,
    ) -> Result<Link, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
        <Row as StorableRow>::WrappedRow: Archive + Serialize<AllocSerializer<N>>,
    {
        let pages = self.pages.read().unwrap();
        let page = pages
            .get::<usize>(link.page_id.into())
            .ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = <Row as StorableRow>::WrappedRow::from_inner(row);
        page.save_row_by_link(&gen_row, link)
            .map_err(ExecutionError::DataPageError)
    }
}

#[derive(Debug, Display, Error, From)]
pub enum ExecutionError {
    DataPageError(DataExecutionError),

    PageNotFound(#[error(not(source))] page::Id),

    Locked
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
    #[archive(compare(PartialEq))]
    #[archive_attr(derive(Debug))]
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
        let link = pages.insert::<24>(row).unwrap();

        assert_eq!(link.page_id, 0.into());
        assert_eq!(link.length, 24);
        assert_eq!(link.offset, 0);

        assert_eq!(pages.row_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn select() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert::<24>(row).unwrap();
        let res = pages.select(link).unwrap();

        assert_eq!(res, row)
    }

    #[test]
    fn update() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert::<24>(row).unwrap();
        let res = pages.select(link).unwrap();

        assert_eq!(res, row)
    }

    #[test]
    fn insert_full() {
        let pages = DataPages::<TestRow, 24>::new();

        let row = TestRow { a: 10, b: 20 };
        let link = pages.insert::<16>(row).unwrap();
        let res = pages.insert::<24>(row);

        assert!(res.is_ok())
    }

    #[test]
    fn bench() {
        let pages = Arc::new(DataPages::<TestRow>::new());

        let mut v = Vec::new();

        let now = Instant::now();

        for j in 0..10 {
            let pages_shared = pages.clone();
            let h = thread::spawn(move || {
                for i in 0..1000 {
                    let row = TestRow { a: i, b: j * i + 1 };

                    pages_shared.insert::<24>(row);
                }
            });

            v.push(h)
        }

        for h in v {
            h.join().unwrap()
        }

        let elapsed = now.elapsed();

        println!("wt2 {:?}", elapsed)
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

        println!("set {:?}", elapsed)
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

        println!("vec {:?}", elapsed)
    }
}
