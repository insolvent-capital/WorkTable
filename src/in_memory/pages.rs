use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};

use derive_more::{Display, Error, From};
use lockfree::stack::Stack;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::ser::serializers::AllocSerializer;
use performance_measurement_codegen::performance_measurement;
use crate::in_memory::page;
use crate::in_memory::page::{DATA_INNER_LENGTH, DataExecutionError, Link};
use crate::in_memory::row::GeneralRow;

pub struct DataPages<Row, const DATA_LENGTH: usize = DATA_INNER_LENGTH> {
    /// Pages vector. Currently, not lock free.
    pages: RwLock<Vec<Arc<page::Data<GeneralRow<Row>, DATA_LENGTH>>>>,

    /// Stack with empty [`Link`]s. It stores [`Link`]s of rows that was deleted.
    empty_links: Stack<Link>,

    /// Hint of `Row` size. Available for sized `Row`s.
    row_size_hint: AtomicI32,

    /// Count of saved rows.
    row_count: AtomicU64,

    last_page_id: AtomicU32,

    current_page: AtomicU32,
}

impl<Row, const DATA_LENGTH: usize> DataPages<Row, DATA_LENGTH> {
    pub fn new() -> Self {
        Self {
            pages: RwLock::new(vec![Arc::new(page::Data::new(0.into()))]),
            empty_links: Stack::new(),
            row_size_hint: AtomicI32::new(-1),
            row_count: AtomicU64::new(0),
            last_page_id: AtomicU32::new(0),
            current_page: AtomicU32::new(0)
        }
    }

    //#[performance_measurement(prefix_name = "DataPages")]
    pub fn insert<const N: usize>(&self, row: Row) -> Result<Link, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
    {
        if let Some(link) = self.empty_links.pop() {
            todo!()
        }

        let general_row = GeneralRow::from_inner(row);

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
            },
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

    fn retry_insert<const N: usize>(&self, general_row: GeneralRow<Row>) -> Result<Link, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
    {
        let pages = self.pages.read().unwrap();
        let current_page = self.current_page.load(Ordering::Relaxed);
        let page = &pages[current_page as usize];

        let res = page.save_row::<N>(&general_row).map_err(ExecutionError::DataPageError);
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

    pub fn select(&self, link: Link) -> Result<Row, ExecutionError>
    where Row: Archive,
          <GeneralRow<Row> as Archive>::Archived: Deserialize<GeneralRow<Row>, rkyv::Infallible>,
    {
        let pages = self.pages.read().unwrap();
        let page = pages.get::<usize>(link.page_id.into()).ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = page.get_row(link).map_err(ExecutionError::DataPageError)?;
        Ok(gen_row.inner)
    }

    pub unsafe fn update<const N: usize>(&self, row: Row, link: Link) -> Result<Link, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
    {
        let pages = self.pages.read().unwrap();
        let page = pages.get::<usize>(link.page_id.into()).ok_or(ExecutionError::PageNotFound(link.page_id))?;
        let gen_row = GeneralRow::from_inner(row);
        page.save_row_by_link(&gen_row, link).map_err(ExecutionError::DataPageError)
    }
}

#[derive(Debug, Display, From)]
pub enum ExecutionError {
    DataPageError(DataExecutionError),
    PageNotFound(page::Id)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, RwLock};
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Instant;

    use rkyv::{Archive, Deserialize, Serialize};
    use worktable_codegen::WorktableRow;
    use crate::in_memory::pages::DataPages;

    #[derive(
        Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    #[archive(compare(PartialEq))]
    #[archive_attr(derive(Debug))]
    struct TestRow {
        a: u64,
        b: u64,
    }

    #[test]
    fn insert() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow {
            a: 10,
            b: 20,
        };
        let link = pages.insert::<24>(row).unwrap();

        assert_eq!(link.page_id, 0.into());
        assert_eq!(link.length, 24);
        assert_eq!(link.offset, 0);

        assert_eq!(pages.row_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn select() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow {
            a: 10,
            b: 20,
        };
        let link = pages.insert::<24>(row).unwrap();
        let res = pages.select(link).unwrap();

        assert_eq!(res, row)
    }

    #[test]
    fn update() {
        let pages = DataPages::<TestRow>::new();

        let row = TestRow {
            a: 10,
            b: 20,
        };
        let link = pages.insert::<24>(row).unwrap();
        let res = pages.select(link).unwrap();

        assert_eq!(res, row)
    }

    #[test]
    fn insert_full() {
        let pages = DataPages::<TestRow, 24>::new();

        let row = TestRow {
            a: 10,
            b: 20,
        };
        let link = pages.insert::<16>(row).unwrap();
        let res = pages.insert::<24>(row);

        println!("{:?}", res)
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
                    let row = TestRow {
                        a: i,
                        b: j * i + 1,
                    };

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
                    let row = TestRow {
                        a: i,
                        b: j * i + 1,
                    };

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
                    let row = TestRow {
                        a: i,
                        b: j * i + 1,
                    };

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
