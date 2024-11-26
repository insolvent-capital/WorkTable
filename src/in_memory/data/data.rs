use crate::persistence::page::INNER_PAGE_SIZE;
use crate::prelude::Link;
use data_bucket::page::PageId;
use data_bucket::{DataPage, GeneralPage};
use derive_more::{Display, Error};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::{
    with::{Skip, Unsafe},
    AlignedBytes, Archive, Deserialize, Serialize,
};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};

#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;

/// Length of the [`Data`] page header.
pub const DATA_HEADER_LENGTH: usize = 4;

/// Length of the inner [`Data`] page part.
pub const DATA_INNER_LENGTH: usize = INNER_PAGE_SIZE - DATA_HEADER_LENGTH;

#[derive(Archive, Deserialize, Debug, Serialize)]
pub struct Data<Row, const DATA_LENGTH: usize = DATA_INNER_LENGTH> {
    /// [`Id`] of the [`General`] page of this [`Data`].
    ///
    /// [`Id]: PageId
    /// [`General`]: page::General
    #[with(Skip)]
    id: PageId,

    /// Offset to the first free byte on this [`Data`] page.
    pub free_offset: AtomicU32,

    /// Inner array of bytes where deserialized `Row`s will be stored.
    #[with(Unsafe)]
    inner_data: UnsafeCell<AlignedBytes<DATA_LENGTH>>,

    /// `Row` phantom data.
    _phantom: PhantomData<Row>,
}

unsafe impl<Row, const DATA_LENGTH: usize> Sync for Data<Row, DATA_LENGTH> {}

impl<Row, const DATA_LENGTH: usize> Data<Row, DATA_LENGTH> {
    /// Creates new [`Data`] page.
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            free_offset: AtomicU32::default(),
            inner_data: UnsafeCell::default(),
            _phantom: PhantomData,
        }
    }

    pub fn from_data_page(page: GeneralPage<DataPage<DATA_LENGTH>>) -> Self {
        Self {
            id: page.header.page_id,
            free_offset: AtomicU32::from(page.header.data_length),
            inner_data: UnsafeCell::new(AlignedBytes(page.inner.data)),
            _phantom: PhantomData,
        }
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataRow")
    )]
    pub fn save_row<const N: usize>(&self, row: &Row) -> Result<Link, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
    {
        let bytes = rkyv::to_bytes(row).map_err(|_| ExecutionError::SerializeError)?;
        let length = bytes.len() as u32;
        let offset = self.free_offset.fetch_add(length, Ordering::SeqCst);
        if offset > DATA_LENGTH as u32 - length {
            return Err(ExecutionError::PageIsFull {
                need: length,
                left: DATA_LENGTH as i64 - offset as i64,
            });
        }

        let inner_data = unsafe { &mut *self.inner_data.get() };
        inner_data[offset as usize..][..length as usize].copy_from_slice(bytes.as_slice());

        let link = Link {
            page_id: self.id,
            offset,
            length,
        };

        Ok(link)
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataRow")
    )]
    pub unsafe fn save_row_by_link<const N: usize>(
        &self,
        row: &Row,
        link: Link,
    ) -> Result<Link, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
    {
        let bytes = rkyv::to_bytes(row).map_err(|_| ExecutionError::SerializeError)?;
        let length = bytes.len() as u32;
        if length != link.length {
            return Err(ExecutionError::InvalidLink);
        }

        let inner_data = unsafe { &mut *self.inner_data.get() };
        inner_data[link.offset as usize..][..link.length as usize]
            .copy_from_slice(bytes.as_slice());

        Ok(link)
    }

    pub unsafe fn get_mut_row_ref(
        &self,
        link: Link,
    ) -> Result<Pin<&mut <Row as Archive>::Archived>, ExecutionError>
    where
        Row: Archive,
    {
        if link.offset > self.free_offset.load(Ordering::Relaxed) {
            return Err(ExecutionError::DeserializeError);
        }

        let inner_data = unsafe { &mut *self.inner_data.get() };
        let bytes = &mut inner_data[link.offset as usize..(link.offset + link.length) as usize];
        Ok(unsafe { rkyv::archived_root_mut::<Row>(Pin::new(&mut bytes[..])) })
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataRow")
    )]
    pub fn get_row_ref(&self, link: Link) -> Result<&<Row as Archive>::Archived, ExecutionError>
    where
        Row: Archive,
    {
        if link.offset > self.free_offset.load(Ordering::Relaxed) {
            return Err(ExecutionError::DeserializeError);
        }

        let inner_data = unsafe { &*self.inner_data.get() };
        let bytes = &inner_data[link.offset as usize..(link.offset + link.length) as usize];
        Ok(unsafe { rkyv::archived_root::<Row>(&bytes[..]) })
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "DataRow")
    )]
    pub fn get_row(&self, link: Link) -> Result<Row, ExecutionError>
    where
        Row: Archive,
        <Row as Archive>::Archived: Deserialize<Row, rkyv::de::deserializers::SharedDeserializeMap>,
    {
        let archived = self.get_row_ref(link)?;
        let mut map = rkyv::de::deserializers::SharedDeserializeMap::new();
        archived
            .deserialize(&mut map)
            .map_err(|_| ExecutionError::DeserializeError)
    }

    pub fn get_bytes(&self) -> [u8; DATA_LENGTH] {
        let data = unsafe { &*self.inner_data.get() };
        data.0.clone()
    }
}

/// Error that can appear on [`Data`] page operations.
#[derive(Copy, Clone, Debug, Display, Error)]
pub enum ExecutionError {
    /// Error of trying to save row in [`Data`] page with not enough space left.
    #[display("need {}, but {} left", need, left)]
    PageIsFull { need: u32, left: i64 },

    /// Error of saving `Row` in [`Data`] page.
    SerializeError,

    /// Error of loading `Row` from [`Data`] page.
    DeserializeError,

    /// Link provided for saving `Row` is invalid.
    InvalidLink,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::{mpsc, Arc};
    use std::thread;

    use rkyv::{Archive, Deserialize, Serialize};

    use crate::in_memory::data::data::{Data, INNER_PAGE_SIZE};

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
    fn data_page_length_valid() {
        let data = Data::<()>::new(1.into());
        let bytes = rkyv::to_bytes::<_, 4096>(&data).unwrap();

        assert_eq!(bytes.len(), INNER_PAGE_SIZE)
    }

    #[test]
    fn data_page_save_row() {
        let page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row::<16>(&row).unwrap();
        assert_eq!(link.page_id, page.id);
        assert_eq!(link.length, 16);
        assert_eq!(link.offset, 0);

        assert_eq!(page.free_offset.load(Ordering::Relaxed), link.length);

        let inner_data = unsafe { &mut *page.inner_data.get() };
        let bytes = &inner_data[link.offset as usize..link.length as usize];
        let archived = unsafe { rkyv::archived_root::<TestRow>(bytes) };
        assert_eq!(archived, &row)
    }

    #[test]
    fn data_page_overwrite_row() {
        let page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row::<16>(&row).unwrap();

        let new_row = TestRow { a: 20, b: 20 };
        let res = unsafe { page.save_row_by_link::<16>(&new_row, link) }.unwrap();

        assert_eq!(res, link);

        let inner_data = unsafe { &mut *page.inner_data.get() };
        let bytes = &inner_data[link.offset as usize..link.length as usize];
        let archived = unsafe { rkyv::archived_root::<TestRow>(bytes) };
        assert_eq!(archived, &new_row)
    }

    #[test]
    fn data_page_full() {
        let page = Data::<TestRow, 16>::new(1.into());
        let row = TestRow { a: 10, b: 20 };
        let _ = page.save_row::<16>(&row).unwrap();

        let new_row = TestRow { a: 20, b: 20 };
        let res = page.save_row::<16>(&new_row);

        assert!(res.is_err());
    }

    #[test]
    fn data_page_full_multithread() {
        let page = Data::<TestRow, 128>::new(1.into());
        let shared = Arc::new(page);

        let (tx, rx) = mpsc::channel();
        let second_shared = shared.clone();

        thread::spawn(move || {
            let mut links = Vec::new();
            for i in 1..10 {
                let row = TestRow {
                    a: 10 + i,
                    b: 20 + i,
                };

                let link = second_shared.save_row::<16>(&row);
                links.push(link)
            }

            tx.send(links).unwrap();
        });

        let mut links = Vec::new();
        for i in 1..10 {
            let row = TestRow {
                a: 30 + i,
                b: 40 + i,
            };

            let link = shared.save_row::<16>(&row);
            links.push(link)
        }
        let other_links = rx.recv().unwrap();

        print!("{:?}", other_links);
        print!("{:?}", links);
    }

    #[test]
    fn data_page_save_many_rows() {
        let page = Data::<TestRow>::new(1.into());

        let mut rows = Vec::new();
        let mut links = Vec::new();
        for i in 1..10 {
            let row = TestRow {
                a: 10 + i,
                b: 20 + i,
            };
            rows.push(row);

            let link = page.save_row::<16>(&row);
            links.push(link)
        }

        let inner_data = unsafe { &mut *page.inner_data.get() };

        for (i, link) in links.into_iter().enumerate() {
            let link = link.unwrap();

            let bytes = &inner_data[link.offset as usize..(link.offset + link.length) as usize];
            let archived = unsafe { rkyv::archived_root::<TestRow>(bytes) };
            let row = rows.get(i).unwrap();

            assert_eq!(row, archived)
        }
    }

    #[test]
    fn data_page_get_row_ref() {
        let page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row::<16>(&row).unwrap();
        let archived = page.get_row_ref(link).unwrap();
        assert_eq!(archived, &row)
    }

    #[test]
    fn data_page_get_row() {
        let page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row::<16>(&row).unwrap();
        let deserialized = page.get_row(link).unwrap();
        assert_eq!(deserialized, row)
    }

    #[test]
    fn multithread() {
        let page = Data::<TestRow>::new(1.into());
        let shared = Arc::new(page);

        let (tx, rx) = mpsc::channel();
        let second_shared = shared.clone();

        thread::spawn(move || {
            let mut links = Vec::new();
            for i in 1..10 {
                let row = TestRow {
                    a: 10 + i,
                    b: 20 + i,
                };

                let link = second_shared.save_row::<16>(&row);
                links.push(link)
            }

            tx.send(links).unwrap();
        });

        let mut links = Vec::new();
        for i in 1..10 {
            let row = TestRow {
                a: 30 + i,
                b: 40 + i,
            };

            let link = shared.save_row::<16>(&row);
            links.push(link)
        }
        let other_links = rx.recv().unwrap();

        let links = other_links
            .into_iter()
            .chain(links.into_iter())
            .map(|v| v.unwrap())
            .collect::<Vec<_>>();

        for link in links {
            let _ = shared.get_row(link).unwrap();
        }
    }
}
