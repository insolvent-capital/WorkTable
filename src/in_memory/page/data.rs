use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicI32, AtomicU16, AtomicU32, Ordering};

use derive_more::Display;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::{with::{Skip, Unsafe}, Archive, Deserialize, Serialize, CheckBytes, Fallible, AlignedBytes};
use rkyv::validation::validators::DefaultValidator;
use smart_default::SmartDefault;
use performance_measurement_codegen::performance_measurement;
use crate::in_memory::page::{self, INNER_PAGE_LENGTH};

/// Length of the [`Data`] page header.
pub const DATA_HEADER_LENGTH: usize = 4;

/// Length of the [`Hint`].
pub const HINT_LENGTH: usize = 8;

/// Length of the inner [`Data`] page part.
pub const DATA_INNER_LENGTH: usize = INNER_PAGE_LENGTH - DATA_HEADER_LENGTH - HINT_LENGTH;

/// Hint can be used to save row size, it `Row` is sized. It can predict how much `Row`s can be saved on page.
/// Also, it counts saved rows.
#[derive(Archive, Deserialize, Debug, Serialize, SmartDefault)]
pub struct Hint {
    #[default(_code = "AtomicI32::new(-1)")]
    row_size: AtomicI32,
    capacity: AtomicU16,
    row_length: AtomicU16,
}

impl Hint {
    pub fn from_row_size(size: usize) -> Self {
        let capacity = DATA_INNER_LENGTH / size;
        Self {
            row_size: AtomicI32::new(-1),
            capacity: AtomicU16::new(capacity as u16),
            row_length: AtomicU16::default(),
        }
    }
}

#[derive(Archive, Deserialize, Debug, Serialize)]
pub struct Data<Row> {
    /// [`Id`] of the [`General`] page of this [`Data`].
    ///
    /// [`Id]: page::Id
    /// [`General`]: page::General
    #[with(Skip)]
    id: page::Id,

    /// Offset to the first free byte on this [`Data`] page.
    free_offset: AtomicU32,

    /// Optional size of the `Row` which stored on this [`Data`] page. If row
    /// is unsized (contains [`String`] etc.) row size will be `None`.
    hint: Hint ,

    /// Inner array of bytes where deserialized `Row`s will be stored.
    #[with(Unsafe)]
    inner_data: UnsafeCell<AlignedBytes<DATA_INNER_LENGTH>>,

    /// `Row` phantom data.
    _phantom: PhantomData<Row>,
}

unsafe impl<Row> Sync for Data<Row> {}

impl<Row> From<page::Empty> for Data<Row> {
    fn from(e: page::Empty) -> Self {
        Self {
            id: e.page_id,
            free_offset: AtomicU32::default(),
            hint: Hint::default(),
            inner_data: UnsafeCell::default(),
            _phantom: PhantomData,
        }
    }
}

impl<Row> Data<Row> {
    /// Creates new [`Data`] page.
    pub fn new(id: page::Id) -> Self {
        Self {
            id,
            free_offset: AtomicU32::default(),
            hint: Hint::default(),
            inner_data: UnsafeCell::default(),
            _phantom: PhantomData,
        }
    }

    #[performance_measurement(prefix_name = "DataRow")]
    pub fn save_row<const N: usize>(&self, row: &Row) -> Result<page::Link, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
    {
        if self.hint.row_size.load(Ordering::SeqCst) != -1 {
            let row_size = self.hint.row_size.load(Ordering::Relaxed) as u32;
            let left = DATA_INNER_LENGTH as u32 - self.free_offset.load(Ordering::Relaxed);
            if  row_size > left {
                return Err(ExecutionError::PageIsFull { need: row_size, left });
            }
        }

        let bytes = rkyv::to_bytes(row).map_err(|_| ExecutionError::SerializeError)?;
        let length = bytes.len() as u32;
        if self.hint.row_size.load(Ordering::SeqCst) == -1 {
            self.hint.row_size.store(length as i32, Ordering::SeqCst)
        }

        let offset = self.free_offset.fetch_add(length, Ordering::SeqCst);
        self.hint.row_length.fetch_add(1, Ordering::Relaxed);

        let inner_data = unsafe { &mut *self.inner_data.get()};
        inner_data[offset as usize..][..length as usize].copy_from_slice(bytes.as_slice());

        let link = page::Link {
            page_id: self.id,
            offset,
            length,
        };

        Ok(link)
    }

    #[performance_measurement(prefix_name = "DataRow")]
    pub fn get_row_ref<'a>(&'a self, link: page::Link) -> Result<&'a <Row as Archive>::Archived, ExecutionError>
    where Row: Archive,
          <Row as Archive>::Archived: CheckBytes<DefaultValidator<'a>>
    {
        if link.offset > self.free_offset.load(Ordering::Relaxed) {
            return Err(ExecutionError::DeserializeError)
        }

        let inner_data = unsafe { & *self.inner_data.get()};
        let bytes = &inner_data[link.offset as usize..(link.offset + link.length) as usize];
        rkyv::check_archived_root::<Row>(&bytes[..]).map_err(|_| ExecutionError::DeserializeError)
    }

    #[performance_measurement(prefix_name = "DataRow")]
    pub fn get_row<'a>(&'a self, link: page::Link) -> Result<Row, ExecutionError>
    where Row: Archive,
          <Row as Archive>::Archived: CheckBytes<DefaultValidator<'a>> + Deserialize<Row, rkyv::Infallible>,
    {
        let archived = self.get_row_ref(link)?;
        archived.deserialize(&mut rkyv::Infallible).map_err(|_| ExecutionError::DeserializeError)
    }
}

/// Error that can appear on [`Data`] page operations.
#[derive(Copy, Clone, Debug)]
pub enum ExecutionError {
    /// Error of trying to save row in [`Data`] page with not enough space left.
    PageIsFull { need: u32, left: u32 },

    /// Error of saving `Row` in [`Data`] page.
    SerializeError,

    /// Error of loading `Row` from [`Data`] page.
    DeserializeError,
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, mpsc};
    use std::sync::atomic::Ordering;
    use std::thread;

    use rkyv::{Archive, Deserialize, Serialize};

    use crate::in_memory::page::data::{Data, INNER_PAGE_LENGTH};

    #[derive(
        Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    #[archive(compare(PartialEq), check_bytes)]
    #[archive_attr(derive(Debug))]
    struct TestRow {
        a: u64,
        b: u64,
    }

    #[test]
    fn data_page_length_valid() {
        let data = Data::<()>::new(1.into());
        let bytes = rkyv::to_bytes::<_, 4096>(&data).unwrap();

        assert_eq!(bytes.len(), INNER_PAGE_LENGTH)
    }

    #[test]
    fn data_page_save_row() {
        let mut page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row::<16>(&row).unwrap();
        assert_eq!(link.page_id, page.id);
        assert_eq!(link.length, 16);
        assert_eq!(link.offset, 0);

        assert_eq!(page.free_offset.load(Ordering::Relaxed), link.length);
        assert_eq!(page.hint.row_size.load(Ordering::Relaxed), link.length as i32);
        assert_eq!(page.hint.row_length.load(Ordering::Relaxed), 1);

        let inner_data = unsafe { &mut *page.inner_data.get()};
        let bytes = &inner_data[link.offset as usize..link.length as usize];
        let archived = rkyv::check_archived_root::<TestRow>(bytes).unwrap();
        assert_eq!(archived, &row)
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

        let inner_data = unsafe { &mut *page.inner_data.get()};

        for (i, link) in links.into_iter().enumerate() {
            let link = link.unwrap();

            let bytes = &inner_data[link.offset as usize..(link.offset + link.length) as usize];
            let archived = rkyv::check_archived_root::<TestRow>(bytes).unwrap();
            let row = rows.get(i).unwrap();

            assert_eq!(row, archived)
        }
    }

    #[test]
    fn data_page_get_row_ref() {
        let mut page = Data::<TestRow>::new(1.into());
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

        let links = other_links.into_iter().chain(links.into_iter()).map(|v| v.unwrap()).collect::<Vec<_>>();

        for link in links {
            let _ = shared.get_row(link).unwrap();
        }
    }
}
