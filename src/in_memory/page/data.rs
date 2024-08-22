use std::marker::PhantomData;

use derive_more::Display;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::{with::Skip, Archive, Deserialize, Serialize, CheckBytes, Fallible};
use rkyv::validation::validators::DefaultValidator;
use crate::in_memory::page::{self, HEADER_LENGTH, INNER_PAGE_LENGTH, PAGE_SIZE};

/// Length of the [`Data`] page header.
pub const DATA_HEADER_LENGTH: usize = 4;

/// Length of the inner [`Data`] page part.
pub const DATA_INNER_LENGTH: usize = INNER_PAGE_LENGTH - DATA_HEADER_LENGTH;

#[derive(
    Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct Data<Row> {
    /// [`Id`] of the [`General`] page of this [`Data`].
    ///
    /// [`Id]: page::Id
    /// [`General`]: page::General
    #[with(Skip)]
    id: page::Id,

    /// Offset to the first free byte on this [`Data`] page.
    free_offset: u32,

    /// Optional size of the `Row` which stored on this [`Data`] page. If row
    /// is unsized (contains [`String`] etc.) row size will be `None`.
    row_size: Option<u32>,

    /// Inner array of bytes where deserialized `Row`s will be stored.
    inner_data: [u8; DATA_INNER_LENGTH],

    /// `Row` phantom data.
    _phantom: PhantomData<Row>,
}

impl<Row> From<page::Empty> for Data<Row> {
    fn from(e: page::Empty) -> Self {
        Self {
            id: e.page_id,
            free_offset: 0,
            row_size: None,
            inner_data: [0; PAGE_SIZE - HEADER_LENGTH - DATA_HEADER_LENGTH],
            _phantom: PhantomData,
        }
    }
}

impl<Row> Data<Row> {
    /// Creates new [`Data`] page.
    pub fn new(id: page::Id) -> Self {
        Self {
            id,
            free_offset: 0,
            row_size: None,
            inner_data: [0; DATA_INNER_LENGTH],
            _phantom: PhantomData,
        }
    }

    pub fn save_row<const N: usize>(&mut self, row: &Row) -> Result<page::Link, ExecutionError>
    where
        Row: Archive + Serialize<AllocSerializer<N>>,
    {
        if let Some(size) = self.row_size {
            let left = INNER_PAGE_LENGTH as u32 - self.free_offset;
            if size > left {
                return Err(ExecutionError::PageIsFull { need: size, left });
            }
        }

        let bytes = rkyv::to_bytes(row).map_err(|_| ExecutionError::SerializeError)?;
        let length = bytes.len() as u32;
        if self.row_size.is_none() {
            self.row_size = Some(bytes.len() as u32);
        }

        let offset = self.free_offset;
        self.free_offset = self.free_offset + length;

        self.inner_data[offset as usize..][..length as usize].copy_from_slice(bytes.as_slice());
        let link = page::Link {
            page_id: self.id,
            offset,
            length,
        };

        Ok(link)
    }

    pub fn get_row_ref<'a>(&'a self, link: page::Link) -> Result<&'a <Row as Archive>::Archived, ExecutionError>
    where Row: Archive,
          <Row as Archive>::Archived: CheckBytes<DefaultValidator<'a>>
    {
        if link.offset > self.free_offset {
            return Err(ExecutionError::DeserializeError)
        }

        let bytes = &self.inner_data[link.offset as usize..link.length as usize];
        rkyv::check_archived_root::<Row>(&bytes[..]).map_err(|_| ExecutionError::DeserializeError)
    }

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

        let link = page.save_row::<8>(&row).unwrap();
        assert_eq!(link.page_id, page.id);
        assert_eq!(link.length, 16);
        assert_eq!(link.offset, 0);

        assert_eq!(page.free_offset, link.length);
        assert_eq!(page.row_size, Some(link.length));

        let bytes = &page.inner_data[link.offset as usize..link.length as usize];
        let archived = rkyv::check_archived_root::<TestRow>(&bytes[..]).unwrap();
        assert_eq!(archived, &row)
    }

    #[test]
    fn data_page_get_row_ref() {
        let mut page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row::<8>(&row).unwrap();
        let archived = page.get_row_ref(link).unwrap();
        assert_eq!(archived, &row)
    }

    #[test]
    fn data_page_get_row() {
        let mut page = Data::<TestRow>::new(1.into());
        let row = TestRow { a: 10, b: 20 };

        let link = page.save_row::<8>(&row).unwrap();
        let deserialized = page.get_row(link).unwrap();
        assert_eq!(deserialized, row)
    }
}
