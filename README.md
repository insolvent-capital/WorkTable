# Absolutely not a Database (WorkTable)

`WorkTable` is in-memory (on-disk persistence is in progress currently) storage.

## Usage

`WorkTable` can be used just in user's code with `worktable!` macro. It will generate table structs and other related
structs that will be used for table logic.

```rust
worktable!(
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: u64 optional,
        exchange: String
    },
    indexes: {
        test_idx: test unique,
        exchnage_idx: exchange,
    }
    queries: {
        update: {
            AnotherByExchange(another) by exchange,
            AnotherByTest(another) by test,
            AnotherById(another) by id,
        },
        delete: {
            ByAnother() by another,
            ByExchange() by exchange,
            ByTest() by test,
        }
    }
);
```

## Declaration parts

### `name` declaration

`name` field is used to define table's name, and is a prefix for generated objects. For example declaration
above will generate struct `TestWorkTable`, so table struct will always have name as `<name>WorkTable`.

```rust
let table = TestWorkTable::default ();
let name = table.name();
assert_eq!(name, "Test");
```

### `columns` declaration

`columns` field is used to define table's row schema. Default usage is `<column_name>: <type>`. But also there are some
flags that can be applied to columns as `<column_name>: <type> <flags>*`.

Flags list:

- `primary_key` flag and related to it.
- `optional` flag.

#### `primary_key` flag declaration

If user want to mark column as primary key `primary_key` flag is used. This flag can be used on multiple columns at a
time. Primary key generation is also supported. For some basic types `autoincrement` is supported. Also `custom`
generation is available. In this case user must provide his own implementation.

```rust
#[derive(
    Archive,
    Debug,
    Default,
    Deserialize,
    Clone,
    Eq,
    From,
    PartialOrd,
    PartialEq,
    Ord,
    Serialize,
    SizeMeasure,
)]
#[rkyv(compare(PartialEq), derive(Debug))]
struct CustomId(u64);

#[derive(Debug, Default)]
pub struct Generator(AtomicU64);

impl PrimaryKeyGenerator<TestPrimaryKey> for Generator {
    fn next(&self) -> TestPrimaryKey {
        let res = self.0.fetch_add(1, Ordering::Relaxed);
        if res >= 10 {
            self.0.store(0, Ordering::Relaxed);
        }
        CustomId::from(res).into()
    }
}

impl TablePrimaryKey for TestPrimaryKey {
    type Generator = Generator;
}

worktable!(
  name: Test,
  columns: {
    id: CustomId primary_key custom,
    test: u64
  }
);
```

For primary key newtype is generated for declared type:

```rust
// Generated code
#[derive(
    Clone,
    rkyv::Archive,
    Debug,
    rkyv::Deserialize,
    rkyv::Serialize,
    From,
    Eq,
    Into,
    PartialEq,
    PartialOrd,
    Ord
)]
pub struct TestPrimaryKey(u64);
```

#### `optional` flag declaration

If column field is `Option<T>`, `optional` flag can be used like it was done in declaration.

```rust
another: u64 optional,
```

#### Row type generation

For described column row type struct is generated:

```rust
// Generated code
#[derive(
    rkyv::Archive,
    Debug,
    rkyv::Deserialize,
    Clone,
    rkyv::Serialize,
    PartialEq
)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct TestRow {
    pub id: u64,
    pub test: i64,
    pub another: Option<u64>,
    pub exchange: String,
}
 ```

This struct is used in `WorkTable` interface and will be used by users.

### `indexes` declaration

`indexes` field is used to define table's index schema. Default usage is `<index_name>: <column_name> <unique>?`.

Index allows faster access to data by some field. Adding `indexes` field adds methods to the generated `WorkTable`. This
method for now is `select_by_<indexed_column_name>`. It will be described below.

### Default implemented `queries`

There are some default query implementations that are available for all `WorkTable`'s:

- `select(&self, pk: <Name>PrimaryKey) -> Option<<Name>Row>`;
- `insert(&self, row: <Name>Row) -> Result<<Name>PrimaryKey, WorkTableError>`;
- `upsert(&self, row: <Name>Row) -> Result<(), WorkTableError>`;
- `update(&self, row: <Name>Row) -> Result<(), WorkTableError>`;
- `delete(&self, pk: <Name>PrimaryKey) -> Result<(), WorkTableError>`;
- `select_all<'a>(&'a self) -> SelectQueryBuilder<'a, <Name>Row, Self>`;

### `queries` declaration

`indexes` field is used to define table's queries schema. Queries are used to update/select/delete data.

```
queries: {
    update: {
        AnotherByExchange(another) by exchange,
        AnotherByTest(another) by test,
        AnotherById(another) by id,
    },
    delete: {
        ByAnother() by another,
        ByExchange() by exchange,
        ByTest() by test,
    }
}
```

Default query declaration is `<QueryName>(<column_name>*) by <column_name>`. It is same for update/select/delete.

For each query `<QueryName>Query` and `<QueryName>By` structs are generated. They will be used by user to call the
query.

#### `update` query declaration

`update` queries are used to update row's data partially. Default generated `update` allows only full update of the row.
But if user's logic needs some simultaneous update of row parts from different code parts. `update` logic supports
smart lock logic that allows simultaneous update of not overlapping row fields.

## WorkTable internals structure

```rust 
worktable
    pub struct WorkTable   -- The main container that holds all data and manages its structure.

Fields

    data: DataPages<Row, DATA_LENGTH>       // stores data as pages (DataPages)
    pk_map: IndexType                       // primary index ensuring the uniqueness of records
    indexes: SecondaryIndexes               // secondary indexes for efficient searches across other columns
    pk_gen: PkGen                           // Primary Key Generator 
    lock_map: LockMap                       // from indexset crate, supports data ordering with LockMap
    table_name: &'static str                // table name (e.g., Test, which generates TestWorkTable and TestRow
    pk_phantom: PhantomData<PrimaryKey>     // a helper field for type management

Implementations 

   pub fn default() -- creates default WorkTable

```

```rust
worktable::in_memory
    pub struct DataPages  -- A container for managing data pages

Fields (/*private*/)

    pages: RwLock<Vec<Arc<Data<...>>>>,  // an array of pages (Data) that hold the records
    empty_links: Stack<Link>,            // a stack for storing links to deleted records
    row_count: AtomicU64,                // a counter for the current number of records
    last_page_id: AtomicU32,             // identifier for last page 
    current_page_id: AtomicU32,          // identifier for current page

Implementations

   pub fn new() -> Self
   pub fn from_data(vec: Vec<Arc<Data<<Row as StorableRow>::WrappedRow, DATA_LENGTH>>>,) -> Self
   pub fn insert(&self, row: Row) -> Result<Link, ExecutionError>   
   pub fn select(&self, link: Link) -> Result<Row, ExecutionError>
   pub fn with_ref<Op, Res>(&self, link: Link, op: Op,) -> Result<Res, ExecutionError>
   pub unsafe fn with_mut_ref<Op, Res>(&self, link: Link, op: Op,) -> Result<Res, ExecutionError>
   pub unsafe fn update<const N: usize>(&self, row: Row, link: Link,) -> Result<Link, ExecutionError>
   pub fn delete(&self, link: Link) -> Result<(), ExecutionError>
   pub fn get_bytes(&self) -> Vec<([u8; DATA_LENGTH], u32)>
   pub fn get_page_count(&self) -> usize
   pub fn get_empty_links(&self) -> Vec<Link>
   pub fn with_empty_links(self, links: Vec<Link>) -> Self
```

```rust 
in-memory::data
    pub struct Data  -- Data itself 

Fields
    pub free_offset: AtomicU32,                        // the offset to the first free byte 
    (/* private */)   
    id: PageId,                                        // the identifier of the page
    inner_data: UnsafeCell<AlignedBytes<DATA_LENGTH>>, // a byte array where rows are stored 
    _phantom: PhantomData<Row>,                        // a helper field for type management

Implementations 

   pub fn new(id: PageId) -> Self 
   pub fn from_data_page(page: GeneralPage<DataPage<DATA_LENGTH>>) -> Self 
   pub fn set_page_id(&mut self, id: PageId) 
   pub fn save_row(&self, row: &Row) -> Result<Link, ExecutionError
   pub unsafe fn save_row_by_link(&self, row: &Row, link: Link) -> Result<Link, ExecutionError
   pub unsafe fn get_mut_row_ref
   pub fn get_row_ref(&self, link: Link) -> Result<&<Row as Archive>::Archived, ExecutionError
   pub fn get_row(&self, link: Link) -> Result<Row, ExecutionError
   pub fn get_bytes(&self) -> [u8; DATA_LENGTH] 



```

```rust 
enum WorkTableError
    NotFound,
    AlreadyExists,
    SerializeError,
    PagesError(in_memory::PagesExecutionError),
```


## Examples 

Check out - [Examples](./examples)


