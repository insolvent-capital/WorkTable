# Absolutely not a Database (WorkTable)

## What we have for now

WorkTable macro for generating type alias and row type.

```rust
    worktable! (
        name: Test,
        columns: {
            id: u64 primary_key,
            test: i64
        }
    );
```

Expanded as:

```rust
#[derive(Debug, Clone)]
    pub struct TestRow {
        id: u64,
        test: i64,
    }

impl worktable::TableRow<u64> for TestRow { fn get_primary_key(&self) -> &u64 { &self.id } }

type TestWorkTable<I> = worktable::WorkTable<TestRow, u64, I>;
```

* Underlying structure as `Vec<Row>` with no sync algorithms, `BTreeMap` for primary key.

## TODO parts:

1. [Underlying structure refactor.](#underlying-structure-refactor)
2. [Query macros support.](#query-macros-support)
3. [Persistence support.](#persistence-support)

## Underlying structure refactor.

We have big amount off nearly small objects, so we need to control allocations to optimize theirs creation. For
achieving this we can store table's data on `Pages`.

### Pages

Pages are byte arrays of some size (4Kb as minimal value, it's better to choose this value from disk storage page size).
Data stored in these pages in some deserialized format (some kind of binary format. **rkyv** can be used for
serialization and deserialization).

```rust
struct Page {
    data: [u8; PAGE_SIZE],
    
    info: PageInfo, // Some info about `Page` like it's index etc.
}
```

To navigate on pages _link_'s can be used.

```rust
struct Link {
    /// Id of the page where data is located.
    page_id: u64, 
    
    /// Offset ona page (< PAGE_SIZE, so u16 will be enough up to 64Kb page)
    offset: u16,
    
    /// Length of the data. For rows this will be same, so maybe not used.
    len: u16,
}
```

### Empty link storage

When we added some data and then deleted we will have a gap in bytes, and we need to control this gaps and fill them
with data. If we have rows-based storage, all rows will have same length, so when old is deleted, we can easily
replace it with new one. So we need some storage (stack) for this empty links.

Lock-free stack (using atomics) can be used here, because we don't want to lock on new row addition.

### Defragmentation (?)

When count of empty link will massively grow, we will have empty pages or big gaps in pages data. So, if we need, we can
have defragmentation algorithm that will tighten data and delete gaps.

### Locks on operations

As was said before we don't want to lock on addition, so we can use atomic for page's tail, and lock-free stack for
empty links. On row addition we wil first check empty link registry, and use link popped from it if there is some. If
there is no links, we will use current tails index (which will be stored in atomic) and use it for new data storage.
We possibly can have locks only on new page allocation.

Updates and deletes will be always lock actions.

So, for lock control we can use map that will map row's primary key to it's `RwLock`. On new data addition. For indexes
lock-free map can be used.

Upd. Maybe atomic pointers can be used here, but I'm no sure.

So, modifying algorithm will look like: we get `RwLock` by row's primary key (or index key), then modify row, and\
releasing lock.

Upd. No locks for updating/deleting. Delete as flag for row, update as delete + insert.

### Filtering data

I think for filtering we will need to copy table, which is bad. I need to think about filtering more to make it more
effective.

### Foreign keys

Foreign keys can be implemented as map from key-id to other's table row link. So join operation on row can be done by
O(1).

## Query macros support.

We need to extend macro usage to minimize client boilerplate code. Example:
```rust
{
worktable!(
    name: Price
    columns: {
        id: u64 primary key autoincrement,
        exchange: u8,
        level: u8,
        asks_price: f64,
        bids_price: f64,
        asks_qty: f64,
        bids_qty: f64,
        timestamp: u64,
    }
    queries: {
        select: {
            // similar to SELECT bids_price, bids_qty FROM price where exchange=$1
            BidsPriceQty("bids_price", "bids_qty") by "exchange" as bids_price_by_exchange, // name override
            // similar to SELECT bids_price, bids_qty FROM price where bids_price>$1
            BidsPriceQty("bids_price", "bids_qty") by "bids_price" > as bids_price_above,
            // similar to SELECT bids_price, bids_qty FROM price where timestamp>$1 and timestamp<$2
            BidsPriceQty("bids_price", "bids_qty") by "timestamp" > and "timestamp" < as bids_price_by_date,
        }
    }
);

let price_table = PriceWorkTable::new();

// Result is multiple rows.
// without override price_table.bids_price_qty_by_exchange()
let binance_orders: BidsPriceByExchange = price_table.bids_price_by_exchange(Exchange::BinanceSpot as u8);

// Result is multiple rows.
// without override price_table.bids_price_qty_by_bids_price_more()
let binance_orders: BidsPriceAbove = price_table.bids_price_above(1000.0);

// Result is still multiple rows.
// without override price_table.bids_price_qty_by_timestamp_more_and_timestamp_less()
let binance_orders: BidsPriceByDate = price_table.bids_price_by_date(123312341, 1234128345);
}
```

As inspiration for macro interfaces/design [this crate](https://github.com/helsing-ai/atmosphere) can be used.
It contains derive macro implementation, but it's still usable for our case.



## Persistence support.

Next step after in-memory storage we need to add persistence support.

For starting point we can use [mmap-sync](https://github.com/cloudflare/mmap-sync/tree/main) which has mapped files
implementation and read/write interface. We will need pages reader/writer for our storage engine.

### Data container format

As starting point innodb format was chosen. We can use it for storing tables data
([leaf pages](https://github.com/Codetector1374/InnoDB_rs/blob/master/src/innodb/page/mod.rs) of b-tree must have nearly
same layout).

#### Page format

Original innodb [format](https://blog.jcole.us/2013/01/03/the-basics-of-innodb-space-file-layout/)

General `Page` layout:

```text
+----------------------+---------+
| Offset (Page number) | 4 bytes |
+----------------------+---------+
| Previous page ID     | 4 bytes |
+----------------------+---------+
| Next page ID         | 4 bytes |
+----------------------+---------+
| Page type            | 2 bytes |
+----------------------+---------+
| Space ID             | 4 bytes |
+----------------------+---------+
| Page data            | n bytes |
+----------------------+---------+
```

Total header length is `18 bytes`.

* Offset is current `Page`'s ID, in code will be represented as `u32` (4,294,967,295 available pages).
* Previous page ID is ID of previous _logical_ `Page`.
* Next page ID is ID of next _logical_ `Page`. These IDs are used to form doubly-linked list from pages in one file.
* Page type describes type of this page (TODO: Describe pages types)
* Space ID is ID of file (Space) to which this `Page` is related to.
* Page data is just pages internal data.

Comparison with original InnoDB:
* No checksum part in header (we don't care about this).
* No LSN page modification header
* No flush LSN header
* No `Page` trailer

#### File layout

Original InnoDB [format](https://blog.jcole.us/2013/01/04/page-management-in-innodb-space-files/)

For each table separate file will be used. This files will be named as `Space`'s. Each space will have some general
structure.

General `Space` layout:

```text
+-------------------------------+---------+
| Space internals page          | 1 Page  |
+-------------------------------+---------+
| Space Pages                   | n Pages |
+-------------------------------+---------+
```

`Space` internal page:

```text
+-----------------------+-----------+
| General page header   | 18 bytes  |
+-----------------------+-----------+
| Space header          | 12 bytes  |
+-----------------------+-----------+
| Table schema          | n bytes   |
+-----------------------+-----------+
```

As each `Space` is related to separate `Table`, it must contain this `Table`'s schema to validate data and row structure.

`Space` header:

```text
+-------------------------------+---------+
| Space ID                      | 4 bytes |
+-------------------------------+---------+
| Highest used Page number      | 4 bytes |
+-------------------------------+---------+
| Highest allocated Page number | 4 bytes |
+-------------------------------+---------+
```

#### Page types

Original InnoDB page [types](https://github.com/Codetector1374/InnoDB_rs/blob/6a153a7185feb31e8a31369c9671c4497f56e1c7/src/innodb/page/mod.rs#L99C3-L99C4)

```rust
#[repr(u16)]
enum PageType {
    /// Newly allocated pages.
    Free = 0,
    /// Space header `Page` type.
    SpaceHeader = 1,
    /// Table data `Page` type.
    Data = 2,
    /// Index `Page` type.
    Index = 3,
}
```

### Not sized types (strings)

Strings must be stored as varchars, we don't need to have empty padding in rows to have same width. We can have
different row width, because we will hae links for the rows which contain its length. So we don't need this empty tail.

Same logic can be used for not sized array data types.

### Files read/write logic

For [fastest way possible to read bytes](https://users.rust-lang.org/t/fastest-way-possible-to-read-bytes/86177) we need
multiple opened read/write threads for one file. Using this we can update different parts of file at one time. To use
this io_uring can be used. For this approach we have multiple solutions:

* https://github.com/ringbahn/iou - Interface to Linux's io_uring interface
* https://github.com/bytedance/monoio - A thread-per-core Rust runtime with io_uring/epoll/kqueue. It's goal is to
  replace Tokio. They have benchmarks that proves that they are _blazingly_ fast [wow](https://github.com/bytedance/monoio/blob/master/docs/en/benchmark.md).
* https://github.com/tokio-rs/io-uring - tokios io-uring raw interface. Is unsafe.
* https://github.com/tokio-rs/tokio-uring - io-uring in Tokio runtime. Safe.
* https://github.com/compio-rs/compio - inspired by MonoIO crate. Don't think we rally need this because it's feature is
  that this crates supports Windows. Do we need Windows?

[Discussion](https://users.rust-lang.org/t/file-reading-async-sync-performance-differences-hyper-tokio/34696) about
difference between sync and async i/o (5 years ago before io_uring was added).

### Bunch of links that must be sorted...................

https://crates.io/crates/faster-hex - where do we need to use hex? I don't know............


