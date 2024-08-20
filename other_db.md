* https://github.com/rust-lib-project/calibur - file system code https://github.com/rust-lib-project/calibur/tree/main/src/common/file_system. 
Strange, only read/write logic. https://github.com/rust-lib-project/calibur/blob/main/src/table/block_based/table_builder.rs
write logic usage for table.

* https://gitlab.com/persy/persy has only sync file interface, no async. It's bad....

* https://www.sqlite.org/fileformat.html useful file format. Pretty simple and good.

* https://github.com/cloudflare/mmap-sync/tree/main uses memory mapped files. Is used to save some abstract data, can
be refactored for our use case. Also memory mapped files usage is good example.

* https://github.com/naoto0822/mysql-parser-rs MySql dialect query parser. not usable at all.

 .---------------------------------------------------------------------------------------------------------------------

* https://github.com/zombodb/zombodb/ not usable, just a wrapper around postgres + elastic.

* https://github.com/zeedb/ZeeDB/?tab=readme-ov-file not usable, no disk write logic, sync lockable pages.

* https://github.com/erikgrinaker/toydb/blob/master/src/storage/bitcask.rs key-value storage. append-only, don't think 
it's really useful

* https://github.com/oxigraph/oxigraph/tree/main nothing useful

* https://github.com/vincent-herlemont/native_db/tree/main key-value, nothing useful.

* https://github.com/helsing-ai/atmosphere useful macro parts and interface.

* https://github.com/tontinton/dbeel/tree/main uses glommio with io_uring, can be useful

* https://github.com/cutsea110/simpledb/tree/master sync filesystem using mutexes. not think can be usable

* https://github.com/influxdata/influxdb/tree/main/influxdb3_write/src some useful parts can be found, but it's sync.

* https://github.com/tikv/tikv key-value, nothing useful.

* https://github.com/PoloDB/PoloDB/tree/master rocksdb wrapper.