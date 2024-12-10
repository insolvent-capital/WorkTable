Change Log
==========

## Unreleased

### Added

- add ability to choose index type in `worktable!` declaration.

### BC Breaks

- `.wt` files which are generated now have names as snake-case of table's name.
- `new` function now has only `DatabaseManager` as argument.

### Fixed

- `new` function generated if `persist: true` now is public.
- Bugs with insets and deletes after table load from file.

## [0.4.0]

### Added

- `SelectQueryBuilder` object that is used to customize `select_all` query. It has `limit` and `order_by` methods that 
can be used to limit returned row's count. `order_by` has not full functionality and is only available for indexed columns
and only `Oreder::Asc`.
- `SelectResult` object with is partially same to `SelectQueryBuilder`. It allows to limit/order returned rows. Both 
`Oreder::Asc` and `Oreder::Desc` are available. No issues with not indexed columns.
- added `offset` for `SelectQueryBuilder` and `SelectResult`.
- added `optional` column attribute instead of explicit `Option` type declaration.
- support for enums in queries
- Added generation of `Space` object that represents file that stores table's data.
- Added `DatbaseManager` object that is used to control multiple tables.
- Added methods for `Worktables` to use data in files. `persist` is used to save data to file. `load_from_file` is
used to load table from file.

### BC Breaks

- `select_all` now returns `SelectQueryBuilder` instead of `Vec<Row>`. To have same functionality old `select_all` users must call `execute` on returned builder.
- `select_by_{}` now returns `SelectResult` instead of `Vec<Row>`. To have same functionality old `select_all` users must call `execute` on returned builder.

## [0.3.10]

### BC Breaks

- Users don't need to define `<{ TestRow::ROW_SIZE }>` for `insert`, `update` and `upsert`.

### Added

- Support for `Option` types in columns.
- Support of `delete` queries.

### Fixed

- `Clippy` errors in macro declaration about unused `Result`'s.