Change Log
==========

## Unreleased

### Added

- `SelectQueryBuilder` object that is used to customize `select_all` query. It has `limit` and `order_by` methods that 
can be used to limit returned row's count. `order_by` has not full functionality and is only available for indexed columns
and only `Oreder::Asc`.
- added `optional` column attribute instead of explicit `Option` type declaration.

### BC Breaks

- `select_all` now returns `SelectQueryBuilder` instead of `Vec<Row>`. To have same functionality old `select_all` users must call `execute` on returned builder.

## [0.3.10]

### BC Breaks

- Users don't need to define `<{ TestRow::ROW_SIZE }>` for `insert`, `update` and `upsert`.

### Added

- Support for `Option` types in columns.
- Support of `delete` queries.

### Fixed

- `Clippy` errors in macro declaration about unused `Result`'s.