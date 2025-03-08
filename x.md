- [x] Add methods for data page to update it's parts. It will have `Link` and byte array as args.
- [ ] Fix `PageId`s mismatch in `Space` and `DataPages`. `DataPages` always adds pages with incremental `PageId`'s first. But
  in `Space` it can become different `PageId`. For example `SpaceInfo` is always 0, then primary index, so data is at
  least 3rd. After read we will get page with 3 as id, but all `Link`'s in indexes will be wrong. (in progress by ATsibin)
- [ ] Check `PersistTable` macro and see `Space` type that is generated (`Space` describes file structure).
  You need to add methods to add pages to the `Space` correctly. You must be careful with `Intervals` that describes data layout.
- [ ] Check `indexset` and see how to map internal nodes to disk representation. Also there must be possibility to set node size to
meet page size correctly.
- [ ] Add `PesristEngine` object to `WorkTable`. It will contain queue of write operations to sync in-memory with file.
    - [x] Create operation as struct representation. As I think it can be enum of `Create`, `Update` and `Delete`.
      `Create` ops contains primary + secondary keys data, `Link` on data page and data as bytes array. `Update` should just
      contain `Link` on data page and data as bytes array. `Delete` should contain primary + secondary keys data to find and
      remove them from index pages (index pages are not optimised now, empty links are also not optimised now).
    - [ ] Create `PesristEngine` object that will contain queue of ops and will apply them to the file. (in progress by me)
    - [ ] Add logic to generated tables code in methods to push operations into `PesristEngine`.

