I think we can use some logic, that will give us differences between current row state and state we need and we will
use this data to update indexes.

We can have some object like

```rust
pub struct Difference {
    // Any.... will have some notes about it below, I think it's some open question for now.
    old_value: Box<dyn Any>,
    new_value: Box<dyn Any>,
}
```

And we will have some trait `Comparable`

```rust
pub trait Comparable<With> {
    fn compare(&self, with: With) -> HashMap<&'static str, Difference>;
}
```

It will be used to compare `Row` with `Row` and some query values with row. It will return `Difference`s that can be
used
in secondary index object.

Main issue about this is that `Difference` will have different types for different row columns. So we need some way to
unify this, because we can't just have some `Difference<T>`. So first option is `Any`. We can just `downcast_ref` for
type we need because we will be fully sure that types will be correct.

Second option is enum. We can generate private enum like `AvailableType` for every table like:

```rust
enum AvailableType {
    U64(u64),
    I64(i64)
}
```

And our Difference will become:

```rust
pub struct Difference {
    old_value: AvailableType,
    new_value: AvailableType,
}
```

You can choose between two of this.

Next step is `TableSecondaryIndex` trait update. It will become something like:

```rust
pub trait TableSecondaryIndex<Row> {
    fn save_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn delete_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn process_differences(&self, differences: HashMap<Difference>) -> Result<(), WorkTableError>;
}
```

This method will be easily generated via codegen as I think. We can easily generate map from index name to field with
index
to update.