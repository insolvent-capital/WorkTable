# Queries

WorkTable support query definition feature. Users can add custom `update`, `delete` and `update_in_place` queries.

```rust
worktable!(
    name: Something,
    columns: {
        id: u64 primary_key autoincrement,
        name: String
        amount: u64,
        some_value: i64,
    },
    indexes: {
        value_idx: value unique,
        name_idx: name,
        some_value_idx: some_value,
    },
    // Queries declaration section.
    queries: {
        // `update` queries
        update: {
            AmountById(amount) by id,
        },
        // `delete` queries
        delete: {
            ByName() by name,
        },
        in_place: {
            SomeValueById(some_value) by id,
        }
    }
);
```

### `update` queries

`TODO`

### `update_in_place` queries

`update_in_place` queries are special update queries that allow you to update field's value
without need to select it before query. It is useful for counters, as example, because with
internal mutation queries locking logic user's don't need to add explicit locks over `WorkTable`
object. So you can safely use `update_in_place` queries in multiple threads simultaneously.

!!! For now only `by {pk_field}` queries are supported !!!

To declare `update_in_place` query you need to add `in_place` section to `queries`. Query definition is
same to `update`: `{YourQueryNameCamelCase}({fields_you_want_to_update}) by {by_field_name}`.
For example, in declaration above `update_in_place` is declared like this:

```
in_place: {
    SomeValueById(some_value) by id,
}
```

It will generate `update_some_value_by_id_in_place` method for `WorkTable` object (name generation logic is same
as for other queries). It will have two arguments: your `by` field value and closure, where you can use mutable
field value itself.

```rust
#[tokio::main]
async fn main() -> eyre::Result<()> {
    // Table creation.
    let table = SomethingWorkTable::default();
    let row = SomethingRow {
        // Autoincrement primary key generation.
        id: table.get_next_pk().into(),
        name: "SomeName".to_string(),
        amount: 100,
        some_value: 0,
    };
    let pk = table.insert(row)?;
    // This will lead to `some_value` field update by adding 100 to it value.
    table
        .update_some_value_by_id_in_place(|some_value| *some_value += 100, pk.0)
        .await?;
    let row = table.select(pk)?;
    assert_eq!(row.some_value, 100);

    Ok(())
}
```

You can find tests that covers `update_in_place` queries [here](../tests/worktable/in_place.rs).

### `delete` queries

`TODO`