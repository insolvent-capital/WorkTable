use crate::column::IntoColumn;
use crate::Value;

pub trait WorkTableField {
    #[allow(private_bounds)]
    type Type: IntoColumn + Into<Value>;
    const INDEX: usize;
    const NAME: &'static str;
    const PRIMARY: bool = false;
}
#[macro_export]
macro_rules! field {
    (
        $index: expr, $v: vis $f: ident: $ty: ty, $name: expr $(, primary = $indexed: expr)?
    ) => {
        $v struct $f;
        impl $crate::WorkTableField for $f {
            type Type = $ty;
            const INDEX: usize = $index;
            const NAME: &'static str = $name;
            $(const PRIMARY: bool = $indexed;)? // optional
        }
    };
}

// // Example schema macro usage
// worktable!(
//     name: Example
//     columns: {
//         id: i64 primary key,
//         example_column: f64,
//         another: u64
//     }
//     queries: {
//         select: {
//             ExampleAnother("example_column", "another")
//         },
//         update: {
//             ExampleAnother("example_column", "another")
//         }
//     }
// )
//
//
// // This macr will generate row type. If name will be added it can be NameRow
// struct ExampleRow {
//     id: i64,
//     example_column: f64,
//     another: u64
// }
// // Worktable can be used like
// let mut example_table = ExampleWorkTable::new();
// let example_row = Row {
//     id: 1,
//     example_column: 0.11234,
//     another: 2,
// }
// example_table.insert(examlpe_row); // basic insertion.
//
// let (example, another): (Column<f64>, Column<u64>) = example_table.select_example_another(); // full columns
// let (example, another): (f64, u64) = example_table.select_example_another_by_id(1); // one row
// let (example, another): (Column<f64>, Column<u64>) = example_table.select_example_another_by_ids([1, 2, 3]); // partial columns
// // TODO: Other select scenarios? Filtering?
//
// let u = UpdateExampleAnother {
//     id: 1, // Update must always have primary key to identify row
//     example_column: 1.432,
//     another 2,
// }
// example_table.select_example_another(s);
//
// {
// worktable!(
//     name: Price
//     columns: {
//         exchange: u8 primary key,
//         asks_price: [f64; 5],
//         bids_price: [f64; 5],
//         asks_qty: [f64; 5],
//         bids_qty: [f64; 5],
//     }
// )
//
// worktable!(
//     name: IsRising
//     columns: {
//         exchange: u8 primary key,
//         asks_price: [f64; 10],
//         bids_price: [f64; 10],
//     }
// )
//
// impl IsRising {
//     fn insert_new_price(&self, q: NewPriceQuery) -> bool {
//         // Manual because it's custom not boilerplate logic.
//         let prices = self.select(q.exchange);
//         let is_rising = prices.hidden(q); // update row with new price and check is_rising
//         self.insert(prices);
//
//         is_rising
//     }
// }
//
// struct PriceManager {
//     price_table: PriceWorkTable,
//     is_rising_table: IsRisingWorkTable
//     tx_orderbook_updated: AsyncSender<SignalOrderbookUpdated>,
// }
//
// impl PriceManager {
//     fn on_orderbook_response(&self, response: OrderBookResponse) {
//         let previous_orderbook = self.price_table.select(response.exchange);
//
//         let new_orderbook = self.price_table.update(respone.hidden());
//
//         let is_rising = self.is_rising_table.insert_new_price(response.into())
//
//         // TODO: Signal logic from rows.
//     }
// }
// }
