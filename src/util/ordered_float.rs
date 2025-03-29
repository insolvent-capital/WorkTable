use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize)]
#[rkyv(remote = ordered_float::OrderedFloat<f64>, archived = ArchivedF64)]
#[rkyv(derive(Debug))]
pub struct OrderedF64Def {
    #[rkyv(getter = std::ops::Deref::deref)]
    value: f64,
}

impl From<OrderedF64Def> for ordered_float::OrderedFloat<f64> {
    fn from(value: OrderedF64Def) -> Self {
        ordered_float::OrderedFloat(value.value)
    }
}

#[derive(Archive, Serialize, Deserialize)]
#[rkyv(remote = ordered_float::OrderedFloat<f32>, archived = ArchivedF32)]
#[rkyv(derive(Debug))]
pub struct OrderedF32Def {
    #[rkyv(getter = std::ops::Deref::deref)]
    value: f32,
}

impl From<OrderedF32Def> for ordered_float::OrderedFloat<f32> {
    fn from(value: OrderedF32Def) -> Self {
        ordered_float::OrderedFloat(value.value)
    }
}
