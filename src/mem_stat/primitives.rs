use super::MemStat;

#[macro_export]
macro_rules! impl_memstat_zero {
    ($($t:ty),*) => {
        $(
            impl MemStat for $t {
                fn heap_size(&self) -> usize { 0 }
                fn used_size(&self) -> usize { 0 }
            }
        )*
    };
}

impl_memstat_zero!(
    u8,
    i8,
    u16,
    i16,
    u32,
    i32,
    u64,
    i64,
    usize,
    isize,
    f32,
    f64,
    bool,
    char,
    u128,
    i128,
    std::num::NonZeroU8,
    std::num::NonZeroU16,
    std::num::NonZeroU32,
    std::num::NonZeroU64,
    std::num::NonZeroU128,
    std::num::NonZeroUsize,
    std::num::NonZeroI8,
    std::num::NonZeroI16,
    std::num::NonZeroI32,
    std::num::NonZeroI64,
    std::num::NonZeroI128,
    std::num::NonZeroIsize,
    std::time::Duration,
    std::time::SystemTime,
    std::time::Instant
);
