use std::mem;

use crate::in_memory::data::LINK_LENGTH;
use crate::prelude::Link;

/// Marks an objects that can return theirs approximate size after archiving via
/// [`rkyv`].
pub trait SizeMeasurable {
    /// Returns approximate size of the object archiving via [`rkyv`].
    fn approx_size(&self) -> usize;
}

macro_rules! size_measurable_for_sized {
    ($($t:ident),+) => {
        $(
            impl SizeMeasurable for $t {
                fn approx_size(&self) -> usize {
                    mem::size_of::<$t>()
                }
            }
        )+
    };
}

size_measurable_for_sized! {u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64, bool}

impl SizeMeasurable for Link {
    fn approx_size(&self) -> usize {
        LINK_LENGTH
    }
}

// That was found on practice... Check unit test for proofs that works.
impl SizeMeasurable for String {
    fn approx_size(&self) -> usize {
        if self.len() <= 8 {
            8
        } else {
            if (self.len() + 8) % 4 == 0 {
                self.len() + 8
            } else {
                (self.len() + 8) + (4 - (self.len() + 8) % 4)
            }
        }
    }
}

impl SizeMeasurable for [u8; 32] {
    fn approx_size(&self) -> usize {
        mem::size_of::<[u8; 32]>()
    }
}

impl SizeMeasurable for [u8; 20] {
    fn approx_size(&self) -> usize {
        mem::size_of::<[u8; 20]>()
    }
}

#[cfg(test)]
mod test {
    use crate::util::sized::SizeMeasurable;

    #[test]
    fn test_string() {
        // Test if approximate size is correct for strings
        for i in 0..10_000 {
            let s = String::from_utf8(vec![b'a'; i]).unwrap();
            assert_eq!(
                s.approx_size(),
                rkyv::to_bytes::<rkyv::rancor::Error>(&s).unwrap().len()
            )
        }
    }

    #[test]
    fn test_size_measurable_for_u8_array() {
        let array = [0u8; 32];
        assert_eq!(array.approx_size(), 32);
    }
}
