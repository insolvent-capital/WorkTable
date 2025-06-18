use data_bucket::Link;
use indexset::core::multipair::MultiPair;
use indexset::core::pair::Pair;

pub trait MultiPairRecreate<T> {
    fn with_last_discriminator(self, discriminator: u64) -> MultiPair<T, Link>;
}

impl<T> MultiPairRecreate<T> for Pair<T, Link> {
    fn with_last_discriminator(self, discriminator: u64) -> MultiPair<T, Link> {
        MultiPair {
            key: self.key,
            value: self.value,
            discriminator: fastrand::u64(discriminator..),
        }
    }
}
