use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};

pub trait TablePrimaryKey {
    type Generator;
}

pub trait PrimaryKeyGenerator<T> {
    fn next(&self) -> T;
}

pub trait PrimaryKeyGeneratorState {
    type State;

    fn get_state(&self) -> Self::State;

    fn from_state(state: Self::State) -> Self;
}

impl<T> PrimaryKeyGenerator<T> for AtomicU32
where
    T: From<u32>,
{
    fn next(&self) -> T {
        self.fetch_add(1, Ordering::Relaxed).into()
    }
}

impl PrimaryKeyGeneratorState for AtomicU32 {
    type State = u32;

    fn get_state(&self) -> Self::State {
        self.load(Ordering::Relaxed)
    }

    fn from_state(state: Self::State) -> Self {
        AtomicU32::from(state)
    }
}

impl<T> PrimaryKeyGenerator<T> for AtomicU64
where
    T: From<u64>,
{
    fn next(&self) -> T {
        self.fetch_add(1, Ordering::Relaxed).into()
    }
}

impl PrimaryKeyGeneratorState for AtomicU64 {
    type State = u64;

    fn get_state(&self) -> Self::State {
        self.load(Ordering::Relaxed)
    }

    fn from_state(state: Self::State) -> Self {
        AtomicU64::from(state)
    }
}

impl<T> PrimaryKeyGenerator<T> for AtomicI64
where
    T: From<i64>,
{
    fn next(&self) -> T {
        self.fetch_add(1, Ordering::Relaxed).into()
    }
}

impl PrimaryKeyGeneratorState for AtomicI64 {
    type State = i64;

    fn get_state(&self) -> Self::State {
        self.load(Ordering::Relaxed)
    }

    fn from_state(state: Self::State) -> Self {
        AtomicI64::from(state)
    }
}

impl PrimaryKeyGeneratorState for () {
    type State = ();

    fn get_state(&self) -> Self::State {
        ()
    }

    fn from_state((): Self::State) -> Self {
        ()
    }
}
