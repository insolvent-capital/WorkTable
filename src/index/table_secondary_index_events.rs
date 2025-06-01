pub trait TableSecondaryIndexEventsOps {
    fn extend(&mut self, another: Self)
    where
        Self: Sized;
}
