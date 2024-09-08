pub trait TableRow<Pk> {
    const ROW_SIZE: usize;

    fn get_primary_key(&self) -> Pk;
}
