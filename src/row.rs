pub trait TableRow<Pk> {
    fn get_primary_key(&self) -> &Pk;
}
