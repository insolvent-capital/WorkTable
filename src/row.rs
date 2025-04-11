pub trait TableRow<Pk> {
    fn get_primary_key(&self) -> Pk;
    fn row_schema() -> Vec<(String, String)>;
    fn primary_key_fields() -> Vec<String>;
}
