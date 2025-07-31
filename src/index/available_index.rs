pub trait AvailableIndex {
    fn to_string(&self) -> String;
}

impl AvailableIndex for () {
    fn to_string(&self) -> String {
        "".to_string()
    }
}
