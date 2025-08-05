pub trait AvailableIndex {
    fn to_string_value(&self) -> String;
}

impl AvailableIndex for () {
    fn to_string_value(&self) -> String {
        "".to_string()
    }
}
