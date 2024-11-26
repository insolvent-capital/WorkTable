// This manager is used to share common table information.
#[derive(Debug, Clone)]
pub struct DatabaseManager {
    pub config_path: String,
}

impl DatabaseManager {
    pub fn new(config_path: String) -> Self {
        Self { config_path }
    }
}
