// This manager is used to share common table information.
#[derive(Debug, Clone)]
pub struct DatabaseManager {
    pub config_path: String,
    pub database_files_dir: String,
}

impl DatabaseManager {
    pub fn new(config_path: String, database_files_dir: String) -> Self {
        Self {
            config_path,
            database_files_dir,
        }
    }
}
