#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    pub config_path: String,
    pub tables_path: String,
}

impl PersistenceConfig {
    pub fn new<S1: Into<String>, S2: Into<String>>(
        config_path: S1,
        table_files_dir: S2,
    ) -> eyre::Result<Self> {
        Ok(Self {
            config_path: config_path.into(),
            tables_path: table_files_dir.into(),
        })
    }
}
