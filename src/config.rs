use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub log: Option<LogSection>,
    pub database: Option<DatabaseSection>,
    // testing section removed
}

#[derive(Debug, Deserialize)]
pub struct LogSection {
    pub enable_stdout: Option<bool>,
    pub log_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseSection {
    pub db_path: Option<String>,
    pub chunk_size: Option<usize>,
    pub create_indexes: Option<bool>,
}

// testing section removed: tests should use environment variables for failure injection

/// Runtime-ready configuration values derived from `Config`.
pub struct RuntimeConfig {
    pub db_path: String,
    pub chunk_size: usize,
    pub create_indexes: bool,
    pub enable_stdout: bool,
    pub log_dir: Option<PathBuf>,
}

impl Config {
    pub fn load() -> Self {
        // Search order for config files (same as before)
        let candidates = {
            let mut v: Vec<PathBuf> = Vec::new();

            // standard candidate locations (no environment overrides)
            v.push(PathBuf::from("config").join("config.toml"));
            v.push(PathBuf::from("config.toml"));
            if let Some(home) = dirs::home_dir() {
                v.push(
                    home.join(".config")
                        .join("sqllog-analysis")
                        .join("config.toml"),
                );
            }
            if cfg!(windows) {
                // On Windows, also check ProgramData default location
                v.push(
                    PathBuf::from("C:")
                        .join("ProgramData")
                        .join("sqllog-analysis")
                        .join("config.toml"),
                );
            } else {
                v.push(
                    PathBuf::from("/etc")
                        .join("sqllog-analysis")
                        .join("config.toml"),
                );
            }

            v
        };

        for path in candidates {
            if path.exists() {
                if let Ok(s) = std::fs::read_to_string(&path) {
                    if let Ok(cfg) = toml::from_str::<Config>(&s) {
                        return cfg;
                    }
                }
            }
        }

        // fallback: empty config (values will be resolved from defaults later)
        Config {
            log: None,
            database: None,
        }
    }

    /// Resolve runtime configuration values with precedence: config file -> env vars -> defaults.
    pub fn resolve_runtime(&self) -> RuntimeConfig {
        // database values
        let db_path = self
            .database
            .as_ref()
            .and_then(|d| d.db_path.clone())
            .unwrap_or_else(|| "sqllogs.duckdb".into());

        let chunk_size = self
            .database
            .as_ref()
            .and_then(|d| d.chunk_size)
            .unwrap_or(1000);

        let create_indexes = self
            .database
            .as_ref()
            .and_then(|d| d.create_indexes)
            .unwrap_or(true);

        // log values
        let enable_stdout = self
            .log
            .as_ref()
            .and_then(|l| l.enable_stdout)
            .unwrap_or(cfg!(debug_assertions));

        let log_dir = self.log.as_ref().and_then(|l| l.log_dir.clone());

        RuntimeConfig {
            db_path,
            chunk_size,
            create_indexes,
            enable_stdout,
            log_dir,
        }
    }
}
