use serde::Deserialize;
use std::{env, fs, path::PathBuf};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub log: Option<LogSection>,
    pub database: Option<DatabaseSection>,
    pub export: Option<ExportSection>,
    pub sqllog: Option<SqllogSection>,
}

#[derive(Debug, Deserialize)]
pub struct LogSection {
    pub enable_stdout: Option<bool>,
    pub log_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseSection {
    pub db_path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ExportSection {
    pub enabled: Option<bool>,
    pub format: Option<String>,
    pub out_path: Option<PathBuf>,
    pub per_thread_out: Option<bool>,
    pub overwrite_or_ignore: Option<bool>,
    pub overwrite: Option<bool>,
    pub append: Option<bool>,
    pub file_size_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct SqllogSection {
    pub sqllog_dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ExportOptions {
    pub per_thread_out: bool,
    pub write_flags: WriteFlags,
    pub file_size_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct WriteFlags {
    pub overwrite_or_ignore: bool,
    pub overwrite: bool,
    pub append: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub db_path: String,
    pub enable_stdout: bool,
    pub log_dir: Option<PathBuf>,
    pub sqllog_dir: Option<PathBuf>,
    pub export_enabled: bool,
    pub export_format: String,
    pub export_out_path: Option<PathBuf>,
    pub export_options: ExportOptions,
}

impl Config {
    #[must_use]
    pub fn load() -> RuntimeConfig {
        // Default empty config
        let mut cfg =
            Self { log: None, database: None, export: None, sqllog: None };

        // Try loading config from: $SQLLOG_CONFIG, ./config.toml, or config_dir()/sqllog-analysis/config.toml
        let config_path = (|| {
            if let Ok(p) = env::var("SQLLOG_CONFIG") {
                return Some(PathBuf::from(p));
            }
            if let Ok(cwd) = env::current_dir() {
                let p = cwd.join("config.toml");
                if p.exists() {
                    return Some(p);
                }
            }
            if let Some(cfg_dir) = dirs::config_dir() {
                let p = cfg_dir.join("sqllog-analysis").join("config.toml");
                if p.exists() {
                    return Some(p);
                }
            }
            None
        })();

        if let Some(path) = config_path {
            match fs::read_to_string(&path) {
                Ok(contents) => match toml::from_str::<Self>(&contents) {
                    Ok(parsed) => {
                        cfg = parsed;
                        log::info!("使用配置文件: {}", path.display());
                    }
                    Err(e) => {
                        log::error!(
                            "解析配置文件失败 {}: {}",
                            path.display(),
                            e
                        );
                        // treat parse errors as fatal: misconfigured input
                        std::process::exit(2);
                    }
                },
                Err(e) => {
                    log::warn!("读取配置文件失败 {}: {}", path.display(), e);
                }
            }
        } else {
            log::info!("未找到配置文件；使用默认运行时配置");
        }

        // Merge with defaults
        let db_path = cfg
            .database
            .as_ref()
            .and_then(|d| d.db_path.clone())
            .unwrap_or_else(|| "sqllogs.duckdb".into());

        let enable_stdout = cfg
            .log
            .as_ref()
            .and_then(|l| l.enable_stdout)
            .unwrap_or(cfg!(debug_assertions));

        let log_dir = cfg.log.as_ref().and_then(|l| l.log_dir.clone());

        let export_enabled =
            cfg.export.as_ref().and_then(|e| e.enabled).unwrap_or(false);

        let export_format = cfg
            .export
            .as_ref()
            .and_then(|e| e.format.clone())
            .unwrap_or_else(|| "csv".into());

        let export_out_path =
            cfg.export.as_ref().and_then(|e| e.out_path.clone());

        let export_per_thread_out =
            cfg.export.as_ref().and_then(|e| e.per_thread_out).unwrap_or(false);

        let export_overwrite_or_ignore = cfg
            .export
            .as_ref()
            .and_then(|e| e.overwrite_or_ignore)
            .unwrap_or(false);

        let export_overwrite =
            cfg.export.as_ref().and_then(|e| e.overwrite).unwrap_or(false);
        let export_append =
            cfg.export.as_ref().and_then(|e| e.append).unwrap_or(false);
        let export_file_size_bytes =
            cfg.export.as_ref().and_then(|e| e.file_size_bytes);

        let export_options = ExportOptions {
            per_thread_out: export_per_thread_out,
            write_flags: WriteFlags {
                overwrite_or_ignore: export_overwrite_or_ignore,
                overwrite: export_overwrite,
                append: export_append,
            },
            file_size_bytes: export_file_size_bytes,
        };

        let sqllog_dir = cfg
            .sqllog
            .as_ref()
            .and_then(|s| s.sqllog_dir.clone())
            .or_else(|| Some(PathBuf::from("sqllog")));

        RuntimeConfig {
            db_path,
            enable_stdout,
            log_dir,
            sqllog_dir,
            export_enabled,
            export_format,
            export_out_path,
            export_options,
        }
    }
}
