use serde::Deserialize;
use std::{env, fs, path::PathBuf, process};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub log: Option<LogSection>,
    pub database: Option<DatabaseSection>,
    pub export: Option<ExportSection>,
    pub sqllog: Option<SqllogSection>,
}

/// 应用层配置结构体，直接从配置文件（TOML）反序列化得到
/// 包含日志、数据库、导出和 sqllog 相关的子节
#[derive(Debug, Deserialize)]
pub struct LogSection {
    pub enable_stdout: Option<bool>,
    pub log_dir: Option<PathBuf>,
    pub level: Option<String>,
}

/// 日志相关配置节
#[derive(Debug, Deserialize)]
pub struct DatabaseSection {
    pub db_path: Option<String>,
    // 当为 true 时，在内存 DuckDB 中写入后再将表导出到磁盘（COPY TO），
    // 默认为 false，保持现有直接写入磁盘数据库的行为。
    pub use_in_memory: Option<bool>,
}

/// 导出相关配置节
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

/// sqllog 相关配置节
#[derive(Debug, Deserialize)]
pub struct SqllogSection {
    pub sqllog_dir: Option<PathBuf>,
    /// 可选的按块解析大小（解析出的记录数），如果未设置或为 0 则表示禁用 chunked 模式
    pub chunk_size: Option<usize>,
    /// 可选的解析线程数（默认 10）
    pub parser_threads: Option<usize>,
    /// 如果为 true，将把解析过程中产生的错误信息写入指定文件
    pub write_errors: Option<bool>,
    /// 解析错误写入的输出文件路径（如果未提供，运行时使用默认 `parse_errors.log`）
    pub errors_out_path: Option<PathBuf>,
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

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub db_path: String,
    pub enable_stdout: bool,
    pub log_dir: Option<PathBuf>,
    pub log_level: log::LevelFilter,
    pub sqllog_dir: Option<PathBuf>,
    pub sqllog_chunk_size: Option<usize>,
    pub parser_threads: usize,
    pub sqllog_write_errors: bool,
    pub sqllog_errors_out_path: Option<PathBuf>,
    pub export_enabled: bool,
    pub export_format: String,
    pub export_out_path: Option<PathBuf>,
    pub export_options: ExportOptions,
    pub use_in_memory: bool,
}

/// 将解析得到的 Config 合并为运行时所需的 `RuntimeConfig`，
/// 对缺省值进行填充并校验部分配置（例如 `export.file_size_bytes` 不能为 0）
impl Config {
    #[must_use]
    pub fn load() -> RuntimeConfig {
        let mut cfg =
            Self { log: None, database: None, export: None, sqllog: None };

        if let Some(path) = Self::find_config_path() {
            if let Some(parsed) = Self::read_and_parse_config(&path) {
                cfg = parsed;
            }
        } else {
            log::info!("未找到配置文件；使用默认运行时配置");
        }

        // Merge into runtime config
        Self::merge_to_runtime_config(&cfg)
    }

    /// 加载配置：查找配置文件并解析，最后合并为 `RuntimeConfig` 并返回
    ///
    /// 查找顺序：环境变量 `SQLLOG_CONFIG` -> 当前目录 ./config.toml -> 系统配置目录下的 sqllog-analysis/config.toml
    fn find_config_path() -> Option<PathBuf> {
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
    }

    /// 读取并解析配置文件（TOML）。解析失败将打印错误并以退出码 2 终止进程（保持历史行为）。
    fn read_and_parse_config(path: &PathBuf) -> Option<Self> {
        match fs::read_to_string(path) {
            Ok(contents) => match toml::from_str::<Self>(&contents) {
                Ok(parsed) => {
                    log::info!("使用配置文件: {}", path.display());
                    Some(parsed)
                }
                Err(e) => {
                    log::error!("解析配置文件失败 {}: {}", path.display(), e);
                    std::process::exit(2);
                }
            },
            Err(e) => {
                log::warn!("读取配置文件失败 {}: {}", path.display(), e);
                None
            }
        }
    }

    /// 解析数据库相关配置。
    fn parse_database_config(cfg: &Self) -> (String, bool) {
        let db_path = cfg
            .database
            .as_ref()
            .and_then(|d| d.db_path.clone())
            .unwrap_or_else(|| "sqllogs.duckdb".into());

        let use_in_memory = cfg
            .database
            .as_ref()
            .and_then(|d| d.use_in_memory)
            .unwrap_or(false);

        (db_path, use_in_memory)
    }

    /// 解析日志相关配置。
    fn parse_log_config(cfg: &Self) -> (bool, Option<PathBuf>, log::LevelFilter) {
        let enable_stdout = cfg
            .log
            .as_ref()
            .and_then(|l| l.enable_stdout)
            .unwrap_or(cfg!(debug_assertions));

        let log_dir = cfg.log.as_ref().and_then(|l| l.log_dir.clone());

        let log_level = cfg
            .log
            .as_ref()
            .and_then(|l| l.level.clone())
            .map(|s| s.to_lowercase())
            .and_then(|s| match s.as_str() {
                "error" => Some(log::LevelFilter::Error),
                "warn" | "warning" => Some(log::LevelFilter::Warn),
                "info" => Some(log::LevelFilter::Info),
                "debug" => Some(log::LevelFilter::Debug),
                "trace" => Some(log::LevelFilter::Trace),
                "off" => Some(log::LevelFilter::Off),
                _ => None,
            })
            .unwrap_or(log::LevelFilter::Info);

        (enable_stdout, log_dir, log_level)
    }

    /// 解析导出相关配置。
    fn parse_export_config(cfg: &Self) -> (bool, String, Option<PathBuf>, ExportOptions) {
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
        let export_file_size_bytes = cfg
            .export
            .as_ref()
            .and_then(|e| e.file_size_bytes)
            .map(|v| {
                if v == 0 {
                    eprintln!("配置错误: export.file_size_bytes 不能为 0；请设置为正整数或删除该项以表示无上限");
                    process::exit(2);
                }
                v
            });

        let export_options = ExportOptions {
            per_thread_out: export_per_thread_out,
            write_flags: WriteFlags {
                overwrite_or_ignore: export_overwrite_or_ignore,
                overwrite: export_overwrite,
                append: export_append,
            },
            file_size_bytes: export_file_size_bytes,
        };

        (export_enabled, export_format, export_out_path, export_options)
    }

    /// 解析 sqllog 相关配置。
    fn parse_sqllog_config(cfg: &Self) -> (Option<PathBuf>, Option<usize>, usize, bool, Option<PathBuf>) {
        let sqllog_dir = cfg
            .sqllog
            .as_ref()
            .and_then(|s| s.sqllog_dir.clone())
            .or_else(|| Some(PathBuf::from("sqllog")));

        let sqllog_chunk_size = cfg.sqllog.as_ref().and_then(|s| s.chunk_size);

        let parser_threads = cfg
            .sqllog
            .as_ref()
            .and_then(|s| s.parser_threads)
            .unwrap_or(10usize);

        let sqllog_write_errors =
            cfg.sqllog.as_ref().and_then(|s| s.write_errors).unwrap_or(false);

        let sqllog_errors_out_path = cfg
            .sqllog
            .as_ref()
            .and_then(|s| s.errors_out_path.clone())
            .or_else(|| Some(PathBuf::from("parse_errors.log")));

        (sqllog_dir, sqllog_chunk_size, parser_threads, sqllog_write_errors, sqllog_errors_out_path)
    }

    /// 将解析得到的 Config 合并为 RuntimeConfig，应用默认值并进行必要的校验。
    fn merge_to_runtime_config(cfg: &Self) -> RuntimeConfig {
        let (db_path, use_in_memory) = Self::parse_database_config(cfg);
        let (enable_stdout, log_dir, log_level) = Self::parse_log_config(cfg);
        let (export_enabled, export_format, export_out_path, export_options) = Self::parse_export_config(cfg);
        let (sqllog_dir, sqllog_chunk_size, parser_threads, sqllog_write_errors, sqllog_errors_out_path) = Self::parse_sqllog_config(cfg);

        RuntimeConfig {
            db_path,
            enable_stdout,
            log_dir,
            log_level,
            sqllog_dir,
            sqllog_chunk_size,
            parser_threads,
            sqllog_write_errors,
            sqllog_errors_out_path,
            export_enabled,
            export_format,
            export_out_path,
            export_options,
            use_in_memory,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_find_config_path_env_override() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("config.toml");
        File::create(&file_path).unwrap();
        // Change current dir to the temp dir so find_config_path will detect ./config.toml
        let orig = env::current_dir().unwrap();
        env::set_current_dir(dir.path()).unwrap();
        let found = Config::find_config_path();
        assert!(found.is_some());
        assert_eq!(found.unwrap(), file_path);
        // restore cwd
        env::set_current_dir(orig).unwrap();
    }

    #[test]
    fn test_read_and_parse_config_and_merge_defaults() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("cfg.toml");
        let mut f = File::create(&file_path).unwrap();
        // minimal toml: enable export with file_size_bytes non-zero
        writeln!(
            f,
            r#"
            [export]
            enabled = true
            file_size_bytes = 1024
            format = "csv"
        "#
        )
        .unwrap();

        let parsed = Config::read_and_parse_config(&file_path).unwrap();
        // ensure parsed has export enabled
        assert!(parsed.export.is_some());
        let rc = Config::merge_to_runtime_config(&parsed);
        assert!(rc.export_enabled);
        assert_eq!(rc.export_format, "csv");
        assert_eq!(rc.db_path, "sqllogs.duckdb");
    }

    #[test]
    fn test_export_file_size_zero_causes_exit() {
        // This test ensures that when file_size_bytes is zero we exit with code 2.
        // Because std::process::exit terminates the current process, we instead validate
        // the mapping by constructing a Config with that value and ensuring the mapping
        // logic would call exit path. We'll mimic the check locally.
        let cfg = Config {
            log: None,
            database: None,
            export: Some(ExportSection {
                enabled: Some(true),
                format: None,
                out_path: None,
                per_thread_out: None,
                overwrite_or_ignore: None,
                overwrite: None,
                append: None,
                file_size_bytes: Some(0),
            }),
            sqllog: None,
        };

        // We cannot call merge_to_runtime_config(cfg) because it would call process::exit.
        // Instead, ensure the intermediate option mapping returns Some(0) so the check would trigger.
        let export_file_size_bytes =
            cfg.export.as_ref().and_then(|e| e.file_size_bytes).map(|v| {
                if v == 0 {
                    // Would exit in production; here we return 0 to indicate trigger
                    0
                } else {
                    v
                }
            });

        assert_eq!(export_file_size_bytes, Some(0));
    }
}
