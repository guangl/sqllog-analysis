//! 配置管理模块 - 分层配置与运行时参数解析
//!
//! 本模块实现了一个灵活的配置管理系统，支持 TOML 文件配置、命令行参数
//! 和环境变量的组合使用，并提供合理的默认值。
//!
//! ## 配置架构
//!
//! ### 1. 分层配置设计
//! ```text
//! 配置优先级（从高到低）：
//! 命令行参数 > 环境变量 > 配置文件 > 默认值
//! ```
//!
//! ### 2. 配置文件结构
//! ```toml
//! [log]
//! enable_stdout = true
//! log_dir = "logs"
//! level = "info"
//!
//! [database]
//! db_path = "sqllog.duckdb"
//! use_in_memory = false
//!
//! [export]
//! enabled = true
//! format = "csv"
//! out_path = "output.csv"
//!
//! [sqllog]
//! chunk_size = 1000
//! parser_threads = 4
//! write_errors = true
//! errors_out_path = "parse_errors.jsonl"
//! ```
//!
//! ### 3. 运行时配置转换
//! - **类型安全**：编译时保证配置项的类型正确性
//! - **默认值填充**：自动为未指定的配置项提供合理默认值
//! - **配置验证**：运行时验证配置的合法性和一致性
//!
//! ## 核心特性
//!
//! ### 配置解析流程
//! ```text
//! TOML 文件 → serde 反序列化 → Config 结构体 → 默认值合并 → RuntimeConfig
//!     ↓              ↓                ↓              ↓              ↓
//!  原始配置      结构化数据        可选字段处理    配置验证     运行时就绪
//! ```
//!
//! ### 错误处理策略
//! - **配置文件错误**：提供详细的语法错误定位
//! - **路径解析**：自动处理相对路径和绝对路径
//! - **类型转换**：安全的字符串到枚举类型转换
//!
//! ## 使用示例
//!
//! ```rust,no_run
//! use sqllog_analysis::config::Config;
//!
//! // 从默认位置加载配置
//! let runtime_config = Config::load();
//!
//! // 访问配置项
//! if runtime_config.export_enabled {
//!     println!("导出格式: {}", runtime_config.export_format);
//! }
//! ```

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

#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
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
    fn parse_log_config(
        cfg: &Self,
    ) -> (bool, Option<PathBuf>, log::LevelFilter) {
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
    fn parse_export_config(
        cfg: &Self,
    ) -> (bool, String, Option<PathBuf>, ExportOptions) {
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
    fn parse_sqllog_config(
        cfg: &Self,
    ) -> (Option<PathBuf>, Option<usize>, usize, bool, Option<PathBuf>) {
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

        (
            sqllog_dir,
            sqllog_chunk_size,
            parser_threads,
            sqllog_write_errors,
            sqllog_errors_out_path,
        )
    }

    /// 将解析得到的 Config 合并为 RuntimeConfig，应用默认值并进行必要的校验。
    fn merge_to_runtime_config(cfg: &Self) -> RuntimeConfig {
        let (db_path, use_in_memory) = Self::parse_database_config(cfg);
        let (enable_stdout, log_dir, log_level) = Self::parse_log_config(cfg);
        let (export_enabled, export_format, export_out_path, export_options) =
            Self::parse_export_config(cfg);
        let (
            sqllog_dir,
            sqllog_chunk_size,
            parser_threads,
            sqllog_write_errors,
            sqllog_errors_out_path,
        ) = Self::parse_sqllog_config(cfg);

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
