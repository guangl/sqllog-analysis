//! 配置管理模块
//!
//! 提供统一的配置文件读取和管理功能

use crate::error::{Result, SqllogError};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// 主配置结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// 日志配置
    pub log: LogConfig,
    /// SQL 日志解析配置
    pub sqllog: SqllogConfig,
    /// 导出配置
    pub export: ExportConfig,
}

/// 日志配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// 是否启用控制台输出
    pub enable_stdout: bool,
    /// 日志输出目录
    pub log_dir: String,
    /// 日志级别 (trace, debug, info, warn, error)
    pub level: String,
}

/// SQL 日志解析配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqllogConfig {
    /// SQL 日志文件目录
    pub sqllog_dir: String,
    /// 分块大小，0表示不分块
    pub chunk_size: usize,
    /// 是否写入错误文件
    pub write_errors: bool,
    /// 并发线程数
    pub thread_count: usize,
    /// 错误输出文件路径
    pub errors_out_path: String,
}

/// 导出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportConfig {
    /// CSV 导出配置
    #[serde(default)]
    pub csv: Vec<CsvConfig>,
    /// Excel 导出配置
    #[serde(default)]
    pub excel: Vec<ExcelConfig>,
    /// JSON 导出配置
    #[serde(default)]
    pub json: Vec<JsonConfig>,
    /// SQLite 导出配置
    #[serde(default)]
    pub sqlite: Vec<SqliteConfig>,
    /// DuckDB 导出配置
    #[serde(default)]
    pub duckdb: Vec<DuckDbConfig>,
    /// PostgreSQL 导出配置
    #[serde(default)]
    pub postgres: Vec<PostgresConfig>,
    /// Oracle 导出配置
    #[serde(default)]
    pub oracle: Vec<OracleConfig>,
}

/// CSV 导出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvConfig {
    /// 输出文件路径
    pub out_path: String,
    /// 是否覆盖现有文件
    pub overwrite: bool,
    /// 是否追加到现有文件
    pub append: bool,
    /// 文件大小限制（字节），0表示无限制
    pub file_size_bytes: u64,
}

/// Excel 导出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExcelConfig {
    /// 输出文件路径
    pub out_path: String,
    /// 是否覆盖现有文件
    pub overwrite: bool,
    /// 是否追加到现有文件
    pub append: bool,
    /// 文件大小限制（字节），0表示无限制
    pub file_size_bytes: u64,
}

/// JSON 导出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonConfig {
    /// 输出文件路径
    pub out_path: String,
    /// 是否覆盖现有文件
    pub overwrite: bool,
    /// 是否追加到现有文件
    pub append: bool,
    /// 文件大小限制（字节），0表示无限制
    pub file_size_bytes: u64,
}

/// SQLite 导出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqliteConfig {
    /// 数据库文件路径
    pub out_path: String,
    /// 表名
    pub table_name: String,
    /// 是否覆盖现有文件
    pub overwrite: bool,
    /// 是否追加到现有表
    pub append: bool,
}

/// DuckDB 导出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuckDbConfig {
    /// 数据库文件路径
    pub out_path: String,
    /// 表名
    pub table_name: String,
    /// 是否覆盖现有文件
    pub overwrite: bool,
    /// 是否追加到现有表
    pub append: bool,
}

/// PostgreSQL 导出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// 主机地址
    pub host: String,
    /// 端口号
    pub port: u16,
    /// 用户名
    pub username: String,
    /// 密码
    pub password: String,
    /// 数据库名
    pub database: String,
    /// 表名
    pub table_name: String,
    /// 是否追加到现有表
    pub append: bool,
}

/// Oracle 导出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleConfig {
    /// 主机地址
    pub host: String,
    /// 端口号
    pub port: u16,
    /// 服务名
    pub service_name: String,
    /// 用户名
    pub username: String,
    /// 密码
    pub password: String,
    /// 表名
    pub table_name: String,
    /// 是否追加到现有表
    pub append: bool,
}

impl Config {
    /// 从文件加载配置
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// 从字符串加载配置
    pub fn from_str(content: &str) -> Result<Self> {
        let config: Config = toml::from_str(content)?;
        config.validate()?;
        Ok(config)
    }

    /// 保存配置到文件
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// 验证配置的有效性
    pub fn validate(&self) -> Result<()> {
        // 验证日志级别
        match self.log.level.as_str() {
            "trace" | "debug" | "info" | "warn" | "error" => {}
            _ => {
                return Err(SqllogError::config(format!(
                    "无效的日志级别: {}",
                    self.log.level
                )));
            }
        }

        // 验证线程数
        if self.sqllog.thread_count == 0 {
            return Err(SqllogError::config("线程数不能为0"));
        }

        // 验证导出配置
        if self.export.csv.is_empty()
            && self.export.excel.is_empty()
            && self.export.json.is_empty()
            && self.export.sqlite.is_empty()
            && self.export.duckdb.is_empty()
            && self.export.postgres.is_empty()
            && self.export.oracle.is_empty()
        {
            log::warn!("没有配置任何导出格式");
        }

        Ok(())
    }

    /// 获取默认配置
    pub fn default() -> Self {
        Self {
            log: LogConfig {
                enable_stdout: true,
                log_dir: "logs".to_string(),
                level: "info".to_string(),
            },
            sqllog: SqllogConfig {
                sqllog_dir: "sqllog".to_string(),
                chunk_size: 1000,
                write_errors: true,
                thread_count: std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(4),
                errors_out_path: "parse_errors.jsonl".to_string(),
            },
            export: ExportConfig {
                csv: vec![],
                excel: vec![],
                json: vec![],
                sqlite: vec![],
                duckdb: vec![],
                postgres: vec![],
                oracle: vec![],
            },
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::default()
    }
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            csv: vec![],
            excel: vec![],
            json: vec![],
            sqlite: vec![],
            duckdb: vec![],
            postgres: vec![],
            oracle: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        // 测试无效日志级别
        config.log.level = "invalid".to_string();
        assert!(config.validate().is_err());

        // 测试线程数为0
        config.log.level = "info".to_string();
        config.sqllog.thread_count = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let toml_str = toml::to_string(&config).unwrap();
        let parsed_config: Config = toml::from_str(&toml_str).unwrap();
        assert_eq!(config.log.level, parsed_config.log.level);
    }
}