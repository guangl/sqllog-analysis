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
    /// 最大并发线程数，默认为文件数量
    pub thread_count: Option<usize>,
    /// 每个线程处理的批次大小
    pub batch_size: usize,
    /// 任务队列缓冲大小
    pub queue_buffer_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self { log: LogConfig::default(), sqllog: SqllogConfig::default() }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            enable_stdout: true,
            log_dir: "logs".to_string(),
            level: "info".to_string(),
        }
    }
}

impl Default for SqllogConfig {
    fn default() -> Self {
        Self {
            thread_count: None, // 默认为文件数量
            batch_size: 1000,
            queue_buffer_size: 10000,
        }
    }
}

impl Config {
    /// 从 TOML 文件加载配置
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            SqllogError::other(format!("读取配置文件失败: {}", e))
        })?;

        let config: Config = toml::from_str(&content).map_err(|e| {
            SqllogError::other(format!("解析配置文件失败: {}", e))
        })?;

        Ok(config)
    }

    /// 保存配置到 TOML 文件
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self).map_err(|e| {
            SqllogError::other(format!("序列化配置失败: {}", e))
        })?;

        std::fs::write(path, content).map_err(|e| {
            SqllogError::other(format!("写入配置文件失败: {}", e))
        })?;

        Ok(())
    }
}
