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
