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
    /// 最大并发线程数，0 表示自动根据文件数量确定
    pub thread_count: Option<usize>,
    /// 每个线程处理的批次大小，0 表示不分块直接解析整个文件
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
            thread_count: Some(0), // 默认为0，表示自动根据文件数量确定
            batch_size: 0,         // 默认为0，表示不分块直接解析整个文件
            queue_buffer_size: 10000,
        }
    }
}

impl Config {
    /// 从 TOML 文件加载配置
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        #[cfg(feature = "logging")]
        tracing::info!("开始加载配置文件: {}", path_ref.display());

        let content = std::fs::read_to_string(path_ref).map_err(|e| {
            #[cfg(feature = "logging")]
            tracing::error!(
                "读取配置文件失败: {}, 错误: {}",
                path_ref.display(),
                e
            );
            SqllogError::other(format!("读取配置文件失败: {}", e))
        })?;

        #[cfg(feature = "logging")]
        tracing::trace!("配置文件内容长度: {} 字节", content.len());

        let config: Config = toml::from_str(&content).map_err(|e| {
            #[cfg(feature = "logging")]
            tracing::error!(
                "解析配置文件失败: {}, 错误: {}",
                path_ref.display(),
                e
            );
            SqllogError::other(format!("解析配置文件失败: {}", e))
        })?;

        #[cfg(feature = "logging")]
        tracing::info!(
            "成功加载配置文件: {}, 日志级别: {}, 线程数: {:?}",
            path_ref.display(),
            config.log.level,
            config.sqllog.thread_count
        );

        Ok(config)
    }

    /// 保存配置到 TOML 文件
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path_ref = path.as_ref();
        #[cfg(feature = "logging")]
        tracing::info!("开始保存配置到文件: {}", path_ref.display());

        let content = toml::to_string_pretty(self).map_err(|e| {
            #[cfg(feature = "logging")]
            tracing::error!("序列化配置失败: {}", e);
            SqllogError::other(format!("序列化配置失败: {}", e))
        })?;

        #[cfg(feature = "logging")]
        tracing::trace!("序列化后的配置长度: {} 字节", content.len());

        std::fs::write(path_ref, content).map_err(|e| {
            #[cfg(feature = "logging")]
            tracing::error!(
                "写入配置文件失败: {}, 错误: {}",
                path_ref.display(),
                e
            );
            SqllogError::other(format!("写入配置文件失败: {}", e))
        })?;

        #[cfg(feature = "logging")]
        tracing::info!("成功保存配置到文件: {}", path_ref.display());

        Ok(())
    }
}
