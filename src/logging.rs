//! 日志初始化和配置模块
//!
//! 这个模块提供了统一的日志初始化功能，使用 tracing 库。
//! 默认配置：debug 级别，输出到控制台和 logs 目录，7天循环。

use std::io;
use std::sync::Once;
use tracing::Level;
use tracing_subscriber::{
    EnvFilter, Registry,
    fmt::{self, time::SystemTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

/// 日志配置结构体
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// 日志级别
    pub level: Level,
}

impl LogConfig {
    /// 创建新的日志配置，使用默认级别
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置日志级别
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self { level: Level::TRACE } // 改为TRACE级别以显示更详细的调试信息
    }
}

/// 自动初始化日志系统（仅初始化一次）
static INIT_LOGGER: Once = Once::new();

/// 确保日志系统已初始化
///
/// 这个函数会在首次调用时自动初始化日志系统，后续调用不会重复初始化
/// 如果初始化失败（比如已经初始化过），会安静地忽略错误
pub(crate) fn ensure_logger_initialized() {
    INIT_LOGGER.call_once(|| {
        // 忽略初始化错误，因为可能已经被其他地方初始化了
        let _ = init_default_logging();
    });
}
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("IO错误: {0}")]
    Io(#[from] io::Error),
    #[error("日志配置错误: {0}")]
    Config(String),
    #[error("日志初始化错误: {0}")]
    Init(String),
}

/// 日志初始化结果
pub type LogResult<T> = Result<T, LogError>;

/// 初始化日志系统
///
/// 使用固定配置：
/// - 默认输出到控制台和 logs 目录
/// - 7天循环日志文件
/// - debug 级别（可通过 config 调整）
/// - 简洁的格式化输出
///
/// # Arguments
///
/// * `config` - 日志配置（目前仅支持级别设置）
///
/// # Returns
///
/// 返回初始化结果
///
/// # Examples
///
/// ```no_run
/// use sqllog_analysis::logging::{init_logging, LogConfig};
/// use tracing::Level;
///
/// // 默认配置（DEBUG 级别）
/// let config = LogConfig::new();
/// init_logging(config).unwrap();
///
/// // 自定义级别
/// let config = LogConfig::new().level(Level::INFO);
/// init_logging(config).unwrap();
/// ```
pub fn init_logging(config: LogConfig) -> LogResult<()> {
    // 创建环境过滤器，默认使用配置的级别
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.level.to_string()));

    // 构建订阅者
    let subscriber = Registry::default().with(env_filter);

    // 控制台输出层
    let console_layer = fmt::layer()
        .with_timer(SystemTime)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_ansi(true);

    // 文件输出层 - 7天循环，输出到 logs 目录
    let file_appender = tracing_appender::rolling::daily("logs", "sqllog");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = fmt::layer()
        .with_writer(non_blocking)
        .with_timer(SystemTime)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_ansi(false); // 文件中不使用颜色

    // 尝试初始化，如果失败说明已经初始化过了
    match subscriber.with(console_layer).with(file_layer).try_init() {
        Ok(_) => {
            // 存储 guard 以防止 appender 被丢弃
            std::mem::forget(_guard);
            tracing::info!(
                "日志系统初始化完成 - 输出到控制台和 logs 目录，7天循环"
            );
            Ok(())
        }
        Err(_) => {
            // 已经初始化过了，这不是错误
            Ok(())
        }
    }
}

/// 使用默认配置初始化日志系统
///
/// 这是一个便捷函数，使用默认配置初始化日志系统。
/// 默认配置会输出 INFO 级别的日志到控制台。
///
/// # Examples
///
/// ```no_run
/// use sqllog_analysis::logging::init_default_logging;
///
/// init_default_logging().unwrap();
/// ```
pub fn init_default_logging() -> LogResult<()> {
    init_logging(LogConfig::default())
}
