// 分析日志模块 - 负责配置和初始化应用程序的日志记录功能
//
// 该模块提供了灵活的日志配置，支持：
// - 文件日志记录（自动按日期命名）
// - 控制台输出（可选）
// - 可配置的日志等级
// - 异步非阻塞写入（提高性能）

use chrono::Local;
use lazy_static::lazy_static;
use log::LevelFilter;
use std::sync::Mutex;
use std::{env, fs, fs::OpenOptions, io, path::PathBuf};
use tracing::info;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

lazy_static! {
    /// 全局日志守护者，用于确保日志工作线程在程序退出时正确清理
    /// 保持 guard 在程序生命周期内，退出时可以 take() 来触发 flush/drop
    static ref LOG_GUARD: Mutex<Option<WorkerGuard>> = Mutex::new(None);
}

/// 日志配置参数结构体
///
/// 控制应用程序的日志行为，包括输出目标、日志等级等设置。
pub struct LogConfig {
    /// 是否启用日志功能
    pub enabled: bool,
    /// 日志等级过滤器（Error/Warn/Info/Debug/Trace）
    pub level: LevelFilter,
    /// 日志文件路径配置（可以是文件路径或目录名）
    pub log_file: Option<PathBuf>,
    /// 是否同时在控制台输出日志
    pub enable_stdout: bool,
}

impl Default for LogConfig {
    /// 提供默认的日志配置
    ///
    /// - 启用日志记录
    /// - 日志等级为 Info
    /// - 默认日志目录为 "sqllog"
    /// - 关闭控制台输出（仅文件记录）
    fn default() -> Self {
        Self {
            enabled: true,
            level: LevelFilter::Info,
            log_file: Some("sqllog".into()),
            enable_stdout: false,
        }
    }
}

impl LogConfig {
    /// 初始化日志系统（使用 `tracing_subscriber`）
    ///
    /// 该方法配置并初始化异步日志系统，支持同时写入文件和控制台。
    /// 日志文件按日期自动命名（格式：sqllog-analysis-YYYY-MM-DD.log）。
    ///
    /// 日志目录/文件路径解析规则：
    /// - 如果 `log_file` 为 None，使用当前目录下的 `logs` 文件夹
    /// - 如果 `log_file` 包含文件扩展名，视为完整文件路径
    /// - 如果 `log_file` 不含扩展名，视为目录名，在其下创建按日期命名的文件
    ///
    /// # Errors
    ///
    /// 当无法创建日志目录或打开日志文件时，会返回 `io::Error`。
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = LogConfig {
    ///     enabled: true,
    ///     level: LevelFilter::Info,
    ///     log_file: Some(PathBuf::from("logs")),
    ///     enable_stdout: true,
    /// };
    /// config.init()?;
    /// ```
    pub fn init(&self) -> io::Result<()> {
        if !self.enabled {
            return Ok(());
        }

        // 创建环境变量过滤器，如果环境变量未设置则使用配置中的日志等级
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(format!("{}", self.level)));

        // 根据配置确定最终的日志文件路径
        // 支持三种配置方式：
        // 1. None - 使用默认的 cwd/logs 目录
        // 2. 带扩展名的路径 - 直接作为文件路径
        // 3. 不带扩展名的路径 - 作为目录，在其下创建按日期命名的文件
        let file_path = self.log_file.as_ref().map_or_else(
            || {
                // 情况 1：使用默认路径 (cwd/logs/sqllog-analysis-YYYY-MM-DD.log)
                let mut p = match env::current_dir() {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!(
                            "无法获取当前工作目录，使用 '.' 作为基准: {e}"
                        );
                        PathBuf::from(".")
                    }
                };
                p.push("logs");
                let date = Local::now().format("%Y-%m-%d").to_string();
                let filename = format!("sqllog-analysis-{date}.log");
                p.join(filename)
            },
            |p| {
                if p.extension().is_some() {
                    // 情况 2：直接作为完整文件路径使用
                    p.clone()
                } else {
                    // 情况 3：作为目录名，在其下创建按日期命名的文件
                    let dir = p.clone();
                    let date = Local::now().format("%Y-%m-%d").to_string();
                    let filename = format!("sqllog-analysis-{date}.log");
                    dir.join(filename)
                }
            },
        );

        // 确保日志文件的父目录存在，失败则早期返回错误
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // 以追加模式打开日志文件（如果不存在则创建）
        let file =
            OpenOptions::new().create(true).append(true).open(&file_path)?;

        // 创建非阻塞写入器，提高日志性能
        let (non_blocking, guard) = NonBlocking::new(file);

        // 将守护者存储到全局变量，确保程序退出时正确清理
        if let Ok(mut g) = LOG_GUARD.lock() {
            *g = Some(guard);
        }

        // 配置控制台输出层的过滤器
        let stdout_filter = if self.enable_stdout {
            filter.clone()
        } else {
            EnvFilter::new("off") // 关闭控制台输出
        };

        // 创建输出层：
        // - stdout_layer: 控制台输出层（根据配置启用/禁用）
        // - file_layer: 文件输出层（始终启用）
        // 配置格式包含：时间戳、日志级别、目标模块、文件名:行号、函数名、消息内容
        let stdout_layer = fmt::layer()
            .with_writer(io::stdout)
            .with_target(true) // 显示目标模块
            .with_file(true) // 显示文件名
            .with_line_number(true) // 显示行号
            .with_level(true) // 显示日志级别
            .with_thread_ids(false) // 不显示线程ID（避免输出过于冗长）
            .with_thread_names(false)
            .compact() // 使用紧凑格式
            .with_filter(stdout_filter);

        let file_layer = fmt::layer()
            .with_writer(non_blocking)
            .with_target(true) // 显示目标模块
            .with_file(true) // 显示文件名
            .with_line_number(true) // 显示行号
            .with_level(true) // 显示日志级别
            .with_thread_ids(true) // 文件中显示线程ID
            .with_thread_names(true)
            .compact() // 使用紧凑格式
            .with_filter(filter);

        // 初始化 tracing-log 兼容性层，使 log crate 的消息能被 tracing 处理
        // 需要在 registry 初始化之前设置
        if let Err(e) = tracing_log::LogTracer::init() {
            // 如果已经初始化过，忽略错误（可能在测试中会发生）
            eprintln!("警告: log 兼容性层初始化失败: {e}");
        }

        // 注册并初始化 tracing 订阅器
        // 使用 try_init() 来避免重复初始化问题
        if let Err(e) = tracing_subscriber::registry()
            .with(stdout_layer)
            .with(file_layer)
            .try_init()
        {
            // 如果已经初始化过（比如在测试中），只打印警告而不返回错误
            eprintln!("警告: tracing subscriber 已经初始化: {e}");
        }

        // 记录初始化成功信息
        if self.enable_stdout {
            info!("日志功能已启用（stdout + file），等级: {:?}", self.level);
        } else {
            info!("日志功能已启用（仅文件），等级: {:?}", self.level);
        }

        Ok(())
    }
}
