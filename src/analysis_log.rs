//! 日志功能模块
//!
//! 提供统一的日志初始化与参数解析接口（使用 `tracing` / `tracing-subscriber`），
//! 支持同时输出到终端和文件（按天命名），并尽量在初始化失败时给出友好的错误提示。

use chrono::Local;
use log::LevelFilter;
use tracing::info;
use std::fs::OpenOptions;
use std::path::PathBuf;
use tracing_appender::non_blocking;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

/// 日志配置参数
pub struct LogConfig {
    pub enabled: bool,
    pub level: LevelFilter,
    pub log_file: Option<PathBuf>,
}

impl LogConfig {
    /// 从命令行参数解析日志配置
    pub fn from_args<I: Iterator<Item = String>>(args: I) -> Self {
        let mut enabled = true;
        let mut level = LevelFilter::Info;
        let mut log_file: Option<PathBuf> = None;
        for arg in args {
            if arg == "--no-log" {
                enabled = false;
            } else if let Some(lvl) = arg.strip_prefix("--log-level=") {
                level = match lvl.to_lowercase().as_str() {
                    "error" => LevelFilter::Error,
                    "warn" => LevelFilter::Warn,
                    "debug" => LevelFilter::Debug,
                    "trace" => LevelFilter::Trace,
                    _ => LevelFilter::Info,
                };
            } else if let Some(path) = arg.strip_prefix("--log-file=") {
                log_file = Some(PathBuf::from(path));
            }
        }
        Self {
            enabled,
            level,
            log_file,
        }
    }

    /// 初始化日志（使用 `env_logger`）
    ///
    /// 说明：`env_logger` 不提供内置的文件轮换；如果需要轮换日志文件，建议使用 `flexi_logger` 或其他库。
    pub fn init(&self) {
        if !self.enabled {
            return;
        }

        // 使用 tracing_subscriber 初始化格式化与过滤
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(format!("{}", self.level)));

        // 默认：如果没有传入 --log-file，则使用当前工作目录下的 `logs` 目录
        let dir = self.log_file.as_ref().map_or_else(
            || {
                let mut p = match std::env::current_dir() {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("无法获取当前工作目录，使用 '.' 作为基准: {e}");
                        PathBuf::from(".")
                    }
                };
                p.push("logs");
                // 如果目录不存在，尝试创建
                if let Err(e) = std::fs::create_dir_all(&p) {
                    let p_display = p.display();
                    eprintln!("无法创建日志目录 {p_display}: {e}");
                }
                p
            },
            Clone::clone,
        );

        // 构建精确文件名 sqllog-analysis-YYYY-MM-DD.log
        let date = Local::now().format("%Y-%m-%d").to_string();
        let filename = format!("sqllog-analysis-{date}.log");
        let file_path = dir.join(filename);

        // 打开（创建并追加）文件
        let file = match OpenOptions::new().create(true).append(true).open(&file_path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("无法创建日志文件 {}: {e}", file_path.display());
                return;
            }
        };

        let (non_blocking, guard) = non_blocking::NonBlocking::new(file);
        // 将 guard 泄漏为静态引用以保证其在程序生命周期内存活，从而
        // 可以安全地使用 non_blocking writer（简洁且比 std::mem::forget 更明确）。
        let _guard_ref: &'static _ = Box::leak(Box::new(guard));

        // 创建两个输出层：stdout 层与文件层，注册到全局 subscriber
        let stdout_layer = fmt::layer()
            .with_writer(std::io::stdout)
            .with_filter(filter.clone());
        let file_layer = fmt::layer().with_writer(non_blocking).with_filter(filter);

        tracing_subscriber::registry()
            .with(stdout_layer)
            .with(file_layer)
            .init();

        info!("日志功能已启用，等级: {:?}", self.level);
    }
}
