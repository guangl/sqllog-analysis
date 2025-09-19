//! 日志功能模块
//!
//! 提供统一的日志初始化与参数解析接口。

use chrono::Local;
use flexi_logger::{Cleanup, Criterion, Duplicate, FileSpec, Logger, Naming};
use log::{LevelFilter, info};

/// 日志配置参数
pub struct LogConfig {
    pub enabled: bool,
    pub level: LevelFilter,
}

impl LogConfig {
    /// 从命令行参数解析日志配置
    pub fn from_args<I: Iterator<Item = String>>(args: I) -> Self {
        let mut enabled = true;
        let mut level = LevelFilter::Info;
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
            }
        }
        Self { enabled, level }
    }

    /// `初始化日志（flexi_logger），支持日志轮换`
    pub fn init(&self) {
        if self.enabled {
            let date = Local::now().format("%Y-%m-%d").to_string();
            let logger = Logger::try_with_str("info")
                .unwrap()
                .log_to_file(
                    FileSpec::default()
                        .directory("logs")
                        .basename(format!("sqllog-analysis-{date}")),
                )
                .format_for_files(flexi_logger::detailed_format)
                .duplicate_to_stdout(Duplicate::Trace)
                .format_for_stdout(flexi_logger::detailed_format)
                .rotate(
                    Criterion::Size(10_000_000), // 单文件最大 10MB
                    Naming::Timestamps,          // 文件名带时间戳，无 _rCURRENT 后缀
                    Cleanup::KeepLogFiles(7),    // 最多保留 7 个日志文件
                )
                .append(); // 追加模式

            // 可选：按天轮换
            // .rotate(Criterion::Age(Age::Day), Naming::Timestamps, Cleanup::KeepLogFiles(7))

            logger.start().unwrap();
            info!("日志功能已启用（轮换），等级: {}", self.level);
        }
    }
}
