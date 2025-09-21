use chrono::Local;
use lazy_static::lazy_static;
use log::LevelFilter;
use std::sync::Mutex;
use std::{env, fs, fs::OpenOptions, io, path::PathBuf};
use tracing::info;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

lazy_static! {
    // 保持 guard 在程序生命周期内，退出时可以 take() 来触发 flush/drop
    static ref LOG_GUARD: Mutex<Option<WorkerGuard>> = Mutex::new(None);
}

/// 日志配置参数
pub struct LogConfig {
    pub enabled: bool,
    pub level: LevelFilter,
    pub log_file: Option<PathBuf>,
    pub enable_stdout: bool,
}

impl Default for LogConfig {
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
    // 日志配置逻辑：当前通过 `Default` 提供默认配置

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

        let dir = self.log_file.as_ref().map_or_else(
            || {
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
                // 如果目录不存在，尝试创建
                if let Err(e) = fs::create_dir_all(&p) {
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
        let file =
            match OpenOptions::new().create(true).append(true).open(&file_path)
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("无法创建日志文件 {}: {e}", file_path.display());
                    return;
                }
            };

        let (non_blocking, guard) = NonBlocking::new(file);
        // 将 guard 存入全局可控位置，以便在退出时可显式 drop（触发 flush）
        if let Ok(mut g) = LOG_GUARD.lock() {
            *g = Some(guard);
        }

        // 创建输出层：文件层始终启用；stdout 层使用配置中的值（若指定），否则在 debug 构建启用，在 release 构建关闭。
        let stdout_filter = if self.enable_stdout {
            filter.clone()
        } else {
            EnvFilter::new("off")
        };

        let stdout_layer =
            fmt::layer().with_writer(io::stdout).with_filter(stdout_filter);
        let file_layer =
            fmt::layer().with_writer(non_blocking).with_filter(filter);

        tracing_subscriber::registry()
            .with(stdout_layer)
            .with(file_layer)
            .init();

        if self.enable_stdout {
            info!("日志功能已启用（stdout + file），等级: {:?}", self.level);
        } else {
            info!("日志功能已启用（仅文件），等级: {:?}", self.level);
        }
    }
}
