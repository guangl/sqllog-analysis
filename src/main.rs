mod analysis_log;
mod app;

use analysis_log::LogConfig;
use anyhow::Result;
use sqllog_analysis::config::{Config, RuntimeConfig};
use std::{
    backtrace::Backtrace,
    panic, process,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

fn main() -> Result<()> {
    let runtime = load_runtime_config();

    init_logging(&runtime);

    set_panic_hook();

    // 停止标志，用于在接收到 Ctrl-C 时通知正在执行的任务优雅终止
    let stop = Arc::new(AtomicBool::new(false));
    register_ctrlc_handler(&stop);

    // 将停止标志传入 app::run（按引用传递以避免不必要的移动）
    app::run(&stop)
}

// 载入运行时配置
fn load_runtime_config() -> RuntimeConfig {
    // Config::load 现在直接返回解析后的运行时配置 (RuntimeConfig)
    Config::load()
}

// 初始化日志系统
fn init_logging(runtime: &RuntimeConfig) {
    let log_config = LogConfig {
        enable_stdout: runtime.enable_stdout,
        log_file: runtime.log_dir.clone(),
        ..Default::default()
    };
    log_config.init();
}

// 设置全局 panic hook（只记录，不强制退出）
fn set_panic_hook() {
    panic::set_hook(Box::new(|info| {
        if let Some(s) = info.payload().downcast_ref::<&str>() {
            log::error!("发生 panic：{s}");
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            log::error!("发生 panic：{s}");
        } else {
            log::error!("发生 panic：{info}");
        }
        let bt = Backtrace::force_capture();
        log::error!("回溯信息:\n{bt:?}");
    }));
}

// 注册 Ctrl-C 处理器；在无法注册时直接退出进程
fn register_ctrlc_handler(stop: &Arc<AtomicBool>) {
    if let Err(e) = ctrlc::set_handler({
        let stop = stop.clone();
        move || {
            log::info!("收到中断信号，准备退出（设置停止标志）");
            stop.store(true, Ordering::SeqCst);
        }
    }) {
        log::error!("无法注册 Ctrl-C 处理器: {e}");
        // 注册失败属于严重错误，直接退出
        process::exit(2);
    }
}
