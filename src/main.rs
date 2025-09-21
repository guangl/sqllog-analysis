mod analysis_log;
mod app;

use analysis_log::LogConfig;
use sqllog_analysis::config::{Config, RuntimeConfig};
use std::{
    backtrace::Backtrace,
    io, panic, process,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

fn main() {
    let runtime = load_runtime_config();

    init_logging(&runtime);

    set_panic_hook();

    // 停止标志，用于在接收到 Ctrl-C 时通知正在执行的任务优雅终止
    let stop = Arc::new(AtomicBool::new(false));
    register_ctrlc_handler(&stop);

    // 将停止标志传入 app::run（按引用传递以避免不必要的移动）
    // 同时在后台监听 stdin 中的 stop 指令（交互式环境下）
    register_stop_command_listener(&stop);

    app::run(&stop);
}

/// 在后台线程中监听标准输入中的命令；当收到单行命令 `stop` 时，设置停止标志。
///
/// 说明：该函数会在交互式环境中启动一个线程持续读取 stdin，遇到 `stop`（不区分大小写）时将停止标志设为 true。
/// 在非交互式环境（stdin 不可读）时直接返回，不会启动线程。
fn register_stop_command_listener(stop: &Arc<AtomicBool>) {
    // 尝试克隆 stdin 的句柄；在某些运行环境下 stdin 可能不可用
    if atty::isnt(atty::Stream::Stdin) {
        // 非交互式环境，跳过监听
        return;
    }

    let stop = stop.clone();
    thread::spawn(move || {
        let stdin = io::stdin();
        let mut buf = String::new();
        while stdin.read_line(&mut buf).is_ok() {
            let cmd = buf.trim().to_lowercase();
            buf.clear();
            if cmd == "stop" {
                log::info!("收到 stop 指令（stdin），设置停止标志");
                stop.store(true, Ordering::SeqCst);
                break;
            }
            // 允许继续监听其它命令或空行
        }
    });
}

/// 载入运行时配置。
///
/// 目前直接调用 `Config::load()` 并返回 `RuntimeConfig`。
fn load_runtime_config() -> RuntimeConfig {
    Config::load()
}

/// 初始化日志系统并在初始化失败时退出进程。
///
/// 参数：
/// - `runtime`：运行时配置，包含日志级别、是否输出到 stdout、日志目录等。
fn init_logging(runtime: &RuntimeConfig) {
    let log_config = LogConfig {
        enable_stdout: runtime.enable_stdout,
        log_file: runtime.log_dir.clone(),
        level: runtime.log_level,
        ..Default::default()
    };
    // 在初始化日志之前先打印当前日志相关配置（便于在 enable_stdout=false 时也能看到等级）
    println!(
        "日志等级配置: {:?}, stdout: {}",
        log_config.level, log_config.enable_stdout
    );
    if let Err(e) = log_config.init() {
        eprintln!("日志初始化失败: {e}");
        // 无法初始化日志属于严重错误，退出
        process::exit(2);
    }
}

/// 设置全局 panic hook，用于在 panic 时记录错误信息与回溯信息。
///
/// 该 hook 不会阻止进程继续退出，但会将 panic 信息记录到日志中，便于后续排查。
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

/// 注册 Ctrl-C 处理器；当接收到中断信号时设置停止标志。
///
/// 在无法注册处理器的情况下视为严重错误并退出进程。
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
