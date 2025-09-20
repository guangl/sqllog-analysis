mod analysis_log;

use analysis_log::LogConfig;
use anyhow::Result;
use log::{error, info, trace};
use sqllog_analysis::{
    input_path::get_sqllog_dir,
    process::{process_sqllog_dir, write_error_files},
};
use std::env;

fn main() -> Result<()> {
    // 日志参数解析与初始化
    let log_config = LogConfig::from_args(env::args().skip(1));
    log_config.init();

    trace!("开始获取 sqllog 目录");
    let dir = get_sqllog_dir();
    trace!("获取到 sqllog 目录: {}", dir.display());
    if !dir.exists() {
        error!("目录不存在: {}", env::current_dir()?.display());
        return Ok(());
    }
    trace!("开始处理目录: {}", dir.display());
    let (total_files, total_logs, error_files, elapsed) = process_sqllog_dir(&dir)?;
    info!(
        "解析完成，共处理 {} 个文件，成功解析 {} 条日志，失败解析 {} 条日志，总耗时: {:.2?}",
        total_files,
        total_logs,
        error_files.len(),
        elapsed
    );
    write_error_files(&error_files)?;
    Ok(())
}
