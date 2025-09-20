mod analysis_log;
mod app;

use analysis_log::LogConfig;
use anyhow::Result;
use sqllog_analysis::config::Config;

fn main() -> Result<()> {
    // 从配置读取是否需要在 stdout 输出日志（用于 release 下根据配置决定）
    let cfg = Config::load();
    let runtime = cfg.resolve_runtime();

    // 初始化日志
    let mut log_config = LogConfig::default();
    log_config.enable_stdout = Some(runtime.enable_stdout);
    if let Some(dir) = runtime.log_dir {
        log_config.log_file = Some(dir);
    }
    log_config.init();

    // 将实际工作委托给 app 模块
    app::run()
}
