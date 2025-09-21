use crate::config::Config;
use log::{info, trace};
use std::{env, path::PathBuf};

/// 获取 sqllog 文件夹路径，优先使用配置文件中的 `[sqllog].sqllog_dir`。
/// 如果未在配置中提供，回退到当前工作目录。
#[must_use]
pub fn get_sqllog_dir() -> PathBuf {
    let runtime = Config::load();

    if let Some(p) = runtime.sqllog_dir.as_ref() {
        trace!("从配置读取 sqllog 路径: {}", p.display());
        info!("sqllog 路径: {}", p.display());
        return p.clone();
    }

    // 回退到当前工作目录
    let cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    trace!("回退到当前工作目录作为 sqllog 路径: {}", cwd.display());
    info!("sqllog 路径: {}", cwd.display());
    cwd
}
