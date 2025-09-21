use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use sqllog_analysis::config::Config;
use sqllog_analysis::sqllog::Sqllog;

#[allow(clippy::unnecessary_wraps)]
pub fn run(_stop: &Arc<AtomicBool>) -> Result<()> {
    let runtime = Config::load();
    if let Some(sqllog_dir) = runtime.sqllog_dir {
        let target = sqllog_dir.join("dmsql_OA01_20250916_200253.log");
        if target.exists() && target.is_file() {
            log::info!("开始解析文件: {}", target.display());
            match Sqllog::parse_all(
                &target,
                |all: &[sqllog_analysis::sqllog::Sqllog]| {
                    log::info!("文件解析完成，record_count={}", all.len());
                },
                |_: &[(
                    usize,
                    String,
                    sqllog_analysis::sqllog::SqllogError,
                )]| {},
            ) {
                Ok(()) => {
                    log::info!("解析完成");
                }
                Err(e) => {
                    log::error!("解析失败: {e}");
                }
            }
        } else {
            log::warn!("目标文件不存在: {}", target.display());
        }
    } else {
        log::warn!("未配置 sqllog_dir，跳过解析");
    }

    Ok(())
}
