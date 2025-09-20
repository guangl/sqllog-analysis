mod analysis_log;

use analysis_log::LogConfig;
use anyhow::Result;
use log::{error, info, trace};
use serde_json::to_writer_pretty;
use sqllog_analysis::{
    duckdb_writer,
    input_path::get_sqllog_dir,
    process::{process_sqllog_dir, write_error_files},
    sqllog::Sqllog,
};
use std::env;
use std::path::PathBuf;

fn main() -> Result<()> {
    // 日志参数解析与初始化
    let args: Vec<String> = env::args().collect();
    let log_config = LogConfig::from_args(args.iter().skip(1).cloned());
    log_config.init();

    trace!("开始获取 sqllog 目录");
    let dir = get_sqllog_dir();
    trace!("获取到 sqllog 目录: {}", dir.display());
    if !dir.exists() {
        error!("目录不存在: {}", env::current_dir()?.display());
        return Ok(());
    }

    // Optional CLI: --duckdb-path=<path> and --duckdb-report=<path|->
    let mut duckdb_path: Option<PathBuf> = None;
    let mut duckdb_report: Option<String> = None;
    let mut chunk_size: usize = 1000;
    for a in args.iter().skip(1) {
        if let Some(p) = a.strip_prefix("--duckdb-path=") {
            duckdb_path = Some(PathBuf::from(p));
        } else if let Some(p) = a.strip_prefix("--duckdb-report=") {
            duckdb_report = Some(p.to_string());
        } else if let Some(n) = a.strip_prefix("--duckdb-chunk-size=") {
            if let Ok(v) = n.parse::<usize>() {
                chunk_size = v;
            }
        }
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

    // If duckdb path provided, re-parse files and collect sqllogs to write into DuckDB
    if let Some(db_path) = duckdb_path {
        let mut all_logs: Vec<Sqllog> = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("dmsql") && PathBuf::from(name).extension().is_some() {
                    let (logs, _errs) = Sqllog::from_file_with_errors(&path);
                    all_logs.extend(logs);
                }
            }
        }

        if !all_logs.is_empty() {
            info!(
                "写入 DuckDB: {} 条记录 -> {}",
                all_logs.len(),
                db_path.display()
            );
            let reports = duckdb_writer::write_sqllogs_to_duckdb_with_chunk_and_report(
                &db_path, &all_logs, chunk_size, true,
            )?;
            // If user requested a report, serialize as JSON to file or stdout
            if let Some(rpath) = duckdb_report {
                if rpath == "-" {
                    to_writer_pretty(std::io::stdout(), &reports)?;
                } else {
                    let f = std::fs::File::create(rpath)?;
                    to_writer_pretty(f, &reports)?;
                }
            }
        } else {
            info!("没有解析到任何记录，跳过 DuckDB 写入");
        }
    }

    Ok(())
}
