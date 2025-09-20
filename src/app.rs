use anyhow::Result;
use log::{error, info, trace};
use sqllog_analysis::config::Config;
use sqllog_analysis::process;
use sqllog_analysis::{
    duckdb_writer, input_path::get_sqllog_dir, process::write_error_files, sqllog::Sqllog,
};
use std::{fs, path::PathBuf, time::Instant};

pub fn run() -> Result<()> {
    trace!("开始获取 sqllog 目录");
    let dir = get_sqllog_dir();
    trace!("获取到 sqllog 目录: {}", dir.display());
    if !dir.exists() {
        error!("目录不存在: {}", dir.display());
        return Ok(());
    }

    let cfg = Config::load();
    let _runtime = cfg.resolve_runtime();
    let db_path = _runtime.db_path;
    let chunk_size = _runtime.chunk_size;
    let create_indexes = _runtime.create_indexes;

    let start = Instant::now();
    let mut total_files = 0usize;
    let mut total_logs = 0usize;
    let mut error_files: Vec<(String, String)> = Vec::new();
    let mut chunk: Vec<Sqllog> = Vec::new();

    process_directory(
        &dir,
        chunk_size,
        &db_path,
        &mut chunk,
        &mut total_files,
        &mut total_logs,
        &mut error_files,
    )?;

    let elapsed = start.elapsed();
    info!(
        "解析完成，共处理 {} 个文件，成功解析 {} 条日志，失败解析 {} 条日志，总耗时: {:.2?}",
        total_files,
        total_logs,
        error_files.len(),
        elapsed
    );
    write_error_files(&error_files)?;

    flush_chunk_if_needed(&db_path, &mut chunk)?;

    if create_indexes {
        create_indexes_and_report(&db_path, chunk_size)?;
    }

    Ok(())
}

// load_config moved to `config::Config::resolve_runtime`

fn process_directory(
    dir: &PathBuf,
    chunk_size: usize,
    db_path: &str,
    chunk: &mut Vec<Sqllog>,
    total_files: &mut usize,
    total_logs: &mut usize,
    error_files: &mut Vec<(String, String)>,
) -> Result<()> {
    trace!("开始处理目录: {}", dir.display());

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("dmsql") && PathBuf::from(name).extension().is_some() {
                *total_files += 1;
                process_file(
                    &path,
                    name,
                    chunk_size,
                    db_path,
                    chunk,
                    total_logs,
                    error_files,
                )?;
            }
        }
    }

    Ok(())
}

fn process_file(
    path: &PathBuf,
    name: &str,
    chunk_size: usize,
    db_path: &str,
    chunk: &mut Vec<Sqllog>,
    total_logs: &mut usize,
    error_files: &mut Vec<(String, String)>,
) -> Result<()> {
    trace!("开始解析文件: {name}");
    let file_start = Instant::now();
    let (logs, formatted_errors) = process::parse_sqllog_file(path);
    let elapsed_file = file_start.elapsed();
    trace!("文件 {name} 解析耗时: {elapsed_file:.2?}");
    *total_logs += logs.len();

    // stream in parsed logs
    for rec in logs {
        chunk.push(rec);
        if chunk.len() >= chunk_size {
            // flush chunk to duckdb
            if let Err(e) = duckdb_writer::append_sqllogs_chunk(db_path, chunk) {
                error!("流式写入 DuckDB 失败: {}", e);
            }
            chunk.clear();
        }
    }

    // collect any parse errors for later reporting
    for (file, msg) in formatted_errors {
        error_files.push((file, msg));
    }

    Ok(())
}

fn flush_chunk_if_needed(db_path: &str, chunk: &mut Vec<Sqllog>) -> Result<()> {
    if !chunk.is_empty() {
        if let Err(e) = duckdb_writer::append_sqllogs_chunk(db_path, chunk) {
            error!("流式写入 DuckDB 失败: {}", e);
        }
        chunk.clear();
    }
    Ok(())
}

fn create_indexes_and_report(db_path: &str, chunk_size: usize) -> Result<()> {
    trace!("开始创建索引并收集报告");
    match duckdb_writer::write_sqllogs_to_duckdb_with_chunk_and_report(
        db_path,
        &[],
        chunk_size,
        true,
    ) {
        Ok(reports) => {
            for r in reports {
                if let Some(err) = r.error {
                    error!("索引创建失败: {} -> {}", r.statement, err);
                } else if let Some(ms) = r.elapsed_ms {
                    info!("索引创建成功: {} ({} ms)", r.statement, ms);
                } else {
                    info!("索引创建完成但无耗时信息: {}", r.statement);
                }
            }
        }
        Err(e) => error!("创建索引失败: {}", e),
    }

    Ok(())
}
