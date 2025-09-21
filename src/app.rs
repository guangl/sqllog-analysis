use anyhow::Result;
use log::{error, info, trace};
use sqllog_analysis::{
    config::Config,
    duckdb_writer,
    input_path::get_sqllog_dir,
    process::{parse_sqllog_file, write_error_files},
};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

pub fn run(stop: &Arc<AtomicBool>) -> Result<()> {
    trace!("开始获取 sqllog 目录");
    let dir = get_sqllog_dir();
    trace!("获取到 sqllog 目录: {}", dir.display());
    if !dir.exists() {
        error!("目录不存在: {}", dir.display());
        return Ok(());
    }

    let runtime = Config::load();
    let db_path = runtime.db_path;
    let use_in_memory = runtime.use_in_memory;

    // 如果启用了内存模式，创建内存连接并在整个运行过程中复用
    let mut mem_conn_opt: Option<duckdb::Connection> = None;
    if use_in_memory {
        mem_conn_opt = Some(
            sqllog_analysis::duckdb_writer::create_in_memory_connection()?,
        );
    }

    let start = Instant::now();
    let mut total_files = 0usize;
    let mut total_logs = 0usize;
    let mut error_files: Vec<(String, String)> = Vec::new();

    if let Some(conn) = mem_conn_opt.as_mut() {
        // 内存模式：复用内存连接的处理路径
        process_directory_in_memory(
            &dir,
            conn,
            &mut total_files,
            &mut total_logs,
            &mut error_files,
            stop,
        )?;
    } else {
        // 磁盘模式：按路径处理
        process_directory(
            &dir,
            &db_path,
            &mut total_files,
            &mut total_logs,
            &mut error_files,
            stop,
        )?;
    }

    let elapsed = start.elapsed();
    info!(
        "解析完成，共处理 {} 个文件，成功解析 {} 条日志，失败解析 {} 条日志，总耗时: {:.2?}",
        total_files,
        total_logs,
        error_files.len(),
        elapsed
    );
    write_error_files(&error_files)?;

    // 导出（可选，由配置控制）
    if runtime.export_enabled {
        let out_path = if let Some(p) = runtime.export_out_path.as_ref() {
            p.clone()
        } else {
            // derive filename from db_path, e.g. sqllogs.duckdb -> sqllogs_export.<ext>
            let pb = std::path::PathBuf::from(&db_path);
            let stem =
                pb.file_stem().and_then(|s| s.to_str()).unwrap_or("sqllogs");
            let ext = match runtime.export_format.as_str() {
                "json" => "json",
                "excel" | "xlsx" => "xlsx",
                _ => "csv",
            };
            pb.with_file_name(format!("{stem}_export.{ext}"))
        };

        // If we have an in-memory connection, export from that connection; otherwise export from disk path
        if let Some(conn) = mem_conn_opt.as_ref() {
            if let Err(e) = duckdb_writer::export_sqllogs_from_connection(
                conn,
                &out_path,
                &runtime.export_format,
                &runtime.export_options,
            ) {
                error!("从内存连接导出 DuckDB 失败: {e}");
            } else {
                info!("导出完成: {}", out_path.display());
            }
        } else {
            if let Err(e) = duckdb_writer::export_sqllogs_to_file_with_flags(
                &db_path,
                &out_path,
                &runtime.export_format,
                &runtime.export_options,
            ) {
                error!("导出 DuckDB 失败: {e}");
            } else {
                info!("导出完成: {}", out_path.display());
            }
        }
    }

    Ok(())
}

fn process_directory(
    dir: &PathBuf,
    db_path: &str,
    total_files: &mut usize,
    total_logs: &mut usize,
    error_files: &mut Vec<(String, String)>,
    stop: &Arc<AtomicBool>,
) -> Result<()> {
    trace!("开始处理目录: {}", dir.display());

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if stop.load(Ordering::SeqCst) {
            info!("停止标志被触发，提前结束目录处理");
            break;
        }
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            // 匹配文件名：以 'dmsql' 开始并以 '.log' 结尾（不区分大小写）
            let name_lower = name.to_lowercase();
            if name_lower.starts_with("dmsql")
                && std::path::Path::new(&name_lower)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("log"))
            {
                *total_files += 1;
                process_file(
                    &path,
                    name,
                    db_path,
                    total_logs,
                    error_files,
                    stop,
                );
            }
        }
    }

    Ok(())
}

fn process_file(
    path: &PathBuf,
    name: &str,
    db_path: &str,
    total_logs: &mut usize,
    error_files: &mut Vec<(String, String)>,
    stop: &Arc<AtomicBool>,
) {
    trace!("开始解析文件: {name}");
    let file_start = Instant::now();
    let (logs, formatted_errors) = parse_sqllog_file(path);
    let elapsed_file = file_start.elapsed();
    trace!("文件 {name} 解析耗时: {elapsed_file:.2?}");
    *total_logs += logs.len();

    // 将解析后的日志一次性追加到 DuckDB
    if !logs.is_empty() {
        if stop.load(Ordering::SeqCst) {
            info!("停止标志被触发，跳过写入文件: {name}");
            return;
        }
        // 磁盘模式：直接写入到磁盘上的 DuckDB
        let write_res =
            duckdb_writer::write_sqllogs_to_duckdb(db_path, &logs, false);
        if let Err(e) = write_res {
            error!("写入 DuckDB 失败: {e}");
        } else {
            trace!("文件 {} 的 {} 条记录已写入 DuckDB", name, logs.len());
        }
    }

    // 收集解析错误以便后续报告
    for (file, msg) in formatted_errors {
        error_files.push((file, msg));
    }
}

// 内存连接的目录处理版本：接收一个可变的 duckdb Connection 引用并在整个目录处理中复用
fn process_directory_in_memory(
    dir: &PathBuf,
    conn: &mut duckdb::Connection,
    total_files: &mut usize,
    total_logs: &mut usize,
    error_files: &mut Vec<(String, String)>,
    stop: &Arc<AtomicBool>,
) -> Result<()> {
    trace!("开始处理目录 (内存模式): {}", dir.display());

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if stop.load(Ordering::SeqCst) {
            info!("停止标志被触发，提前结束目录处理");
            break;
        }
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            let name_lower = name.to_lowercase();
            if name_lower.starts_with("dmsql")
                && std::path::Path::new(&name_lower)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("log"))
            {
                *total_files += 1;
                process_file_in_memory(
                    &path,
                    name,
                    conn,
                    total_logs,
                    error_files,
                    stop,
                );
            }
        }
    }

    Ok(())
}

// 内存模式下的文件处理函数：直接使用传入的内存连接写入
fn process_file_in_memory(
    path: &PathBuf,
    name: &str,
    conn: &mut duckdb::Connection,
    total_logs: &mut usize,
    error_files: &mut Vec<(String, String)>,
    stop: &Arc<AtomicBool>,
) {
    trace!("开始解析文件 (内存模式): {name}");
    let file_start = Instant::now();
    let (logs, formatted_errors) = parse_sqllog_file(path);
    let elapsed_file = file_start.elapsed();
    trace!("文件 {name} 解析耗时: {elapsed_file:.2?}");
    *total_logs += logs.len();

    if !logs.is_empty() {
        if stop.load(Ordering::SeqCst) {
            info!("停止标志被触发，跳过写入文件: {name}");
            return;
        }
        let write_res = duckdb_writer::write_sqllogs_to_connection(conn, &logs);
        if let Err(e) = write_res {
            error!("写入 DuckDB (内存) 失败: {e}");
        } else {
            trace!("文件 {} 的 {} 条记录已写入内存 DuckDB", name, logs.len());
        }
    }

    for (file, msg) in formatted_errors {
        error_files.push((file, msg));
    }
}
