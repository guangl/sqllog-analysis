use sqllog_analysis::sqllog::SqllogError;
use std::fs;
use std::path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;

use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

use sqllog_analysis::config::{Config, RuntimeConfig};
use sqllog_analysis::sqllog::Sqllog;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::sync::mpsc::{Sender, channel};

/// 在指定目录中收集符合命名规则的 sqllog 日志文件。
///
/// 规则：文件名以 `dmsql_` 开头且扩展名为 `.log`（不区分大小写）。
///
/// 参数：
/// - `sqllog_dir`：要扫描的目录路径。
///
/// 返回：符合规则的文件路径列表（按目录迭代顺序）。
fn collect_sqllog_files(sqllog_dir: &path::Path) -> Vec<path::PathBuf> {
    let mut files: Vec<path::PathBuf> = Vec::new();
    if let Ok(iter) = fs::read_dir(sqllog_dir) {
        for entry in iter.flatten() {
            let p = entry.path();
            if p.is_file() {
                if let Some(n) = p.file_name().and_then(|s| s.to_str()) {
                    // 使用 Path 的 extension 并进行不区分大小写的比较
                    if n.starts_with("dmsql_")
                        && std::path::Path::new(n)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("log"))
                    {
                        files.push(p);
                    }
                }
            }
        }
    }
    files
}

/// 创建错误写入线程，如果启用了错误写入功能。
///
/// 返回：(发送端, 写入线程句柄) 的元组，如果未启用则都为 None。
fn create_error_writer(
    runtime: &RuntimeConfig,
) -> (Option<Sender<String>>, Option<std::thread::JoinHandle<()>>) {
    if !runtime.sqllog_write_errors {
        return (None, None);
    }

    let out = runtime
        .sqllog_errors_out_path
        .clone()
        .unwrap_or_else(|| path::PathBuf::from("parse_errors.log"));

    let mut opts = OpenOptions::new();
    opts.create(true).write(true);
    if runtime.export_options.write_flags.overwrite {
        opts.truncate(true);
    }

    match opts.open(out) {
        Ok(f) => {
            let (tx, rx) = channel::<String>();
            let handle = std::thread::spawn(move || {
                let mut writer = BufWriter::new(f);
                for msg in rx {
                    if let Err(e) = writeln!(writer, "{msg}") {
                        eprintln!("写入解析错误文件失败: {e}");
                    }
                }
                let _ = writer.flush();
            });
            (Some(tx), Some(handle))
        }
        Err(e) => {
            log::error!("无法打开解析错误输出文件: {e}");
            (None, None)
        }
    }
}

/// 发送错误到错误收集器。
fn send_errors_to_collector(
    errors_sender: Option<&Sender<String>>,
    path: &path::Path,
    errs: &[(usize, String, SqllogError)],
) {
    if let Some(tx) = errors_sender {
        for (ln, raw, e) in errs {
            let obj = serde_json::json!({
                "path": path.display().to_string(),
                "line": ln,
                "error": e.to_string(),
                "raw": raw.replace('\n', "\\n"),
            });
            let _ = tx.send(obj.to_string());
        }
    }
}

/// 处理单个文件的解析。
fn process_single_file(
    path: &path::Path,
    runtime: &RuntimeConfig,
    errors_sender: Option<&Sender<String>>,
    stop: &Arc<AtomicBool>,
) {
    if stop.load(Ordering::SeqCst) {
        log::info!(
            "线程 {:?} 检测到停止，跳过 {}",
            thread::current().id(),
            path.display()
        );
        return;
    }

    log::info!("线程 {:?} 开始解析 {}", thread::current().id(), path.display());

    let start = Instant::now();
    let mut parsed = 0usize;
    let mut errors = 0usize;

    let res = if let Some(n) = runtime.sqllog_chunk_size {
        Sqllog::parse_in_chunks(
            path,
            n,
            |chunk: &[Sqllog]| {
                parsed = parsed.saturating_add(chunk.len());
            },
            |errs: &[(usize, String, SqllogError)]| {
                errors = errors.saturating_add(errs.len());
                send_errors_to_collector(errors_sender, path, errs);
            },
        )
    } else {
        Sqllog::parse_all(
            path,
            |chunk: &[Sqllog]| {
                parsed = parsed.saturating_add(chunk.len());
            },
            |errs: &[(usize, String, SqllogError)]| {
                errors = errors.saturating_add(errs.len());
                send_errors_to_collector(errors_sender, path, errs);
            },
        )
    };

    match res {
        Ok(()) => {
            let dur = start.elapsed();
            let ms = dur.as_millis();
            log::info!(
                "解析完成 {} records={} errors={} duration_ms={}",
                path.display(),
                parsed,
                errors,
                ms
            );
        }
        Err(e) => {
            log::error!("解析失败 {}: {}", path.display(), e,);
        }
    }
}

/// 对指定的日志文件列表进行并行解析。
///
/// 此函数会使用 rayon 线程池并行处理每个文件。每个工作线程会在解析前和解析中检查 `stop` 标志，
/// 以便在接收到停止指令时尽快退出。解析过程中按 runtime 配置决定是否按块回调 `Sqllog::parse_in_chunks`。
///
/// 参数：
/// - `files`：待解析文件的路径切片。
/// - `runtime`：运行时配置，包含线程数、chunk 大小等。
/// - `stop`：用于在接收到停止指令时中断处理的共享布尔标志（Arc<AtomicBool>）。
fn process_files(
    files: &[path::PathBuf],
    runtime: &RuntimeConfig,
    stop: &Arc<AtomicBool>,
) {
    if files.is_empty() {
        let dir_display = runtime
            .sqllog_dir
            .as_ref()
            .map_or_else(|| "<none>".to_string(), |p| p.display().to_string());

        log::warn!("在 {dir_display} 中未找到 dmsql_*.log 文件，跳过解析");
        return;
    }

    log::info!(
        "发现 {} 个待解析文件，使用 rayon 线程池（线程数={}）",
        files.len(),
        runtime.parser_threads
    );

    let pool = ThreadPoolBuilder::new()
        .num_threads(runtime.parser_threads)
        .build()
        .expect("failed to build rayon thread pool");

    if stop.load(Ordering::SeqCst) {
        log::info!("收到停止指令，取消文件解析");
        return;
    }

    let (errors_sender, errors_writer_handle) = create_error_writer(runtime);

    pool.install(|| {
        files.par_iter().for_each(|path| {
            process_single_file(path, runtime, errors_sender.as_ref(), stop);
        });
    });

    if let Some(tx) = errors_sender {
        drop(tx);
    }
    if let Some(handle) = errors_writer_handle {
        let _ = handle.join();
    }
}

/// 程序主逻辑入口（由 `main` 调用），负责加载配置并触发文件扫描与解析。
///
/// 参数：
/// - `stop`：共享停止标志的引用，用于响应 Ctrl-C 或 stdin 的停止指令。
pub fn run(stop: &Arc<AtomicBool>) {
    let runtime = Config::load();
    if let Some(sqllog_dir) = runtime.sqllog_dir.clone() {
        let files = collect_sqllog_files(&sqllog_dir);
        process_files(&files, &runtime, stop);
    } else {
        log::warn!("未配置 sqllog_dir，跳过解析");
    }
}
