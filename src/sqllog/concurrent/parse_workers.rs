//! 并发解析和流水线导出功能

use crate::error::Result;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
use std::sync::mpsc;

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
use crate::exporter::sync_impl::SyncExporter;

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
/// 批次数据类型
type BatchData = Vec<crate::sqllog::types::Sqllog>;

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
/// 导出任务消息
enum ExportMessage {
    /// 批次数据
    Batch(BatchData),
    /// 结束信号
    Finish,
}

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
/// 并发解析并流水线导出多个文件
///
/// 架构设计：
/// 1. 按文件数创建解析线程（每个文件一个线程，最大 thread_count 个线程）
/// 2. 单独的导出线程接收所有解析线程的批次数据
/// 3. 解析线程完成后导出线程才结束
pub fn parse_and_export_concurrent<E: SyncExporter + Send + 'static>(
    file_paths: &[PathBuf],
    mut exporter: E,
    batch_size: usize,
    thread_count: usize,
) -> Result<Vec<(usize, usize)>> {
    #[cfg(feature = "logging")]
    let start_time = std::time::Instant::now();

    #[cfg(feature = "logging")]
    tracing::info!(
        "开始并发解析和流水线导出 {} 个文件，线程数配置: {}",
        file_paths.len(),
        if thread_count == 0 {
            "每文件一线程".to_string()
        } else {
            thread_count.to_string()
        }
    );

    if file_paths.is_empty() {
        return Ok(Vec::new());
    }

    // 创建导出消息队列
    let (export_tx, export_rx) = mpsc::channel::<ExportMessage>();

    // 启动导出线程
    let export_handle = thread::spawn(move || -> Result<()> {
        #[cfg(feature = "logging")]
        tracing::info!("导出线程启动");

        let mut batch_count = 0;
        let mut total_exported = 0;

        while let Ok(message) = export_rx.recv() {
            match message {
                ExportMessage::Batch(batch) => {
                    if !batch.is_empty() {
                        batch_count += 1;
                        total_exported += batch.len();

                        #[cfg(feature = "logging")]
                        tracing::debug!(
                            "导出线程处理第 {} 批: {} 条记录，累计: {} 条",
                            batch_count,
                            batch.len(),
                            total_exported
                        );

                        if let Err(e) = exporter.export_batch(&batch) {
                            #[cfg(feature = "logging")]
                            tracing::error!("导出批次失败: {}", e);
                            return Err(e.into());
                        }

                        #[cfg(feature = "logging")]
                        tracing::debug!(
                            "导出线程成功处理第 {} 批",
                            batch_count
                        );
                    }
                }
                ExportMessage::Finish => {
                    #[cfg(feature = "logging")]
                    tracing::info!(
                        "导出线程收到结束信号，总共处理 {} 批，{} 条记录",
                        batch_count,
                        total_exported
                    );
                    break;
                }
            }
        }

        // 完成导出
        #[cfg(feature = "logging")]
        tracing::debug!("导出线程开始最终化");

        if let Err(e) = exporter.finalize() {
            #[cfg(feature = "logging")]
            tracing::error!("完成导出时出错: {}", e);
            return Err(e.into());
        }

        #[cfg(feature = "logging")]
        tracing::info!("导出线程完成，总共导出 {} 条记录", total_exported);
        Ok(())
    });

    // 创建文件队列和结果收集
    let file_paths_owned: Vec<(usize, PathBuf)> =
        file_paths.iter().enumerate().map(|(i, p)| (i, p.clone())).collect();
    let file_queue =
        Arc::new(Mutex::new(VecDeque::from_iter(file_paths_owned)));
    let results =
        Arc::new(Mutex::new(vec![(0usize, 0usize); file_paths.len()]));

    // 计算实际使用的线程数
    let actual_threads = if thread_count == 0 {
        file_paths.len()
    } else {
        thread_count.min(file_paths.len())
    };

    #[cfg(feature = "logging")]
    tracing::info!("启动 {} 个解析线程", actual_threads);

    // 创建解析线程
    let mut parse_handles = Vec::new();
    for thread_id in 0..actual_threads {
        let file_queue = Arc::clone(&file_queue);
        let results = Arc::clone(&results);
        let export_tx = export_tx.clone();

        let handle = thread::spawn(move || {
            #[cfg(feature = "logging")]
            tracing::info!("解析线程 {} 启动", thread_id);

            loop {
                // 获取下一个文件
                let file_info = {
                    let mut queue = file_queue.lock().unwrap();
                    queue.pop_front()
                };

                if let Some((file_index, file_path)) = file_info {
                    let file_path_for_log = file_path.clone();

                    #[cfg(feature = "logging")]
                    tracing::info!(
                        "线程 {} 开始处理文件: {}",
                        thread_id,
                        file_path_for_log.display()
                    );

                    let mut total_records = 0;
                    let mut total_errors = 0;

                    // 解析文件并发送批次到导出线程
                    #[cfg(feature = "logging")]
                    tracing::debug!(
                        "线程 {} 开始解析，批次大小: {}",
                        thread_id,
                        batch_size
                    );

                    let parse_result =
                        crate::sqllog::SyncSqllogParser::parse_with_hooks(
                            &file_path,
                            batch_size,
                            |batch_records, batch_errors| {
                                total_records += batch_records.len();
                                total_errors += batch_errors.len();

                                #[cfg(feature = "logging")]
                                tracing::debug!(
                                    "线程 {} 处理批次: {} 记录, {} 错误",
                                    thread_id,
                                    batch_records.len(),
                                    batch_errors.len()
                                );

                                // 发送批次数据到导出线程
                                if !batch_records.is_empty() {
                                    #[cfg(feature = "logging")]
                                    tracing::debug!(
                                        "线程 {} 发送批次到导出线程: {} 条记录",
                                        thread_id,
                                        batch_records.len()
                                    );

                                    if let Err(e) =
                                        export_tx.send(ExportMessage::Batch(
                                            batch_records.to_vec(),
                                        ))
                                    {
                                        #[cfg(feature = "logging")]
                                        tracing::error!(
                                            "线程 {} 发送批次数据到导出线程失败: {}",
                                            thread_id,
                                            e
                                        );
                                    }
                                } else {
                                    #[cfg(feature = "logging")]
                                    tracing::debug!(
                                        "线程 {} 跳过空批次",
                                        thread_id
                                    );
                                }
                            },
                        );

                    // 记录结果
                    match parse_result {
                        Ok(_) => {
                            let mut results_lock = results.lock().unwrap();
                            results_lock[file_index] =
                                (total_records, total_errors);

                            #[cfg(feature = "logging")]
                            tracing::info!(
                                "线程 {} 完成文件 {}: {} 条记录, {} 个错误",
                                thread_id,
                                file_path_for_log.display(),
                                total_records,
                                total_errors
                            );
                        }
                        Err(e) => {
                            #[cfg(feature = "logging")]
                            tracing::error!(
                                "线程 {} 处理文件 {} 失败: {}",
                                thread_id,
                                file_path_for_log.display(),
                                e
                            );
                            // 记录错误结果
                            let mut results_lock = results.lock().unwrap();
                            results_lock[file_index] =
                                (total_records, total_errors);
                        }
                    }
                } else {
                    // 没有更多文件，退出循环
                    break;
                }
            }

            #[cfg(feature = "logging")]
            tracing::info!("解析线程 {} 完成", thread_id);
        });

        parse_handles.push(handle);
    }

    // 等待所有解析线程完成
    #[cfg(feature = "logging")]
    tracing::debug!("开始等待解析线程完成");

    for (i, handle) in parse_handles.into_iter().enumerate() {
        #[cfg(feature = "logging")]
        tracing::debug!("等待解析线程 {} 完成", i);

        if let Err(e) = handle.join() {
            #[cfg(feature = "logging")]
            tracing::error!("解析线程 {} 异常退出: {:?}", i, e);
        } else {
            #[cfg(feature = "logging")]
            tracing::debug!("解析线程 {} 正常完成", i);
        }
    }

    #[cfg(feature = "logging")]
    tracing::debug!("所有解析线程已完成");

    // 发送完成信号给导出线程
    #[cfg(feature = "logging")]
    tracing::debug!("发送完成信号到导出线程");

    if let Err(e) = export_tx.send(ExportMessage::Finish) {
        #[cfg(feature = "logging")]
        tracing::error!("发送完成信号失败: {}", e);
    }

    drop(export_tx); // 关闭发送端

    #[cfg(feature = "logging")]
    tracing::debug!("开始等待导出线程完成");

    // 等待导出线程完成
    match export_handle.join() {
        Ok(result) => {
            if let Err(e) = result {
                #[cfg(feature = "logging")]
                tracing::error!("导出线程返回错误: {}", e);
                return Err(e);
            }
            #[cfg(feature = "logging")]
            tracing::debug!("导出线程正常完成");
        }
        Err(e) => {
            #[cfg(feature = "logging")]
            tracing::error!("导出线程异常退出: {:?}", e);
            return Err(crate::error::SqllogError::parse_error(
                "导出线程异常退出",
            )
            .into());
        }
    }

    // 提取结果
    let final_results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();

    #[cfg(feature = "logging")]
    {
        let elapsed = start_time.elapsed();
        let total_records: usize = final_results.iter().map(|(r, _)| r).sum();
        let total_errors: usize = final_results.iter().map(|(_, e)| e).sum();
        tracing::info!(
            "并发解析和流水线导出完成: {} 个文件，总记录: {}, 总错误: {}, 耗时: {:?}",
            file_paths.len(),
            total_records,
            total_errors,
            elapsed
        );
    }

    Ok(final_results)
}

/// 仅并发解析多个文件（不导出）
pub fn parse_files_concurrent(
    file_paths: &[PathBuf],
    batch_size: usize,
    thread_count: usize,
) -> Result<(
    Vec<crate::sqllog::types::Sqllog>,
    Vec<crate::sqllog::sync_parser::ParseError>,
)> {
    #[cfg(feature = "logging")]
    let start_time = std::time::Instant::now();

    #[cfg(feature = "logging")]
    tracing::info!(
        "开始并发解析 {} 个文件，线程数配置: {}",
        file_paths.len(),
        if thread_count == 0 {
            "每文件一线程".to_string()
        } else {
            thread_count.to_string()
        }
    );

    if file_paths.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    // 创建文件队列和结果收集
    let file_paths_owned: Vec<(usize, PathBuf)> =
        file_paths.iter().enumerate().map(|(i, p)| (i, p.clone())).collect();
    let file_queue =
        Arc::new(Mutex::new(VecDeque::from_iter(file_paths_owned)));
    let all_records = Arc::new(Mutex::new(Vec::new()));
    let all_errors = Arc::new(Mutex::new(Vec::new()));

    // 计算实际使用的线程数
    let actual_threads = if thread_count == 0 {
        file_paths.len()
    } else {
        thread_count.min(file_paths.len())
    };

    #[cfg(feature = "logging")]
    tracing::info!("启动 {} 个解析线程", actual_threads);

    // 创建解析线程
    let mut handles = Vec::new();
    for thread_id in 0..actual_threads {
        let file_queue = Arc::clone(&file_queue);
        let all_records = Arc::clone(&all_records);
        let all_errors = Arc::clone(&all_errors);

        let handle = thread::spawn(move || {
            #[cfg(feature = "logging")]
            tracing::info!("解析线程 {} 启动", thread_id);

            loop {
                // 获取下一个文件
                let file_info = {
                    let mut queue = file_queue.lock().unwrap();
                    queue.pop_front()
                };

                if let Some((_file_index, file_path)) = file_info {
                    let file_path_for_log = file_path.clone();

                    #[cfg(feature = "logging")]
                    tracing::info!(
                        "线程 {} 开始处理文件: {}",
                        thread_id,
                        file_path_for_log.display()
                    );

                    let mut file_records = Vec::new();
                    let mut file_errors = Vec::new();

                    // 解析文件
                    let parse_result =
                        crate::sqllog::SyncSqllogParser::parse_with_hooks(
                            &file_path,
                            batch_size,
                            |batch_records, batch_errors| {
                                file_records.extend_from_slice(batch_records);
                                file_errors.extend_from_slice(batch_errors);
                            },
                        );

                    // 合并结果
                    match parse_result {
                        Ok(_) => {
                            {
                                let mut records = all_records.lock().unwrap();
                                records.extend(file_records);
                            }
                            {
                                let mut errors = all_errors.lock().unwrap();
                                errors.extend(file_errors);
                            }

                            #[cfg(feature = "logging")]
                            tracing::info!(
                                "线程 {} 完成文件: {}",
                                thread_id,
                                file_path_for_log.display()
                            );
                        }
                        Err(e) => {
                            #[cfg(feature = "logging")]
                            tracing::error!(
                                "线程 {} 处理文件 {} 失败: {}",
                                thread_id,
                                file_path_for_log.display(),
                                e
                            );
                        }
                    }
                } else {
                    // 没有更多文件，退出循环
                    break;
                }
            }

            #[cfg(feature = "logging")]
            tracing::info!("解析线程 {} 完成", thread_id);
        });

        handles.push(handle);
    }

    // 等待所有线程完成
    for handle in handles {
        let _ = handle.join();
    }

    // 提取结果
    let final_records =
        Arc::try_unwrap(all_records).unwrap().into_inner().unwrap();
    let final_errors =
        Arc::try_unwrap(all_errors).unwrap().into_inner().unwrap();

    #[cfg(feature = "logging")]
    {
        let elapsed = start_time.elapsed();
        tracing::info!(
            "并发解析完成: {} 个文件，总记录: {}, 总错误: {}, 耗时: {:?}",
            file_paths.len(),
            final_records.len(),
            final_errors.len(),
            elapsed
        );
    }

    Ok((final_records, final_errors))
}

#[cfg(test)]
#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
mod tests {
    use super::*;
    use crate::error::SqllogError;
    use crate::exporter::sync_impl::SyncExporter;
    use crate::sqllog::types::Sqllog;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    struct DummyExporter {
        fail_on_export: bool,
    }

    impl DummyExporter {
        fn new(fail: bool) -> Self {
            Self { fail_on_export: fail }
        }
    }

    impl SyncExporter for DummyExporter {
        fn name(&self) -> &str {
            "DUMMY"
        }

        fn export_record(
            &mut self,
            _record: &Sqllog,
        ) -> crate::error::Result<()> {
            if self.fail_on_export {
                Err(SqllogError::other("export fail"))
            } else {
                Ok(())
            }
        }

        fn export_batch(
            &mut self,
            records: &[Sqllog],
        ) -> crate::error::Result<()> {
            if self.fail_on_export {
                Err(SqllogError::other(format!(
                    "batch fail size {}",
                    records.len()
                )))
            } else {
                // exercise default behavior by delegating to export_record
                for r in records {
                    self.export_record(r)?;
                }
                Ok(())
            }
        }

        fn finalize(&mut self) -> crate::error::Result<()> {
            Ok(())
        }
    }

    fn write_temp_file(content: &str) -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.log");
        let mut f = File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        (temp_dir, path)
    }

    #[test]
    fn test_parse_and_export_concurrent_empty_files() {
        let exporter = DummyExporter::new(false);
        let res =
            parse_and_export_concurrent::<DummyExporter>(&[], exporter, 10, 2)
                .unwrap();
        assert_eq!(res.len(), 0);
    }

    #[test]
    fn test_parse_and_export_concurrent_exporter_error() {
        let content = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x1 thrd:1 user:U trxid:1 stmt:0x1) Test query"#;
        let (_d, path) = write_temp_file(content);

        let exporter = DummyExporter::new(true);
        let res = parse_and_export_concurrent(&[path], exporter, 10, 1);
        assert!(res.is_err(), "expected exporter error to surface");
    }

    #[test]
    fn test_parse_files_concurrent_basic() {
        let content = r#"Invalid line
2025-09-16 20:02:53.562 (EP[0] sess:0x1 thrd:1 user:U trxid:1 stmt:0x1) Valid query
Another bad line"#;
        let (_d, path) = write_temp_file(content);

        let (records, errors) = parse_files_concurrent(&[path], 10, 1).unwrap();
        assert_eq!(records.len(), 1);
        assert!(errors.len() >= 1);
    }

    #[test]
    fn test_parse_and_export_concurrent_finalize_error() {
        // exporter that fails at finalize
        struct FinalizeFailExporter;
        impl SyncExporter for FinalizeFailExporter {
            fn name(&self) -> &str {
                "FINALIZE_FAIL"
            }
            fn export_record(
                &mut self,
                _record: &Sqllog,
            ) -> crate::error::Result<()> {
                Ok(())
            }
            fn export_batch(
                &mut self,
                _records: &[Sqllog],
            ) -> crate::error::Result<()> {
                Ok(())
            }
            fn finalize(&mut self) -> crate::error::Result<()> {
                Err(SqllogError::other("finalize fail"))
            }
        }

        let content = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x1 thrd:1 user:U trxid:1 stmt:0x1) Test query"#;
        let (_d, path) = write_temp_file(content);

        let exporter = FinalizeFailExporter;
        let res = parse_and_export_concurrent(&[path], exporter, 10, 1);
        assert!(res.is_err(), "expected finalize error to surface");
    }

    #[test]
    fn test_parse_and_export_concurrent_all_invalid_no_batches() {
        // file with only invalid lines should not send any batches to exporter
        let content = "Invalid line 1\nAnother bad line\nYet another invalid";
        let (_d, path) = write_temp_file(content);

        let exporter = DummyExporter::new(false);
        let res =
            parse_and_export_concurrent(&[path], exporter, 10, 1).unwrap();
        // one file processed
        assert_eq!(res.len(), 1);
        // no records parsed
        assert_eq!(res[0].0, 0);
        // there should be some parse errors
        assert!(res[0].1 > 0);
    }

    #[test]
    fn test_parse_and_export_concurrent_export_thread_panics() {
        // exporter that panics during export_batch to simulate an export thread panic
        struct PanicExporter;
        impl SyncExporter for PanicExporter {
            fn name(&self) -> &str {
                "PANIC"
            }

            fn export_record(
                &mut self,
                _record: &Sqllog,
            ) -> crate::error::Result<()> {
                Ok(())
            }

            fn export_batch(
                &mut self,
                _records: &[Sqllog],
            ) -> crate::error::Result<()> {
                panic!("simulated panic in export_batch");
            }

            fn finalize(&mut self) -> crate::error::Result<()> {
                Ok(())
            }
        }

        let content = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x1 thrd:1 user:U trxid:1 stmt:0x1) Test query"#;
        let (_d, path) = write_temp_file(content);

        let exporter = PanicExporter;
        let res = parse_and_export_concurrent(&[path], exporter, 10, 1);

        // export thread panicked -> parse_and_export_concurrent should return an error
        assert!(res.is_err(), "expected error when export thread panics");
    }

    #[test]
    fn test_parse_files_concurrent_empty_input() {
        // empty file list should return empty records and errors
        let (records, errors) = parse_files_concurrent(&[], 10, 1).unwrap();
        assert!(records.is_empty());
        assert!(errors.is_empty());
    }

    #[test]
    fn test_parse_files_concurrent_thread_count_zero() {
        // thread_count == 0 should create one thread per file
        let content1 = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x1 thrd:1 user:U trxid:1 stmt:0x1) Valid line"#;
        let content2 = "Invalid line\nAnother bad";
        let (_d1, p1) = write_temp_file(content1);
        let (_d2, p2) = write_temp_file(content2);

        let (records, errors) =
            parse_files_concurrent(&[p1, p2], 10, 0).unwrap();

        // one valid record from first file, at least one parse error from second
        assert_eq!(records.len(), 1);
        assert!(errors.len() >= 1);
    }
}
