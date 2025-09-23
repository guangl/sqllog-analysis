//! 并发 SQL 日志解析模块
//!
//! 提供多线程并发解析功能，支持配置最大线程数和多 Exporter 并发导出

use crate::config::SqllogConfig;
use crate::error::{Result, SqllogError};
use crate::sqllog::{sync_parser::ParseError, types::Sqllog};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
use crate::exporter::{ExportStats, SyncExporter};

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
use std::time::Instant;

/// 解析任务
#[derive(Debug, Clone)]
pub struct ParseTask {
    /// 文件路径
    pub file_path: PathBuf,
    /// 批次大小
    pub batch_size: usize,
}

/// 解析结果批次
#[derive(Debug, Clone)]
pub struct ParseBatch {
    /// 解析成功的记录
    pub records: Vec<Sqllog>,
    /// 解析错误
    pub errors: Vec<ParseError>,
    /// 源文件路径
    pub source_file: PathBuf,
    /// 批次ID
    pub batch_id: usize,
}

/// 导出任务
#[derive(Debug, Clone)]
pub struct ExportTask {
    /// 要导出的记录
    pub records: Vec<Sqllog>,
    /// 任务ID
    pub task_id: usize,
    /// 源文件路径
    pub source_file: PathBuf,
}

/// 多线程并发解析器
pub struct ConcurrentParser {
    config: SqllogConfig,
}

impl ConcurrentParser {
    /// 创建新的并发解析器
    pub fn new(config: SqllogConfig) -> Self {
        Self { config }
    }

    /// 使用多个 Exporter 并发解析和导出文件
    ///
    /// # 参数
    /// - `file_paths`: 要解析的文件路径列表
    /// - `exporters`: 导出器列表（每个导出器将在独立线程中运行）
    ///
    /// # 返回
    /// 返回 (所有解析错误, 所有导出器统计信息)
    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    pub fn parse_and_export_concurrent(
        &self,
        file_paths: &[PathBuf],
        exporters: Vec<Box<dyn SyncExporter + Send>>,
    ) -> Result<(Vec<ParseError>, Vec<(String, ExportStats)>)> {
        if file_paths.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        if exporters.is_empty() {
            return Err(SqllogError::other("至少需要一个导出器".to_string()));
        }

        let start_time = Instant::now();

        // 确定线程数：配置的 thread_count 或文件数量
        let parse_thread_count = self
            .config
            .thread_count
            .unwrap_or(file_paths.len())
            .min(file_paths.len());
        let export_thread_count = exporters.len();

        #[cfg(feature = "logging")]
        tracing::info!(
            "开始并发解析和导出 {} 个文件，使用 {} 个解析线程，{} 个导出线程",
            file_paths.len(),
            parse_thread_count,
            export_thread_count
        );

        // 创建通道
        let (parse_task_tx, parse_task_rx) = mpsc::channel::<ParseTask>();
        let (error_tx, error_rx) = mpsc::channel::<Vec<ParseError>>();

        // 为每个导出器创建独立的通道
        let mut export_task_txs = Vec::new();
        let mut export_handles = Vec::new();
        let export_stats = Arc::new(Mutex::new(Vec::new()));

        for (exporter_id, mut exporter) in exporters.into_iter().enumerate() {
            let (export_task_tx, export_task_rx) =
                mpsc::channel::<ExportTask>();
            export_task_txs.push(export_task_tx);

            let export_stats = export_stats.clone();
            let batch_size = self.config.batch_size;
            let export_task_rx = Arc::new(Mutex::new(export_task_rx));

            let handle = thread::spawn(move || -> Result<()> {
                let stats = Self::export_worker(
                    exporter_id,
                    &mut exporter,
                    export_task_rx,
                    batch_size,
                )?;
                // 保存导出统计信息
                let mut export_stats = export_stats.lock().unwrap();
                export_stats.push((exporter.name().to_string(), stats));
                Ok(())
            });
            export_handles.push(handle);
        }

        // 启动解析线程
        let mut parse_handles = Vec::new();
        let parse_task_rx = Arc::new(Mutex::new(parse_task_rx));
        let export_task_txs = Arc::new(export_task_txs); // 共享导出通道

        for thread_id in 0..parse_thread_count {
            let parse_task_rx = Arc::clone(&parse_task_rx);
            let export_task_txs = Arc::clone(&export_task_txs);
            let error_tx = error_tx.clone();
            let batch_size = self.config.batch_size;

            let handle = thread::spawn(move || {
                Self::parse_worker(
                    thread_id,
                    parse_task_rx,
                    export_task_txs,
                    error_tx,
                    batch_size,
                )
            });
            parse_handles.push(handle);
        }

        // 分发解析任务
        for file_path in file_paths {
            let task = ParseTask {
                file_path: file_path.clone(),
                batch_size: self.config.batch_size,
            };

            parse_task_tx.send(task).map_err(|e| {
                SqllogError::other(format!("发送解析任务失败: {}", e))
            })?;
        }

        // 关闭任务通道，通知解析线程退出
        drop(parse_task_tx);

        // 等待所有解析线程完成
        for handle in parse_handles {
            if let Ok(result) = handle.join() {
                if let Err(e) = result {
                    #[cfg(feature = "logging")]
                    tracing::error!("解析线程异常: {}", e);
                }
            }
        }

        // 收集所有解析错误
        drop(error_tx);
        let mut all_errors = Vec::new();
        while let Ok(errors) = error_rx.recv() {
            all_errors.extend(errors);
        }

        // 关闭导出任务通道，通知导出线程退出
        // 需要释放Arc引用，以便导出通道能够被drop
        drop(export_task_txs);

        // 等待所有导出线程完成
        for handle in export_handles {
            if let Ok(result) = handle.join() {
                if let Err(e) = result {
                    #[cfg(feature = "logging")]
                    tracing::error!("导出线程异常: {}", e);
                }
            }
        }

        // 获取导出统计信息
        let final_stats = {
            let stats = export_stats.lock().unwrap();
            stats.clone()
        };

        let elapsed = start_time.elapsed();
        #[cfg(feature = "logging")]
        tracing::info!(
            "并发解析和导出完成，耗时: {:?}, 解析错误: {} 个, 导出器: {} 个",
            elapsed,
            all_errors.len(),
            final_stats.len()
        );

        Ok((all_errors, final_stats))
    }

    /// 解析工作线程
    fn parse_worker(
        thread_id: usize,
        task_rx: Arc<Mutex<mpsc::Receiver<ParseTask>>>,
        export_task_txs: Arc<Vec<mpsc::Sender<ExportTask>>>,
        error_tx: mpsc::Sender<Vec<ParseError>>,
        batch_size: usize,
    ) -> Result<()> {
        let mut task_counter = 0;

        #[cfg(feature = "logging")]
        tracing::debug!("解析工作线程 {} 启动", thread_id);

        loop {
            let task = {
                let rx = task_rx.lock().unwrap();
                match rx.recv() {
                    Ok(task) => task,
                    Err(_) => break, // 通道关闭，退出循环
                }
            };

            #[cfg(feature = "logging")]
            tracing::debug!(
                "线程 {} 开始解析文件: {}",
                thread_id,
                task.file_path.display()
            );

            let mut file_errors = Vec::new();
            let mut export_task_id = 0;

            // 流式解析文件，分批发送到导出线程
            let parse_result =
                crate::sqllog::SyncSqllogParser::parse_with_hooks(
                    &task.file_path,
                    batch_size,
                    |batch_records, batch_errors| {
                        // 收集解析错误
                        file_errors.extend_from_slice(batch_errors);

                        // 如果有记录，发送到所有导出线程
                        if !batch_records.is_empty() {
                            let export_task = ExportTask {
                                records: batch_records.to_vec(),
                                task_id: export_task_id,
                                source_file: task.file_path.clone(),
                            };

                            // 将任务发送给所有导出器
                            for (exporter_id, tx) in
                                export_task_txs.iter().enumerate()
                            {
                                if let Err(e) = tx.send(export_task.clone()) {
                                    #[cfg(feature = "logging")]
                                    tracing::error!(
                                        "线程 {} 发送导出任务到导出器 {} 失败: {}",
                                        thread_id,
                                        exporter_id,
                                        e
                                    );
                                }
                            }

                            export_task_id += 1;
                        }
                    },
                );

            match parse_result {
                Ok(_) => {
                    #[cfg(feature = "logging")]
                    tracing::debug!(
                        "线程 {} 成功解析文件: {}, 发送了 {} 个导出任务，{} 个错误",
                        thread_id,
                        task.file_path.display(),
                        export_task_id,
                        file_errors.len()
                    );
                }
                Err(e) => {
                    #[cfg(feature = "logging")]
                    tracing::error!(
                        "线程 {} 解析文件失败: {}, 错误: {}",
                        thread_id,
                        task.file_path.display(),
                        e
                    );

                    let error = ParseError {
                        line: 0,
                        content: format!(
                            "解析文件失败: {}",
                            task.file_path.display()
                        ),
                        error: e.to_string(),
                    };
                    file_errors.push(error);
                }
            }

            // 发送解析错误
            if !file_errors.is_empty() {
                if let Err(e) = error_tx.send(file_errors) {
                    #[cfg(feature = "logging")]
                    tracing::error!("线程 {} 发送错误失败: {}", thread_id, e);
                }
            }

            task_counter += 1;
        }

        #[cfg(feature = "logging")]
        tracing::debug!(
            "解析工作线程 {} 退出，处理了 {} 个任务",
            thread_id,
            task_counter
        );

        Ok(())
    }

    /// 导出工作线程
    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    fn export_worker(
        exporter_id: usize,
        exporter: &mut Box<dyn SyncExporter + Send>,
        task_rx: Arc<Mutex<mpsc::Receiver<ExportTask>>>,
        _batch_size: usize,
    ) -> Result<ExportStats> {
        let mut exported_count = 0;
        let mut failed_count = 0;

        #[cfg(feature = "logging")]
        tracing::debug!(
            "导出工作线程 {} 启动，导出器: {}",
            exporter_id,
            exporter.name()
        );

        loop {
            let task = {
                let rx = task_rx.lock().unwrap();
                match rx.recv() {
                    Ok(task) => task,
                    Err(_) => break, // 通道关闭，退出循环
                }
            };

            #[cfg(feature = "logging")]
            tracing::trace!(
                "导出线程 {} 处理任务 {}: {} 条记录",
                exporter_id,
                task.task_id,
                task.records.len()
            );

            match exporter.export_batch(&task.records) {
                Ok(_) => {
                    exported_count += task.records.len();
                    #[cfg(feature = "logging")]
                    tracing::trace!(
                        "导出线程 {} 成功导出 {} 条记录",
                        exporter_id,
                        task.records.len()
                    );
                }
                Err(e) => {
                    failed_count += task.records.len();
                    #[cfg(feature = "logging")]
                    tracing::error!(
                        "导出线程 {} 导出失败: {}, 影响 {} 条记录",
                        exporter_id,
                        e,
                        task.records.len()
                    );
                }
            }
        }

        // 完成导出器
        if let Err(e) = exporter.finalize() {
            #[cfg(feature = "logging")]
            tracing::error!("导出线程 {} 完成时出错: {}", exporter_id, e);
        }

        let mut stats = exporter.get_stats();
        stats.exported_records = exported_count;
        stats.failed_records = failed_count;
        stats.finish();

        #[cfg(feature = "logging")]
        tracing::info!(
            "导出工作线程 {} 退出，导出器: {}，成功: {} 条，失败: {} 条",
            exporter_id,
            exporter.name(),
            exported_count,
            failed_count
        );

        Ok(stats)
    }

    /// 简化版本：解析文件但不导出，只收集结果
    pub fn parse_files_concurrent(
        &self,
        file_paths: &[PathBuf],
    ) -> Result<(Vec<Sqllog>, Vec<ParseError>)> {
        if file_paths.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let parse_thread_count = self
            .config
            .thread_count
            .unwrap_or(file_paths.len())
            .min(file_paths.len());

        #[cfg(feature = "logging")]
        tracing::info!(
            "开始并发解析 {} 个文件，使用 {} 个线程",
            file_paths.len(),
            parse_thread_count
        );

        let (result_tx, result_rx) = mpsc::channel::<ParseBatch>();

        // 启动解析线程 - 每个线程处理一个文件
        let mut handles = Vec::new();
        for (thread_id, file_path) in
            file_paths.iter().take(parse_thread_count).enumerate()
        {
            let result_tx = result_tx.clone();
            let batch_size = self.config.batch_size;
            let file_path = file_path.clone();

            let handle = thread::spawn(move || {
                Self::simple_parse_single_file(
                    thread_id, file_path, result_tx, batch_size,
                )
            });
            handles.push(handle);
        }

        // 如果文件数量大于线程数，剩余文件用任务分发方式处理
        if file_paths.len() > parse_thread_count {
            let (task_tx, task_rx) = mpsc::channel::<ParseTask>();
            let task_rx = Arc::new(std::sync::Mutex::new(task_rx));

            // 启动额外的工作线程
            for thread_id in
                parse_thread_count..file_paths.len().min(parse_thread_count * 2)
            {
                let task_rx = Arc::clone(&task_rx);
                let result_tx = result_tx.clone();
                let batch_size = self.config.batch_size;

                let handle = thread::spawn(move || {
                    Self::simple_parse_worker_with_shared_rx(
                        thread_id, task_rx, result_tx, batch_size,
                    )
                });
                handles.push(handle);
            }

            // 分发剩余文件
            for file_path in &file_paths[parse_thread_count..] {
                let task = ParseTask {
                    file_path: file_path.clone(),
                    batch_size: self.config.batch_size,
                };
                task_tx.send(task).map_err(|e| {
                    SqllogError::other(format!("发送任务失败: {}", e))
                })?;
            }
            drop(task_tx);
        }

        drop(result_tx);

        // 等待线程完成
        for handle in handles {
            let _ = handle.join();
        }

        // 收集结果
        let mut all_records = Vec::new();
        let mut all_errors = Vec::new();

        while let Ok(batch) = result_rx.recv() {
            all_records.extend(batch.records);
            all_errors.extend(batch.errors);
        }

        #[cfg(feature = "logging")]
        tracing::info!(
            "并发解析完成，共 {} 条记录，{} 个错误",
            all_records.len(),
            all_errors.len()
        );

        Ok((all_records, all_errors))
    }

    /// 解析单个文件的工作线程
    fn simple_parse_single_file(
        thread_id: usize,
        file_path: PathBuf,
        result_tx: mpsc::Sender<ParseBatch>,
        batch_size: usize,
    ) -> Result<()> {
        #[cfg(feature = "logging")]
        tracing::debug!(
            "解析线程 {} 处理文件: {}",
            thread_id,
            file_path.display()
        );

        let mut all_records = Vec::new();
        let mut all_errors = Vec::new();
        let mut batch_counter = 0;

        let parse_result = crate::sqllog::SyncSqllogParser::parse_with_hooks(
            &file_path,
            batch_size,
            |batch_records, batch_errors| {
                all_records.extend_from_slice(batch_records);
                all_errors.extend_from_slice(batch_errors);
                batch_counter += 1;
            },
        );

        match parse_result {
            Ok(_) => {
                let batch = ParseBatch {
                    records: all_records,
                    errors: all_errors,
                    source_file: file_path.clone(),
                    batch_id: batch_counter,
                };

                if let Err(e) = result_tx.send(batch) {
                    #[cfg(feature = "logging")]
                    tracing::error!("线程 {} 发送结果失败: {}", thread_id, e);
                }
            }
            Err(e) => {
                #[cfg(feature = "logging")]
                tracing::error!(
                    "线程 {} 解析文件失败: {}, 错误: {}",
                    thread_id,
                    file_path.display(),
                    e
                );
            }
        }

        Ok(())
    }

    /// 使用共享接收器的简化解析工作线程
    fn simple_parse_worker_with_shared_rx(
        thread_id: usize,
        task_rx: Arc<std::sync::Mutex<mpsc::Receiver<ParseTask>>>,
        result_tx: mpsc::Sender<ParseBatch>,
        batch_size: usize,
    ) -> Result<()> {
        #[cfg(feature = "logging")]
        tracing::debug!("共享解析工作线程 {} 启动", thread_id);

        loop {
            let task = {
                let rx = task_rx.lock().unwrap();
                match rx.recv() {
                    Ok(task) => task,
                    Err(_) => break, // 通道关闭，退出循环
                }
            };

            let mut all_records = Vec::new();
            let mut all_errors = Vec::new();
            let mut batch_counter = 0;

            let parse_result =
                crate::sqllog::SyncSqllogParser::parse_with_hooks(
                    &task.file_path,
                    batch_size,
                    |batch_records, batch_errors| {
                        all_records.extend_from_slice(batch_records);
                        all_errors.extend_from_slice(batch_errors);
                        batch_counter += 1;
                    },
                );

            match parse_result {
                Ok(_) => {
                    let batch = ParseBatch {
                        records: all_records,
                        errors: all_errors,
                        source_file: task.file_path.clone(),
                        batch_id: batch_counter,
                    };

                    if let Err(e) = result_tx.send(batch) {
                        #[cfg(feature = "logging")]
                        tracing::error!(
                            "线程 {} 发送结果失败: {}",
                            thread_id,
                            e
                        );
                    }
                }
                Err(e) => {
                    #[cfg(feature = "logging")]
                    tracing::error!(
                        "线程 {} 解析文件失败: {}, 错误: {}",
                        thread_id,
                        task.file_path.display(),
                        e
                    );
                }
            }
        }

        Ok(())
    }
}

impl Default for ConcurrentParser {
    fn default() -> Self {
        Self::new(SqllogConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_concurrent_parsing() {
        // 创建测试文件
        let mut temp_files = Vec::new();
        let test_content = r#"2024-01-01 12:00:00.000 (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [SEL]: SELECT * FROM users;
EXECTIME: 100(ms) ROWCOUNT: 5 EXEC_ID: 123.
2024-01-01 12:00:01.000 (EP[2] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [UPD]: UPDATE users SET name = 'test';
EXECTIME: 50(ms) ROWCOUNT: 1 EXEC_ID: 124.
"#;

        for _i in 0..3 {
            let mut temp_file = NamedTempFile::new().unwrap();
            temp_file.write_all(test_content.as_bytes()).unwrap();
            temp_files.push(temp_file);
        }

        let file_paths: Vec<PathBuf> =
            temp_files.iter().map(|f| f.path().to_path_buf()).collect();

        // 配置解析器
        let config = SqllogConfig {
            thread_count: Some(2), // 使用2个线程解析3个文件
            batch_size: 1000,
            queue_buffer_size: 100,
        };

        let parser = ConcurrentParser::new(config);

        // 执行并发解析
        let (records, errors) =
            parser.parse_files_concurrent(&file_paths).unwrap();

        // 验证结果
        assert_eq!(records.len(), 6); // 每个文件2条记录，共6条
        assert_eq!(errors.len(), 0); // 没有解析错误

        println!(
            "并发解析测试完成: {} 条记录, {} 个错误",
            records.len(),
            errors.len()
        );
    }

    #[test]
    fn test_default_thread_count() {
        let config = SqllogConfig::default();
        let parser = ConcurrentParser::new(config);

        // 创建4个测试文件
        let mut temp_files = Vec::new();
        let test_content = "2024-01-01 12:00:00.000 (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [SEL]: SELECT 1;\nEXECTIME: 10(ms) ROWCOUNT: 1 EXEC_ID: 1.\n";

        for _i in 0..4 {
            let mut temp_file = NamedTempFile::new().unwrap();
            temp_file.write_all(test_content.as_bytes()).unwrap();
            temp_files.push(temp_file);
        }

        let file_paths: Vec<PathBuf> =
            temp_files.iter().map(|f| f.path().to_path_buf()).collect();

        // 使用默认配置（thread_count = None，应该使用文件数量作为线程数）
        let (records, errors) =
            parser.parse_files_concurrent(&file_paths).unwrap();

        assert_eq!(records.len(), 4); // 4个文件，每个1条记录
        assert_eq!(errors.len(), 0); // 没有错误

        println!(
            "默认线程数测试完成: {} 条记录（应该使用4个线程）",
            records.len()
        );
    }
}
