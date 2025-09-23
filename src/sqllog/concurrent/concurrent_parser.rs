//! 并发 SQL 日志解析器主实现

use crate::config::SqllogConfig;
use crate::error::{Result, SqllogError};
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread;

use super::parse_workers;
use super::types::{ParseBatch, ParseTask};

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
use super::types::{ExportTask, ProcessingSummary};

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
use super::export_workers;

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
use std::sync::Mutex;

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

/// 并发 SQL 日志解析器
#[derive(Debug, Clone)]
pub struct ConcurrentParser {
    /// 配置
    config: SqllogConfig,
}

impl ConcurrentParser {
    /// 创建新的并发解析器
    pub fn new(config: SqllogConfig) -> Self {
        #[cfg(feature = "logging")]
        tracing::debug!("创建并发解析器，配置: {:?}", config);

        Self { config }
    }

    /// 并发解析和导出文件
    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    pub fn parse_and_export_concurrent<
        E: SyncExporter + Send + Clone + 'static,
    >(
        &self,
        file_paths: Vec<PathBuf>,
        exporters: Vec<E>,
    ) -> Result<ProcessingSummary> {
        let start_time = Instant::now();

        #[cfg(feature = "logging")]
        tracing::info!(
            "开始并发解析和导出 {} 个文件到 {} 个导出器",
            file_paths.len(),
            exporters.len()
        );

        let parse_start_time = Instant::now();

        // 创建任务通道
        let (task_tx, task_rx) = mpsc::channel::<ParseTask>();
        let task_rx = Arc::new(Mutex::new(task_rx));

        // 为每个导出器创建导出任务通道
        let mut export_channels = Vec::new();
        let mut export_result_channels = Vec::new();

        for _ in &exporters {
            let (export_tx, export_rx) = mpsc::channel::<ExportTask>();
            let (result_tx, _result_rx) = mpsc::channel::<ExportStats>();
            export_channels.push(export_tx);
            export_result_channels.push((export_rx, result_tx));
        }

        let export_task_txs: Vec<_> =
            export_channels.iter().map(|tx| tx.clone()).collect();

        // 启动解析线程
        let mut parse_handles = Vec::new();
        for thread_id in
            0..self.config.thread_count.unwrap_or(4).min(file_paths.len())
        {
            let task_rx = Arc::clone(&task_rx);
            let export_task_txs = export_task_txs.clone();
            let batch_size = self.config.batch_size;

            let handle = thread::spawn(move || {
                parse_workers::parse_worker(
                    thread_id,
                    task_rx,
                    export_task_txs,
                    batch_size,
                )
            });
            parse_handles.push(handle);
        }

        // 启动导出线程
        let mut export_handles = Vec::new();

        for (thread_id, (exporter, (export_rx, result_tx))) in
            exporters.into_iter().zip(export_result_channels).enumerate()
        {
            let handle = thread::spawn(move || {
                export_workers::export_worker(
                    thread_id, exporter, export_rx, result_tx,
                )
            });
            export_handles.push(handle);
        }

        // 发送解析任务
        for file_path in file_paths {
            let task =
                ParseTask { file_path, batch_size: self.config.batch_size };
            task_tx.send(task).map_err(|e| {
                SqllogError::other(format!("发送解析任务失败: {}", e))
            })?;
        }

        // 关闭任务发送通道，让解析线程知道没有更多任务
        drop(task_tx);

        // 等待解析线程完成
        for (i, handle) in parse_handles.into_iter().enumerate() {
            if let Err(e) = handle.join() {
                #[cfg(feature = "logging")]
                tracing::error!("解析线程 {} panic: {:?}", i, e);
            }
        }

        let parse_duration = parse_start_time.elapsed();
        let export_start_time = Instant::now();

        // 关闭导出任务发送通道
        drop(export_channels);

        // 等待导出线程完成并收集结果
        for (i, handle) in export_handles.into_iter().enumerate() {
            if let Err(e) = handle.join() {
                #[cfg(feature = "logging")]
                tracing::error!("导出线程 {} panic: {:?}", i, e);
            }
        }

        let export_duration = export_start_time.elapsed();
        let total_duration = start_time.elapsed();

        #[cfg(feature = "logging")]
        tracing::info!(
            "并发解析和导出完成，总耗时: {:?}，解析耗时: {:?}，导出耗时: {:?}",
            total_duration,
            parse_duration,
            export_duration
        );

        Ok(ProcessingSummary {
            total_duration,
            parse_duration,
            export_duration,
            parse_errors: Vec::new(), // TODO: 收集解析错误
        })
    }

    /// 并发解析文件（不导出）
    pub fn parse_files_concurrent(
        &self,
        file_paths: &[PathBuf],
    ) -> Result<(
        Vec<crate::sqllog::types::Sqllog>,
        Vec<crate::sqllog::sync_parser::ParseError>,
    )> {
        let start_time = std::time::Instant::now();

        #[cfg(feature = "logging")]
        tracing::info!("开始并发解析 {} 个文件", file_paths.len());

        if file_paths.is_empty() {
            #[cfg(feature = "logging")]
            tracing::warn!("文件列表为空，跳过解析");
            return Ok((Vec::new(), Vec::new()));
        }

        let parse_thread_count = match self.config.thread_count {
            Some(0) | None => file_paths.len(),
            Some(count) => count,
        }
        .min(file_paths.len());

        #[cfg(feature = "logging")]
        tracing::trace!("文件列表: {:?}", file_paths);

        let (result_tx, result_rx) = mpsc::channel::<ParseBatch>();

        // 启动解析线程 - 每个线程处理一个文件
        let mut handles = Vec::new();
        #[cfg(feature = "logging")]
        tracing::debug!(
            "为前 {} 个文件分别创建解析线程",
            parse_thread_count.min(file_paths.len())
        );

        for (thread_id, file_path) in
            file_paths.iter().take(parse_thread_count).enumerate()
        {
            let result_tx = result_tx.clone();
            let batch_size = self.config.batch_size;
            let file_path = file_path.clone();

            #[cfg(feature = "logging")]
            tracing::debug!(
                "创建解析线程 {} 处理文件: {}",
                thread_id,
                file_path.display()
            );

            let handle = thread::spawn(move || {
                parse_workers::simple_parse_single_file(
                    thread_id, file_path, result_tx, batch_size,
                )
            });
            handles.push(handle);
        }

        // 如果文件数量大于线程数，剩余文件用任务分发方式处理
        if file_paths.len() > parse_thread_count {
            let remaining_files = &file_paths[parse_thread_count..];
            #[cfg(feature = "logging")]
            tracing::debug!(
                "剩余 {} 个文件需要用任务分发方式处理",
                remaining_files.len()
            );

            let (task_tx, task_rx) = mpsc::channel::<ParseTask>();
            let task_rx = Arc::new(std::sync::Mutex::new(task_rx));

            // 启动额外的工作线程
            let additional_threads =
                file_paths.len().min(parse_thread_count * 2)
                    - parse_thread_count;
            #[cfg(feature = "logging")]
            tracing::debug!(
                "创建 {} 个额外的工作线程处理剩余文件",
                additional_threads
            );

            for thread_id in
                parse_thread_count..parse_thread_count + additional_threads
            {
                let task_rx = Arc::clone(&task_rx);
                let result_tx = result_tx.clone();
                let batch_size = self.config.batch_size;

                #[cfg(feature = "logging")]
                tracing::debug!("创建额外工作线程 {}", thread_id);

                let handle = thread::spawn(move || {
                    parse_workers::simple_parse_worker_with_shared_rx(
                        thread_id, task_rx, result_tx, batch_size,
                    )
                });
                handles.push(handle);
            }

            // 分发剩余文件
            for (i, file_path) in remaining_files.iter().enumerate() {
                let task = ParseTask {
                    file_path: file_path.clone(),
                    batch_size: self.config.batch_size,
                };
                #[cfg(feature = "logging")]
                tracing::trace!(
                    "分发剩余文件任务 {}: {}",
                    i,
                    file_path.display()
                );

                task_tx.send(task).map_err(|e| {
                    SqllogError::other(format!("发送任务失败: {}", e))
                })?;
            }
            drop(task_tx);
        }

        drop(result_tx);

        #[cfg(feature = "logging")]
        tracing::debug!("等待所有解析线程完成");

        // 等待所有线程完成
        for (i, handle) in handles.into_iter().enumerate() {
            if let Err(e) = handle.join() {
                #[cfg(feature = "logging")]
                tracing::error!("解析线程 {} panic: {:?}", i, e);
            }
        }

        // 收集所有结果
        let mut all_batches = Vec::new();
        while let Ok(batch) = result_rx.try_recv() {
            all_batches.push(batch);
        }

        // 将批次合并为单一结果
        let mut all_records = Vec::new();
        let mut all_errors = Vec::new();

        for batch in all_batches {
            all_records.extend(batch.records);
            all_errors.extend(batch.errors);
        }

        let elapsed = start_time.elapsed();
        #[cfg(feature = "logging")]
        tracing::info!(
            "并发解析完成，处理了 {} 个文件，总记录: {}, 总错误: {}，耗时: {:?}",
            file_paths.len(),
            all_records.len(),
            all_errors.len(),
            elapsed
        );

        Ok((all_records, all_errors))
    }
}

impl Default for ConcurrentParser {
    fn default() -> Self {
        Self::new(SqllogConfig::default())
    }
}
