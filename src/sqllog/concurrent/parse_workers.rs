//! 解析工作线程相关功能

use crate::error::Result;
use std::path::PathBuf;
use std::sync::{Arc, mpsc};

use super::types::{ParseBatch, ParseTask};

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
use super::types::ExportTask;

/// 通用解析文件函数，减少重复代码
pub fn parse_file_to_batch(
    thread_id: usize,
    file_path: &PathBuf,
    batch_size: usize,
    task_description: &str,
) -> Result<ParseBatch> {
    #[cfg(feature = "logging")]
    let task_start_time = std::time::Instant::now();

    let mut all_records = Vec::new();
    let mut all_errors = Vec::new();
    let mut batch_counter = 0;

    let parse_result = crate::sqllog::SyncSqllogParser::parse_with_hooks(
        file_path,
        batch_size,
        |batch_records, batch_errors| {
            all_records.extend_from_slice(batch_records);
            all_errors.extend_from_slice(batch_errors);
            batch_counter += 1;

            #[cfg(feature = "logging")]
            tracing::trace!(
                "线程 {} {} 批次 {}: {} 条记录, {} 个错误",
                thread_id,
                task_description,
                batch_counter,
                batch_records.len(),
                batch_errors.len()
            );
        },
    );

    match parse_result {
        Ok(_) => {
            #[cfg(feature = "logging")]
            {
                let task_elapsed = task_start_time.elapsed();
                tracing::info!(
                    "线程 {} 成功{}: {}，总记录: {}, 总错误: {}, 批次: {}, 耗时: {:?}",
                    thread_id,
                    task_description,
                    file_path.display(),
                    all_records.len(),
                    all_errors.len(),
                    batch_counter,
                    task_elapsed
                );
            }

            Ok(ParseBatch {
                records: all_records,
                errors: all_errors,
                source_file: file_path.clone(),
                batch_id: batch_counter,
            })
        }
        Err(e) => {
            #[cfg(feature = "logging")]
            tracing::error!(
                "线程 {} {}失败: {}, 错误: {}",
                thread_id,
                task_description,
                file_path.display(),
                e
            );
            Err(e)
        }
    }
}

/// 解析单个文件的工作线程
pub fn simple_parse_single_file(
    thread_id: usize,
    file_path: PathBuf,
    result_tx: mpsc::Sender<ParseBatch>,
    batch_size: usize,
) -> Result<()> {
    let batch =
        parse_file_to_batch(thread_id, &file_path, batch_size, "处理单文件")?;

    if let Err(e) = result_tx.send(batch) {
        #[cfg(feature = "logging")]
        tracing::error!("线程 {} 发送结果失败: {}", thread_id, e);
    }

    Ok(())
}

/// 使用共享接收器的简化解析工作线程
pub fn simple_parse_worker_with_shared_rx(
    thread_id: usize,
    task_rx: Arc<std::sync::Mutex<mpsc::Receiver<ParseTask>>>,
    result_tx: mpsc::Sender<ParseBatch>,
    batch_size: usize,
) -> Result<()> {
    #[cfg(feature = "logging")]
    tracing::debug!("共享解析工作线程 {} 启动", thread_id);

    let mut processed_tasks = 0;

    loop {
        let task = {
            let rx = task_rx.lock().unwrap();
            match rx.recv() {
                Ok(task) => {
                    #[cfg(feature = "logging")]
                    tracing::trace!(
                        "线程 {} 接收到共享任务: {}",
                        thread_id,
                        task.file_path.display()
                    );
                    task
                }
                Err(_) => {
                    #[cfg(feature = "logging")]
                    tracing::trace!(
                        "线程 {} 共享任务通道关闭，准备退出",
                        thread_id
                    );
                    break; // 通道关闭，退出循环
                }
            }
        };

        // 使用统一的解析函数
        match parse_file_to_batch(
            thread_id,
            &task.file_path,
            batch_size,
            "处理共享任务",
        ) {
            Ok(batch) => {
                if let Err(e) = result_tx.send(batch) {
                    #[cfg(feature = "logging")]
                    tracing::error!("线程 {} 发送结果失败: {}", thread_id, e);
                }
            }
            Err(e) => {
                #[cfg(feature = "logging")]
                tracing::error!("线程 {} 解析共享任务失败: {}", thread_id, e);
            }
        }

        processed_tasks += 1;
    }

    #[cfg(feature = "logging")]
    tracing::debug!(
        "共享解析工作线程 {} 退出，处理了 {} 个任务",
        thread_id,
        processed_tasks
    );

    Ok(())
}

/// 解析工作线程（用于解析+导出流水线）
#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
pub fn parse_worker(
    thread_id: usize,
    task_rx: Arc<std::sync::Mutex<mpsc::Receiver<ParseTask>>>,
    export_task_txs: Vec<mpsc::Sender<ExportTask>>,
    batch_size: usize,
) -> Result<()> {
    #[cfg(feature = "logging")]
    tracing::debug!("解析工作线程 {} 启动", thread_id);

    let mut processed_files = 0;
    let mut total_records = 0;
    let mut total_errors = 0;

    loop {
        let task = {
            let rx = task_rx.lock().unwrap();
            match rx.recv() {
                Ok(task) => {
                    #[cfg(feature = "logging")]
                    tracing::trace!(
                        "线程 {} 接收到任务: {}",
                        thread_id,
                        task.file_path.display()
                    );
                    task
                }
                Err(_) => {
                    #[cfg(feature = "logging")]
                    tracing::trace!(
                        "线程 {} 任务通道关闭，准备退出",
                        thread_id
                    );
                    break; // 通道关闭，退出循环
                }
            }
        };

        #[cfg(feature = "logging")]
        tracing::debug!(
            "线程 {} 开始解析文件: {}",
            thread_id,
            task.file_path.display()
        );

        // 记录解析任务开始时间
        #[cfg(feature = "logging")]
        let parse_task_start_time = std::time::Instant::now();

        let mut file_errors = Vec::new();
        let mut export_task_id = 0;

        // 流式解析文件，分批发送到导出线程
        let parse_result = crate::sqllog::SyncSqllogParser::parse_with_hooks(
            &task.file_path,
            batch_size,
            |batch_records, batch_errors| {
                // 收集解析错误
                file_errors.extend_from_slice(batch_errors);

                #[cfg(feature = "logging")]
                tracing::trace!(
                    "线程 {} 批次 {}: {} 条记录, {} 个错误",
                    thread_id,
                    export_task_id,
                    batch_records.len(),
                    batch_errors.len()
                );

                // 如果有记录，发送到所有导出线程
                if !batch_records.is_empty() {
                    total_records += batch_records.len();
                    let export_task = ExportTask {
                        records: batch_records.to_vec(),
                        task_id: export_task_id,
                        source_file: task.file_path.clone(),
                    };

                    // 将任务发送给所有导出器
                    for (exporter_id, tx) in export_task_txs.iter().enumerate()
                    {
                        #[cfg(feature = "logging")]
                        tracing::trace!(
                            "线程 {} 向导出器 {} 发送批次 {}",
                            thread_id,
                            exporter_id,
                            export_task_id
                        );

                        if let Err(e) = tx.send(export_task.clone()) {
                            #[cfg(feature = "logging")]
                            tracing::error!(
                                "线程 {} 向导出器 {} 发送任务失败: {}",
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
                {
                    let parse_task_elapsed = parse_task_start_time.elapsed();
                    tracing::info!(
                        "线程 {} 成功解析文件: {}，总记录: {}, 总错误: {}, 解析任务耗时: {:?}",
                        thread_id,
                        task.file_path.display(),
                        total_records,
                        file_errors.len(),
                        parse_task_elapsed
                    );
                }

                total_errors += file_errors.len();
                processed_files += 1;
            }
            Err(e) => {
                #[cfg(feature = "logging")]
                {
                    let parse_task_elapsed = parse_task_start_time.elapsed();
                    tracing::error!(
                        "线程 {} 解析文件失败: {}, 错误: {}, 解析任务耗时: {:?}",
                        thread_id,
                        task.file_path.display(),
                        e,
                        parse_task_elapsed
                    );
                }
                total_errors += 1;
            }
        }
    }

    #[cfg(feature = "logging")]
    tracing::info!(
        "解析工作线程 {} 退出，处理了 {} 个文件，总记录: {}, 总错误: {}",
        thread_id,
        processed_files,
        total_records,
        total_errors
    );

    Ok(())
}
