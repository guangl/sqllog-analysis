//! 导出工作线程相关功能

use crate::error::Result;
use crate::exporter::{ExportStats, SyncExporter};
use std::sync::mpsc;
use std::time::Instant;

use super::types::ExportTask;

/// 导出工作线程
pub fn export_worker<E: SyncExporter + Send + 'static>(
    thread_id: usize,
    mut exporter: E,
    task_rx: mpsc::Receiver<ExportTask>,
    result_tx: mpsc::Sender<ExportStats>,
) -> Result<()> {
    #[cfg(feature = "logging")]
    tracing::debug!("导出工作线程 {} 启动", thread_id);

    let mut processed_batches = 0;
    let mut total_records = 0;

    for export_task in task_rx.iter() {
        #[cfg(feature = "logging")]
        tracing::trace!(
            "导出线程 {} 接收到任务 {}",
            thread_id,
            export_task.task_id
        );

        // 记录导出任务开始时间
        #[cfg(feature = "logging")]
        let export_task_start_time = Instant::now();

        match exporter.export_batch(&export_task.records) {
            Ok(_) => {
                #[cfg(feature = "logging")]
                {
                    let export_task_elapsed = export_task_start_time.elapsed();
                    tracing::info!(
                        "导出线程 {} 成功导出任务 {}，记录数: {}，耗时: {:?}",
                        thread_id,
                        export_task.task_id,
                        export_task.records.len(),
                        export_task_elapsed
                    );
                }

                total_records += export_task.records.len();

                // 创建统计信息
                let mut stats = ExportStats::new();
                stats.exported_records = export_task.records.len();
                stats.finish();

                if let Err(e) = result_tx.send(stats) {
                    #[cfg(feature = "logging")]
                    tracing::error!(
                        "导出线程 {} 发送统计结果失败: {}",
                        thread_id,
                        e
                    );
                }
            }
            Err(e) => {
                #[cfg(feature = "logging")]
                {
                    let export_task_elapsed = export_task_start_time.elapsed();
                    tracing::error!(
                        "导出线程 {} 导出任务 {} 失败: {}，耗时: {:?}",
                        thread_id,
                        export_task.task_id,
                        e,
                        export_task_elapsed
                    );
                }
            }
        }

        processed_batches += 1;
    }

    #[cfg(feature = "logging")]
    tracing::info!(
        "导出工作线程 {} 退出，处理了 {} 个批次，总记录: {}",
        thread_id,
        processed_batches,
        total_records
    );

    Ok(())
}