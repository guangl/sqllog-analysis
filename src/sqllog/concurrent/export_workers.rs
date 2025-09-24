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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqllog::types::Sqllog;
    use std::path::PathBuf;
    use std::sync::mpsc;

    // exporter that always succeeds
    struct SuccessExporter;
    impl crate::exporter::sync_impl::SyncExporter for SuccessExporter {
        fn name(&self) -> &str {
            "SUCCESS"
        }
        fn export_record(
            &mut self,
            _record: &Sqllog,
        ) -> crate::error::Result<()> {
            Ok(())
        }
    }

    // exporter that always fails
    struct FailExporter;
    impl crate::exporter::sync_impl::SyncExporter for FailExporter {
        fn name(&self) -> &str {
            "FAIL"
        }
        fn export_record(
            &mut self,
            _record: &Sqllog,
        ) -> crate::error::Result<()> {
            Err(crate::error::SqllogError::other("fail"))
        }
    }

    #[test]
    fn test_export_worker_success_sends_stats() {
        let (task_tx, task_rx) = mpsc::channel();
        let (res_tx, res_rx) = mpsc::channel();

        // send one task
        let task = ExportTask {
            task_id: 1,
            records: vec![Sqllog::default(), Sqllog::default()],
            source_file: PathBuf::from("test"),
        };
        task_tx.send(task).unwrap();
        // close sender so worker will exit after processing
        drop(task_tx);

        // run worker
        let res = export_worker(0, SuccessExporter, task_rx, res_tx);
        assert!(res.is_ok());

        // we expect one stats sent
        let stats = res_rx.recv().unwrap();
        assert_eq!(stats.exported_records, 2);
    }

    #[test]
    fn test_export_worker_failure_does_not_send_stats() {
        let (task_tx, task_rx) = mpsc::channel();
        let (res_tx, res_rx) = mpsc::channel();

        let task = ExportTask {
            task_id: 2,
            records: vec![Sqllog::default()],
            source_file: PathBuf::from("test"),
        };
        task_tx.send(task).unwrap();
        drop(task_tx);

        let res = export_worker(1, FailExporter, task_rx, res_tx);
        assert!(res.is_ok());

        // channel should be closed or have no messages
        assert!(res_rx.try_recv().is_err());
    }

    #[test]
    fn test_export_worker_send_error_logged_but_ok() {
        let (task_tx, task_rx) = mpsc::channel();
        let (res_tx, res_rx) = mpsc::channel();

        // drop receiver so send will fail inside worker
        drop(res_rx);

        let task = ExportTask {
            task_id: 3,
            records: vec![Sqllog::default()],
            source_file: PathBuf::from("test"),
        };
        task_tx.send(task).unwrap();
        drop(task_tx);

        let res = export_worker(2, SuccessExporter, task_rx, res_tx);
        // worker should return Ok even if send failed
        assert!(res.is_ok());
    }

    #[test]
    fn test_export_worker_processes_multiple_tasks_and_sends_stats() {
        let (task_tx, task_rx) = mpsc::channel();
        let (res_tx, res_rx) = mpsc::channel();

        let task1 = ExportTask {
            task_id: 4,
            records: vec![Sqllog::default()],
            source_file: PathBuf::from("t1"),
        };
        let task2 = ExportTask {
            task_id: 5,
            records: vec![
                Sqllog::default(),
                Sqllog::default(),
                Sqllog::default(),
            ],
            source_file: PathBuf::from("t2"),
        };
        task_tx.send(task1).unwrap();
        task_tx.send(task2).unwrap();
        drop(task_tx);

        let res = export_worker(3, SuccessExporter, task_rx, res_tx);
        assert!(res.is_ok());

        // should receive two stats
        let s1 = res_rx.recv().unwrap();
        let s2 = res_rx.recv().unwrap();
        assert!(s1.exported_records == 1 || s2.exported_records == 1);
        assert!(s1.exported_records + s2.exported_records == 4);
    }

    #[test]
    fn test_export_worker_no_tasks_exits_ok() {
        // no tasks sent, channel closed immediately
        let (task_tx, task_rx) = mpsc::channel();
        let (res_tx, _res_rx) = mpsc::channel();
        drop(task_tx);

        let res = export_worker(4, SuccessExporter, task_rx, res_tx);
        assert!(res.is_ok());
    }

    #[test]
    fn test_export_worker_empty_records_sends_zero() {
        let (task_tx, task_rx) = mpsc::channel();
        let (res_tx, res_rx) = mpsc::channel();

        let task = ExportTask {
            task_id: 6,
            records: vec![],
            source_file: PathBuf::from("empty"),
        };
        task_tx.send(task).unwrap();
        drop(task_tx);

        let res = export_worker(5, SuccessExporter, task_rx, res_tx);
        assert!(res.is_ok());

        let stats = res_rx.recv().unwrap();
        assert_eq!(stats.exported_records, 0);
    }
}
