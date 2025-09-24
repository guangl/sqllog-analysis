//! 导出工作线程测试

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
mod export_worker_tests {
    use sqllog_analysis::exporter::ExportStats;
    use sqllog_analysis::exporter::sync_impl::SyncExporter;
    use sqllog_analysis::prelude::*;
    use sqllog_analysis::sqllog::concurrent::export_workers::export_worker;
    use sqllog_analysis::sqllog::concurrent::types::ExportTask;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;

    // 测试用的导出器实现
    struct TestExporter {
        name: String,
        exported_records: Arc<Mutex<Vec<Sqllog>>>,
        export_delay: Option<Duration>,
        fail_on_batch: Option<usize>,
        call_count: Arc<Mutex<usize>>,
    }

    impl TestExporter {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                exported_records: Arc::new(Mutex::new(Vec::new())),
                export_delay: None,
                fail_on_batch: None,
                call_count: Arc::new(Mutex::new(0)),
            }
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.export_delay = Some(delay);
            self
        }

        fn with_failure_on_batch(mut self, batch_number: usize) -> Self {
            self.fail_on_batch = Some(batch_number);
            self
        }

        #[allow(dead_code)]
        fn get_exported_records(&self) -> Vec<Sqllog> {
            self.exported_records.lock().unwrap().clone()
        }

        #[allow(dead_code)]
        fn get_call_count(&self) -> usize {
            *self.call_count.lock().unwrap()
        }
    }

    impl SyncExporter for TestExporter {
        fn name(&self) -> &str {
            &self.name
        }

        fn export_record(&mut self, record: &Sqllog) -> Result<()> {
            let mut call_count = self.call_count.lock().unwrap();
            *call_count += 1;

            if let Some(fail_batch) = self.fail_on_batch {
                if *call_count >= fail_batch {
                    return Err(
                        sqllog_analysis::error::SqllogError::parse_error(
                            "Test export failure",
                        )
                        .into(),
                    );
                }
            }

            if let Some(delay) = self.export_delay {
                std::thread::sleep(delay);
            }

            self.exported_records.lock().unwrap().push(record.clone());
            Ok(())
        }

        fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
            let mut call_count = self.call_count.lock().unwrap();
            *call_count += 1;

            if let Some(fail_batch) = self.fail_on_batch {
                if *call_count >= fail_batch {
                    return Err(
                        sqllog_analysis::error::SqllogError::parse_error(
                            "Test export failure",
                        )
                        .into(),
                    );
                }
            }

            if let Some(delay) = self.export_delay {
                std::thread::sleep(delay);
            }

            for record in records {
                self.exported_records.lock().unwrap().push(record.clone());
            }
            Ok(())
        }

        fn get_stats(&self) -> ExportStats {
            let mut stats = ExportStats::new();
            stats.exported_records =
                self.exported_records.lock().unwrap().len();
            stats.finish();
            stats
        }
    }

    fn create_test_record(id: u64, sql: &str) -> Sqllog {
        Sqllog {
            occurrence_time: format!("2023-09-16 20:02:53.{:03}", id % 1000),
            ep: format!("EP[{}]", 0),
            session: Some(format!("0x{:x}", id)),
            thread: Some(format!("{}", id)),
            user: Some("TEST_USER".to_string()),
            trx_id: Some(format!("{}", id)),
            statement: Some(format!("0x{:x}", id + 1000)),
            sql_type: Some("SELECT".to_string()),
            description: sql.to_string(),
            appname: Some("TEST_APP".to_string()),
            ip: Some("127.0.0.1".to_string()),
            execute_id: Some(id as i64 + 2000),
            execute_time: Some(10),
            rowcount: Some(100),
        }
    }

    #[test]
    fn test_export_worker_single_task() {
        let exporter = TestExporter::new("test_single");
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建测试任务
        let records = vec![
            create_test_record(1, "SELECT * FROM test1"),
            create_test_record(2, "SELECT * FROM test2"),
        ];
        let task = ExportTask {
            task_id: 1,
            records: records.clone(),
            source_file: PathBuf::from("test.log"),
        };

        // 发送任务
        task_tx.send(task).unwrap();
        drop(task_tx);

        // 启动工作线程
        let exported_records = exporter.exported_records.clone();
        let handle = std::thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成并验证结果
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证导出的记录
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), 2);
        assert_eq!(exported[0].description, "SELECT * FROM test1");
        assert_eq!(exported[1].description, "SELECT * FROM test2");

        // 验证统计结果
        let stats = result_rx.recv().unwrap();
        assert_eq!(stats.exported_records, 2);
    }

    #[test]
    fn test_export_worker_multiple_tasks() {
        let exporter = TestExporter::new("test_multiple");
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建多个测试任务
        let tasks = vec![
            ExportTask {
                task_id: 1,
                records: vec![create_test_record(1, "SELECT 1")],
                source_file: PathBuf::from("test1.log"),
            },
            ExportTask {
                task_id: 2,
                records: vec![
                    create_test_record(2, "SELECT 2"),
                    create_test_record(3, "SELECT 3"),
                ],
                source_file: PathBuf::from("test2.log"),
            },
            ExportTask {
                task_id: 3,
                records: vec![create_test_record(4, "SELECT 4")],
                source_file: PathBuf::from("test3.log"),
            },
        ];

        // 发送所有任务
        for task in tasks {
            task_tx.send(task).unwrap();
        }
        drop(task_tx);

        // 启动工作线程
        let exported_records = exporter.exported_records.clone();
        let handle = std::thread::spawn(move || {
            export_worker(1, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证导出的记录
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), 4);

        // 验证统计结果 - 每个任务都会产生一个统计结果
        let mut total_exported = 0;
        for _ in 0..3 {
            let stats = result_rx.recv().unwrap();
            total_exported += stats.exported_records;
        }
        assert_eq!(total_exported, 4);
    }

    #[test]
    fn test_export_worker_empty_tasks() {
        let exporter = TestExporter::new("test_empty");
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建包含空记录的任务
        let task = ExportTask {
            task_id: 1,
            records: vec![],
            source_file: PathBuf::from("empty.log"),
        };

        task_tx.send(task).unwrap();
        drop(task_tx);

        // 启动工作线程
        let exported_records = exporter.exported_records.clone();
        let handle = std::thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证没有导出记录
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), 0);

        // 验证统计结果
        let stats = result_rx.recv().unwrap();
        assert_eq!(stats.exported_records, 0);
    }

    #[test]
    fn test_export_worker_no_tasks() {
        let exporter = TestExporter::new("test_no_tasks");
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, _result_rx) = mpsc::channel();

        // 立即关闭发送端，无任务
        drop(task_tx);

        // 启动工作线程
        let exported_records = exporter.exported_records.clone();
        let handle = std::thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证没有导出记录
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), 0);
    }

    #[test]
    fn test_export_worker_export_failure() {
        let exporter =
            TestExporter::new("test_failure").with_failure_on_batch(2); // 第二次调用时失败
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建两个任务，第二个会失败
        let tasks = vec![
            ExportTask {
                task_id: 1,
                records: vec![create_test_record(1, "SELECT 1")],
                source_file: PathBuf::from("fail1.log"),
            },
            ExportTask {
                task_id: 2,
                records: vec![create_test_record(2, "SELECT 2")],
                source_file: PathBuf::from("fail2.log"),
            },
        ];

        for task in tasks {
            task_tx.send(task).unwrap();
        }
        drop(task_tx);

        // 启动工作线程
        let exported_records = exporter.exported_records.clone();
        let handle = std::thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok()); // 工作线程本身不应该失败

        // 验证只有第一个任务被导出
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), 1);
        assert_eq!(exported[0].description, "SELECT 1");

        // 验证只收到第一个任务的统计结果
        let stats = result_rx.recv().unwrap();
        assert_eq!(stats.exported_records, 1);

        // 第二个任务失败，不应该有统计结果
        assert!(result_rx.try_recv().is_err());
    }

    #[test]
    fn test_export_worker_large_batches() {
        let exporter = TestExporter::new("test_large");
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建大批量任务
        let mut large_batch = Vec::new();
        for i in 1..=1000 {
            large_batch.push(create_test_record(i, &format!("SELECT {}", i)));
        }

        let task = ExportTask {
            task_id: 1,
            records: large_batch.clone(),
            source_file: PathBuf::from("large.log"),
        };

        task_tx.send(task).unwrap();
        drop(task_tx);

        // 启动工作线程
        let exported_records = exporter.exported_records.clone();
        let handle = std::thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证所有记录都被导出
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), 1000);

        // 验证统计结果
        let stats = result_rx.recv().unwrap();
        assert_eq!(stats.exported_records, 1000);
    }

    #[test]
    fn test_export_worker_concurrent_access() {
        let exporter = TestExporter::new("test_concurrent");
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建多个并发任务
        let task_count = 10;
        for i in 1..=task_count {
            let records = vec![
                create_test_record(i * 10, &format!("SELECT {} * 10", i)),
                create_test_record(
                    i * 10 + 1,
                    &format!("SELECT {} * 10 + 1", i),
                ),
            ];
            let task = ExportTask {
                task_id: i as usize,
                records,
                source_file: PathBuf::from(format!("concurrent_{}.log", i)),
            };
            task_tx.send(task).unwrap();
        }
        drop(task_tx);

        // 启动工作线程
        let exported_records = exporter.exported_records.clone();
        let handle = std::thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证所有记录都被导出
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), (task_count * 2) as usize);

        // 验证所有统计结果
        let mut total_exported = 0;
        for _ in 0..task_count {
            let stats = result_rx.recv().unwrap();
            total_exported += stats.exported_records;
        }
        assert_eq!(total_exported, (task_count * 2) as usize);
    }

    #[test]
    fn test_export_worker_with_delay() {
        let start_time = std::time::Instant::now();
        let exporter = TestExporter::new("test_delay")
            .with_delay(Duration::from_millis(50));
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建单个任务
        let task = ExportTask {
            task_id: 1,
            records: vec![create_test_record(1, "SELECT WITH DELAY")],
            source_file: PathBuf::from("delay.log"),
        };

        task_tx.send(task).unwrap();
        drop(task_tx);

        // 启动工作线程
        let exported_records = exporter.exported_records.clone();
        let handle = std::thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证处理时间至少包含延迟
        let elapsed = start_time.elapsed();
        assert!(elapsed >= Duration::from_millis(50));

        // 验证记录被导出
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), 1);

        // 验证统计结果
        let stats = result_rx.recv().unwrap();
        assert_eq!(stats.exported_records, 1);
    }

    #[test]
    fn test_export_worker_result_channel_closed() {
        let exporter = TestExporter::new("test_closed_result");
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 提前关闭结果接收端
        drop(result_rx);

        let task = ExportTask {
            task_id: 1,
            records: vec![create_test_record(1, "SELECT 1")],
            source_file: PathBuf::from("closed.log"),
        };

        task_tx.send(task).unwrap();
        drop(task_tx);

        // 启动工作线程
        let exported_records = exporter.exported_records.clone();
        let handle = std::thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成 - 应该仍然成功完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证记录仍然被导出
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), 1);
    }

    #[test]
    fn test_export_worker_stats_creation() {
        let exporter = TestExporter::new("test_stats");
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建不同大小的任务
        let tasks = vec![
            ExportTask {
                task_id: 1,
                records: vec![create_test_record(1, "SELECT 1")],
                source_file: PathBuf::from("stats1.log"),
            },
            ExportTask {
                task_id: 2,
                records: vec![
                    create_test_record(2, "SELECT 2"),
                    create_test_record(3, "SELECT 3"),
                    create_test_record(4, "SELECT 4"),
                ],
                source_file: PathBuf::from("stats2.log"),
            },
            ExportTask {
                task_id: 3,
                records: vec![], // 空任务
                source_file: PathBuf::from("stats3.log"),
            },
        ];

        for task in tasks {
            task_tx.send(task).unwrap();
        }
        drop(task_tx);

        // 启动工作线程
        let handle = std::thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证每个任务都有对应的统计结果
        let stats1 = result_rx.recv().unwrap();
        assert_eq!(stats1.exported_records, 1);

        let stats2 = result_rx.recv().unwrap();
        assert_eq!(stats2.exported_records, 3);

        let stats3 = result_rx.recv().unwrap();
        assert_eq!(stats3.exported_records, 0);

        // 确保没有更多统计结果
        assert!(result_rx.try_recv().is_err());
    }

    #[test]
    fn test_export_worker_thread_id_logging() {
        // 这个测试主要验证不同线程ID的工作线程能正常工作
        let mut handles = Vec::new();
        let mut result_receivers = Vec::new();

        for thread_id in 0..3 {
            let exporter =
                TestExporter::new(&format!("test_thread_{}", thread_id));
            let (task_tx, task_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            result_receivers.push(result_rx);

            let task = ExportTask {
                task_id: (thread_id + 1) as usize,
                records: vec![create_test_record(
                    (thread_id + 1) as u64,
                    &format!("SELECT FROM THREAD {}", thread_id),
                )],
                source_file: PathBuf::from(format!("thread_{}.log", thread_id)),
            };

            task_tx.send(task).unwrap();
            drop(task_tx);

            let handle = std::thread::spawn(move || {
                export_worker(thread_id, exporter, task_rx, result_tx)
            });

            handles.push(handle);
        }

        // 等待所有线程完成
        for handle in handles {
            let result = handle.join().unwrap();
            assert!(result.is_ok());
        }

        // 验证所有结果
        for result_rx in result_receivers {
            let stats = result_rx.recv().unwrap();
            assert_eq!(stats.exported_records, 1);
        }
    }
} // 关闭 export_worker_tests 模块
