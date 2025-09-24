//! export_workers.rs 的高级单元测试
//! 专注于测试未覆盖的代码路径和边界情况

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
#[cfg(test)]
mod advanced_export_worker_tests {
    use sqllog_analysis::exporter::{ExportStats, SyncExporter};
    use sqllog_analysis::sqllog::concurrent::export_workers::export_worker;
    use sqllog_analysis::sqllog::concurrent::types::ExportTask;
    use sqllog_analysis::sqllog::types::Sqllog;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    // 高级测试导出器，支持更多的测试场景
    struct AdvancedTestExporter {
        name: String,
        exported_records: Arc<Mutex<Vec<Sqllog>>>,
        call_count: Arc<Mutex<usize>>,
        fail_condition: Option<Box<dyn Fn(usize) -> bool + Send + Sync>>,
        delay_per_record: Option<Duration>,
        max_batch_size: Option<usize>,
        stats: Arc<Mutex<ExportStats>>,
    }

    impl AdvancedTestExporter {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                exported_records: Arc::new(Mutex::new(Vec::new())),
                call_count: Arc::new(Mutex::new(0)),
                fail_condition: None,
                delay_per_record: None,
                max_batch_size: None,
                stats: Arc::new(Mutex::new(ExportStats::new())),
            }
        }

        fn with_fail_condition<F>(mut self, condition: F) -> Self
        where
            F: Fn(usize) -> bool + Send + Sync + 'static,
        {
            self.fail_condition = Some(Box::new(condition));
            self
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay_per_record = Some(delay);
            self
        }

        fn with_max_batch_size(mut self, max_size: usize) -> Self {
            self.max_batch_size = Some(max_size);
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

        #[allow(dead_code)]
        fn get_stats(&self) -> ExportStats {
            self.stats.lock().unwrap().clone()
        }
    }

    impl SyncExporter for AdvancedTestExporter {
        fn name(&self) -> &str {
            &self.name
        }

        fn export_record(&mut self, record: &Sqllog) -> sqllog_analysis::error::Result<()> {
            let mut call_count = self.call_count.lock().unwrap();
            *call_count += 1;
            let current_call = *call_count;
            drop(call_count);

            // 检查失败条件
            if let Some(ref fail_condition) = self.fail_condition {
                if fail_condition(current_call) {
                    return Err(sqllog_analysis::error::SqllogError::parse_error(
                        &format!("Export failure at call {}", current_call)
                    ).into());
                }
            }

            // 添加延迟
            if let Some(delay) = self.delay_per_record {
                thread::sleep(delay);
            }

            self.exported_records.lock().unwrap().push(record.clone());

            // 更新统计
            let mut stats = self.stats.lock().unwrap();
            stats.exported_records += 1;

            Ok(())
        }

        fn export_batch(&mut self, records: &[Sqllog]) -> sqllog_analysis::error::Result<()> {
            let mut call_count = self.call_count.lock().unwrap();
            *call_count += 1;
            let current_call = *call_count;
            drop(call_count);

            // 检查失败条件
            if let Some(ref fail_condition) = self.fail_condition {
                if fail_condition(current_call) {
                    return Err(sqllog_analysis::error::SqllogError::parse_error(
                        &format!("Batch export failure at call {}", current_call)
                    ).into());
                }
            }

            // 检查批次大小限制
            if let Some(max_size) = self.max_batch_size {
                if records.len() > max_size {
                    return Err(sqllog_analysis::error::SqllogError::parse_error(
                        &format!("Batch size {} exceeds limit {}", records.len(), max_size)
                    ).into());
                }
            }

            // 添加延迟
            if let Some(delay) = self.delay_per_record {
                thread::sleep(delay * records.len() as u32);
            }

            for record in records {
                self.exported_records.lock().unwrap().push(record.clone());
            }

            // 更新统计
            let mut stats = self.stats.lock().unwrap();
            stats.exported_records += records.len();

            Ok(())
        }

        fn get_stats(&self) -> ExportStats {
            let mut stats = self.stats.lock().unwrap().clone();
            stats.finish();
            stats
        }
    }

    fn create_test_record(id: u64, sql: &str, file: Option<&str>) -> Sqllog {
        Sqllog {
            occurrence_time: format!("2025-09-16 20:02:53.{:03}", id % 1000),
            ep: format!("EP[{}]", id % 10),
            session: Some(format!("0x{:x}", id)),
            thread: Some(format!("{}", id)),
            user: Some("TEST_USER".to_string()),
            trx_id: Some(format!("{}", id)),
            statement: Some(format!("0x{:x}", id + 1000)),
            sql_type: Some("SELECT".to_string()),
            description: format!("{} -- file: {:?}", sql, file.unwrap_or("unknown")),
            appname: Some("TEST_APP".to_string()),
            ip: Some("127.0.0.1".to_string()),
            execute_id: Some(id as i64 + 2000),
            execute_time: Some(10),
            rowcount: Some(100),
        }
    }

    fn create_export_task(task_id: usize, records: Vec<Sqllog>, file: &str) -> ExportTask {
        ExportTask {
            task_id,
            records,
            source_file: PathBuf::from(file),
        }
    }

    #[test]
    fn test_export_worker_high_frequency_tasks() {
        let exporter = AdvancedTestExporter::new("high_frequency");
        let exported_records = exporter.exported_records.clone();
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建大量小任务
        let task_count = 100;
        for i in 1..=task_count {
            let record = create_test_record(i, &format!("SELECT {}", i), Some("high_freq.log"));
            let task = create_export_task(i as usize, vec![record], "high_freq.log");
            task_tx.send(task).unwrap();
        }
        drop(task_tx);

        // 启动工作线程
        let handle = thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证所有记录都被导出
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), task_count as usize);

        // 验证统计结果
        let mut total_stats = 0;
        for _ in 0..task_count {
            let stats = result_rx.recv().unwrap();
            total_stats += stats.exported_records;
        }
        assert_eq!(total_stats, task_count as usize);
    }

    #[test]
    fn test_export_worker_variable_batch_sizes() {
        let exporter = AdvancedTestExporter::new("variable_batch");
        let exported_records = exporter.exported_records.clone();
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建不同大小的批次
        let batch_sizes = vec![1, 5, 10, 20, 50];
        let mut total_expected = 0;

        for (i, batch_size) in batch_sizes.iter().enumerate() {
            let mut records = Vec::new();
            for j in 0..*batch_size {
                let record = create_test_record(
                    (i * 100 + j) as u64,
                    &format!("SELECT batch_{}_record_{}", i, j),
                    Some(&format!("batch_{}.log", i))
                );
                records.push(record);
            }

            let task = create_export_task(i + 1, records, &format!("batch_{}.log", i));
            task_tx.send(task).unwrap();
            total_expected += batch_size;
        }
        drop(task_tx);

        // 启动工作线程
        let handle = thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证导出记录总数
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), total_expected);

        // 验证每个批次的统计结果
        for expected_batch_size in batch_sizes {
            let stats = result_rx.recv().unwrap();
            assert_eq!(stats.exported_records, expected_batch_size);
        }
    }

    #[test]
    fn test_export_worker_conditional_failures() {
        let exporter = AdvancedTestExporter::new("conditional_fail")
            .with_fail_condition(|call_count| call_count % 3 == 0); // 每第3次调用失败

        let exported_records = exporter.exported_records.clone();
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建5个任务
        for i in 1..=5 {
            let record = create_test_record(i, &format!("SELECT {}", i), Some("fail.log"));
            let task = create_export_task(i as usize, vec![record], "fail.log");
            task_tx.send(task).unwrap();
        }
        drop(task_tx);

        // 启动工作线程
        let handle = thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证只有成功的任务被导出 (第1,2,4,5次调用成功，第3次失败)
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), 4); // 4个成功的记录

        // 验证统计结果 - 只有成功的任务有统计
        let mut successful_stats = 0;
        for _ in 0..4 {
            let stats = result_rx.recv().unwrap();
            successful_stats += stats.exported_records;
        }
        assert_eq!(successful_stats, 4);

        // 不应该有更多统计结果
        assert!(result_rx.try_recv().is_err());
    }

    #[test]
    fn test_export_worker_batch_size_limit() {
        let exporter = AdvancedTestExporter::new("size_limit")
            .with_max_batch_size(5); // 限制批次大小为5

        let exported_records = exporter.exported_records.clone();
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建小批次任务（应该成功）
        let mut small_records = Vec::new();
        for i in 1..=3 {
            small_records.push(create_test_record(i, &format!("SELECT small_{}", i), Some("small.log")));
        }
        let small_task = create_export_task(1, small_records, "small.log");
        task_tx.send(small_task).unwrap();

        // 创建大批次任务（应该失败）
        let mut large_records = Vec::new();
        for i in 1..=10 {
            large_records.push(create_test_record(i + 100, &format!("SELECT large_{}", i), Some("large.log")));
        }
        let large_task = create_export_task(2, large_records, "large.log");
        task_tx.send(large_task).unwrap();

        drop(task_tx);

        // 启动工作线程
        let handle = thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证只有小批次被导出
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), 3); // 只有小批次的3条记录

        // 验证统计结果
        let stats = result_rx.recv().unwrap();
        assert_eq!(stats.exported_records, 3);

        // 大批次失败，不应该有统计结果
        assert!(result_rx.try_recv().is_err());
    }

    #[test]
    fn test_export_worker_performance_under_load() {
        let exporter = AdvancedTestExporter::new("performance");
        let exported_records = exporter.exported_records.clone();
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建大量中等大小的任务
        let task_count = 50;
        let batch_size = 20;
        let total_records = task_count * batch_size;

        let start_time = Instant::now();

        for task_id in 1..=task_count {
            let mut records = Vec::new();
            for record_id in 1..=batch_size {
                let global_id = (task_id - 1) * batch_size + record_id;
                let record = create_test_record(
                    global_id as u64,
                    &format!("SELECT performance_test_{}", global_id),
                    Some(&format!("perf_{}.log", task_id))
                );
                records.push(record);
            }

            let task = create_export_task(task_id, records, &format!("perf_{}.log", task_id));
            task_tx.send(task).unwrap();
        }
        drop(task_tx);

        let send_time = start_time.elapsed();

        // 启动工作线程
        let handle = thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        let total_time = start_time.elapsed();

        assert!(result.is_ok());

        // 验证导出记录总数
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), total_records);

        // 验证统计结果
        let mut total_stats = 0;
        for _ in 0..task_count {
            let stats = result_rx.recv().unwrap();
            total_stats += stats.exported_records;
        }
        assert_eq!(total_stats, total_records);

        println!("性能测试: 发送{}个任务耗时{:?}, 总处理耗时{:?}, 处理{}条记录",
                task_count, send_time, total_time, total_records);

        // 性能断言：处理1000条记录应该在合理时间内完成
        assert!(total_time < Duration::from_secs(2),
               "Performance test took too long: {:?}", total_time);
    }

    #[test]
    fn test_export_worker_timing_precision() {
        let delay_per_record = Duration::from_millis(10);
        let exporter = AdvancedTestExporter::new("timing")
            .with_delay(delay_per_record);

        let exported_records = exporter.exported_records.clone();
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建5条记录的任务
        let record_count = 5;
        let mut records = Vec::new();
        for i in 1..=record_count {
            records.push(create_test_record(i, &format!("SELECT timing_{}", i), Some("timing.log")));
        }

        let task = create_export_task(1, records, "timing.log");
        task_tx.send(task).unwrap();
        drop(task_tx);

        let start_time = Instant::now();

        // 启动工作线程
        let handle = thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        let elapsed = start_time.elapsed();

        assert!(result.is_ok());

        // 验证记录被导出
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), record_count as usize);

        // 验证时间包含延迟 (5 records * 10ms = 50ms minimum)
        let expected_min_delay = delay_per_record * record_count as u32;
        assert!(elapsed >= expected_min_delay,
               "Expected at least {:?}, but took {:?}", expected_min_delay, elapsed);

        // 验证统计结果
        let stats = result_rx.recv().unwrap();
        assert_eq!(stats.exported_records, record_count as usize);
    }

    #[test]
    fn test_export_worker_empty_batch_handling() {
        let exporter = AdvancedTestExporter::new("empty_batch");
        let exported_records = exporter.exported_records.clone();
        let call_count = exporter.call_count.clone();
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建空批次任务
        let empty_task = create_export_task(1, vec![], "empty.log");
        task_tx.send(empty_task).unwrap();

        // 创建非空批次任务
        let record = create_test_record(1, "SELECT non_empty", Some("non_empty.log"));
        let non_empty_task = create_export_task(2, vec![record], "non_empty.log");
        task_tx.send(non_empty_task).unwrap();

        drop(task_tx);

        // 启动工作线程
        let handle = thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证导出记录
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), 1); // 只有非空任务的记录

        // 验证调用次数
        assert_eq!(*call_count.lock().unwrap(), 2); // 两个任务都被处理了

        // 验证统计结果
        let empty_stats = result_rx.recv().unwrap();
        assert_eq!(empty_stats.exported_records, 0);

        let non_empty_stats = result_rx.recv().unwrap();
        assert_eq!(non_empty_stats.exported_records, 1);
    }

    #[test]
    fn test_export_worker_task_ordering() {
        let exporter = AdvancedTestExporter::new("ordering");
        let exported_records = exporter.exported_records.clone();
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建有序的任务
        let task_count = 10;
        for i in 1..=task_count {
            let record = create_test_record(
                i,
                &format!("SELECT order_{:02}", i),
                Some(&format!("order_{}.log", i))
            );
            let task = create_export_task(i as usize, vec![record], &format!("order_{}.log", i));
            task_tx.send(task).unwrap();
        }
        drop(task_tx);

        // 启动工作线程
        let handle = thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证记录按顺序处理
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), task_count as usize);

        for (i, record) in exported.iter().enumerate() {
            let expected_sql = format!("SELECT order_{:02}", i + 1);
            assert!(record.description.contains(&expected_sql),
                   "Record {} has wrong SQL: {}", i, record.description);
        }

        // 验证统计结果
        for _ in 0..task_count {
            let stats = result_rx.recv().unwrap();
            assert_eq!(stats.exported_records, 1);
        }
    }

    #[test]
    fn test_export_worker_resource_cleanup() {
        let exporter = AdvancedTestExporter::new("cleanup");
        let exported_records = exporter.exported_records.clone();
        let call_count = exporter.call_count.clone();
        let stats_handle = exporter.stats.clone();

        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 创建一些任务
        for i in 1..=3 {
            let record = create_test_record(i, &format!("SELECT cleanup_{}", i), Some("cleanup.log"));
            let task = create_export_task(i as usize, vec![record], "cleanup.log");
            task_tx.send(task).unwrap();
        }
        drop(task_tx);

        // 启动工作线程
        let handle = thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        assert!(result.is_ok());

        // 验证资源状态
        assert_eq!(exported_records.lock().unwrap().len(), 3);
        assert_eq!(*call_count.lock().unwrap(), 3);
        assert_eq!(stats_handle.lock().unwrap().exported_records, 3);

        // 验证通道已关闭，不再接收消息
        assert!(result_rx.try_recv().is_ok()); // 第一个统计结果
        assert!(result_rx.try_recv().is_ok()); // 第二个统计结果
        assert!(result_rx.try_recv().is_ok()); // 第三个统计结果
        // 之后应该没有更多结果
    }

    #[test]
    fn test_export_worker_stress_test() {
        let exporter = AdvancedTestExporter::new("stress");
        let exported_records = exporter.exported_records.clone();
        let (task_tx, task_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();

        // 压力测试：大量小任务
        let task_count = 1000;
        let start_time = Instant::now();

        for i in 1..=task_count {
            let record = create_test_record(
                i,
                &format!("SELECT stress_{}", i),
                Some(&format!("stress_{}.log", i % 10)) // 10个不同的文件
            );
            let task = create_export_task(i as usize, vec![record], &format!("stress_{}.log", i % 10));
            task_tx.send(task).unwrap();
        }
        drop(task_tx);

        let send_time = start_time.elapsed();

        // 启动工作线程
        let handle = thread::spawn(move || {
            export_worker(0, exporter, task_rx, result_tx)
        });

        // 等待完成
        let result = handle.join().unwrap();
        let total_time = start_time.elapsed();

        assert!(result.is_ok());

        // 验证所有记录都被导出
        let exported = exported_records.lock().unwrap();
        assert_eq!(exported.len(), task_count as usize);

        // 验证所有统计结果
        let mut total_stats = 0;
        for _ in 0..task_count {
            let stats = result_rx.recv().unwrap();
            total_stats += stats.exported_records;
        }
        assert_eq!(total_stats, task_count as usize);

        println!("压力测试: 发送{}个任务耗时{:?}, 总处理耗时{:?}",
                task_count, send_time, total_time);

        // 性能要求：处理1000个任务应在5秒内完成
        assert!(total_time < Duration::from_secs(5),
               "Stress test took too long: {:?}", total_time);
    }
}