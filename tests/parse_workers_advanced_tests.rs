//! parse_workers.rs 的高级单元测试
//! 专注于测试未覆盖的代码路径和边界情况

#[cfg(test)]
mod advanced_parse_worker_tests {
    use sqllog_analysis::sqllog::concurrent::parse_workers::parse_files_concurrent;

    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    use sqllog_analysis::sqllog::concurrent::parse_workers::parse_and_export_concurrent;

    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    use sqllog_analysis::exporter::sync_impl::SyncExporter;

    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    use sqllog_analysis::exporter::ExportStats;

    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::TempDir;

    // 创建测试用的SQL日志文件
    fn create_test_log_file(
        dir: &TempDir,
        name: &str,
        content: &str,
    ) -> PathBuf {
        let file_path = dir.path().join(name);
        let mut file = fs::File::create(&file_path).unwrap();
        writeln!(file, "{}", content).unwrap();
        file_path
    }

    // 创建多行日志内容
    fn create_multi_line_log_content(
        line_count: usize,
        base_id: usize,
    ) -> String {
        let mut content = String::new();
        for i in 0..line_count {
            let id = base_id + i;
            content.push_str(&format!(
                "2025-09-16 20:02:53.{:03} (EP[{}] sess:0x6da8ccef{:x} thrd:414621{} user:EDM_BASE trxid:12215445302{} stmt:0x6da900ef{:x}) SELECT * FROM table_{} WHERE id={};\n",
                500 + id % 500,
                id % 10,
                id,
                id,
                id,
                id,
                id % 100,
                id
            ));
        }
        content
    }

    // 创建包含错误和有效行的混合内容
    fn create_mixed_error_content() -> String {
        r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT * FROM valid_table_1;
这是一个无效的日志行
2025-09-16 20:02:54.123 (EP[1] sess:0x6da8ccef1 thrd:4146218 user:EDM_BASE trxid:122154453027 stmt:0x6da900ef1) SELECT * FROM valid_table_2;
另一个无效行，没有正确的格式
2025-09-16 20:02:55.456 (EP[2] sess:0x6da8ccef2 thrd:4146219 user:EDM_BASE trxid:122154453028 stmt:0x6da900ef2) UPDATE valid_table_3 SET column1='value';
完全错误的格式：这不是SQL日志
2025-09-16 20:02:56.789 (EP[3] sess:0x6da8ccef3 thrd:4146220 user:EDM_BASE trxid:122154453029 stmt:0x6da900ef3) DELETE FROM valid_table_4 WHERE id=123;
"#.to_string()
    }

    #[test]
    fn test_parse_files_concurrent_empty_file_list() {
        let result = parse_files_concurrent(&[], 10, 2, None);
        assert!(result.is_ok());
        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 0);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_parse_files_concurrent_thread_count_zero() {
        let temp_dir = TempDir::new().unwrap();
        let log_content1 = create_multi_line_log_content(3, 0);
        let log_content2 = create_multi_line_log_content(2, 100);

        let file1 = create_test_log_file(&temp_dir, "file1.log", &log_content1);
        let file2 = create_test_log_file(&temp_dir, "file2.log", &log_content2);

        // thread_count=0 表示每文件一个线程
        let result = parse_files_concurrent(&[file1, file2], 5, 0, None);
        assert!(result.is_ok());
        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 5); // 3 + 2
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_parse_files_concurrent_thread_count_exceeds_files() {
        let temp_dir = TempDir::new().unwrap();
        let log_content = create_multi_line_log_content(5, 0);
        let file = create_test_log_file(&temp_dir, "single.log", &log_content);

        // 10个线程但只有1个文件
        let result = parse_files_concurrent(&[file], 3, 10, None);
        assert!(result.is_ok());
        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_parse_files_concurrent_with_nonexistent_files() {
        let temp_dir = TempDir::new().unwrap();
        let log_content = create_multi_line_log_content(2, 0);
        let valid_file =
            create_test_log_file(&temp_dir, "valid.log", &log_content);
        let nonexistent_file = temp_dir.path().join("nonexistent.log");

        let result =
            parse_files_concurrent(&[valid_file, nonexistent_file], 5, 2, None);
        assert!(result.is_ok());
        let (records, _errors) = result.unwrap();
        // 应该只解析到有效文件的记录
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_parse_files_concurrent_with_mixed_content() {
        let temp_dir = TempDir::new().unwrap();
        let mixed_content = create_mixed_error_content();
        let mixed_file =
            create_test_log_file(&temp_dir, "mixed.log", &mixed_content);

        let result = parse_files_concurrent(&[mixed_file], 2, 1, None);
        assert!(result.is_ok());
        let (records, _errors) = result.unwrap();
        // 应该解析出有效的记录
        assert!(records.len() >= 3); // 至少应该有有效的记录
    }

    #[test]
    fn test_parse_files_concurrent_large_batch_size() {
        let temp_dir = TempDir::new().unwrap();
        let log_content = create_multi_line_log_content(10, 0);
        let file =
            create_test_log_file(&temp_dir, "large_batch.log", &log_content);

        // 批次大小大于记录数
        let result = parse_files_concurrent(&[file], 100, 1, None);
        assert!(result.is_ok());
        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 10);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_parse_files_concurrent_small_batch_size() {
        let temp_dir = TempDir::new().unwrap();
        let log_content = create_multi_line_log_content(7, 0);
        let file =
            create_test_log_file(&temp_dir, "small_batch.log", &log_content);

        // 非常小的批次大小
        let result = parse_files_concurrent(&[file], 1, 1, None);
        assert!(result.is_ok());
        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 7);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_parse_files_concurrent_many_files_few_threads() {
        let temp_dir = TempDir::new().unwrap();
        let mut files = Vec::new();
        let mut expected_total = 0;

        // 创建多个小文件
        for i in 0..8 {
            let record_count = i + 1; // 1, 2, 3, ..., 8
            let log_content =
                create_multi_line_log_content(record_count, i * 100);
            let file = create_test_log_file(
                &temp_dir,
                &format!("file_{}.log", i),
                &log_content,
            );
            files.push(file);
            expected_total += record_count;
        }

        // 使用较少的线程处理多个文件
        let result = parse_files_concurrent(&files, 3, 2, None);
        assert!(result.is_ok());
        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), expected_total);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_parse_files_concurrent_concurrent_access_safety() {
        let temp_dir = TempDir::new().unwrap();
        let mut files = Vec::new();

        // 创建多个文件，每个文件包含唯一的记录
        for i in 0..5 {
            let log_content = create_multi_line_log_content(10, i * 1000);
            let file = create_test_log_file(
                &temp_dir,
                &format!("concurrent_{}.log", i),
                &log_content,
            );
            files.push(file);
        }

        let result = parse_files_concurrent(&files, 5, 5, None);
        assert!(result.is_ok());
        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 50); // 5 files × 10 records
        assert_eq!(errors.len(), 0);

        // 验证没有重复的记录（通过SQL语句检查）
        let mut sql_statements = std::collections::HashSet::new();
        for record in &records {
            assert!(
                sql_statements.insert(record.description.clone()),
                "Found duplicate SQL: {}",
                record.description
            );
        }
        assert_eq!(sql_statements.len(), 50);
    }

    #[test]
    fn test_parse_files_concurrent_file_queue_management() {
        let temp_dir = TempDir::new().unwrap();
        let mut files = Vec::new();

        // 创建不同大小的文件来测试队列管理
        let file_sizes = vec![1, 5, 2, 8, 3, 10, 4, 6];
        let mut expected_total = 0;

        for (i, size) in file_sizes.iter().enumerate() {
            let log_content = create_multi_line_log_content(*size, i * 100);
            let file = create_test_log_file(
                &temp_dir,
                &format!("queue_{}.log", i),
                &log_content,
            );
            files.push(file);
            expected_total += size;
        }

        // 使用适中数量的线程
        let result = parse_files_concurrent(&files, 4, 3, None);
        assert!(result.is_ok());
        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), expected_total);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_parse_files_concurrent_error_handling() {
        let temp_dir = TempDir::new().unwrap();

        // 创建一个完全无效的文件
        let invalid_content =
            "这完全不是SQL日志格式\n另一行无效内容\n还是无效的\n";
        let invalid_file =
            create_test_log_file(&temp_dir, "invalid.log", invalid_content);

        // 创建一个有效的文件
        let valid_content = create_multi_line_log_content(3, 0);
        let valid_file =
            create_test_log_file(&temp_dir, "valid.log", &valid_content);

        let result =
            parse_files_concurrent(&[invalid_file, valid_file], 2, 2, None);
        assert!(result.is_ok());
        let (records, _errors) = result.unwrap();
        // 应该至少解析出有效文件的记录
        assert_eq!(records.len(), 3);
    }

    // 测试导出功能（如果启用了导出feature）
    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    mod export_tests {
        use super::*;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        // 测试用的导出器
        struct TestExporter {
            records: Arc<Mutex<Vec<sqllog_analysis::sqllog::types::Sqllog>>>,
            should_fail: bool,
            delay: Option<Duration>,
        }

        impl TestExporter {
            fn new() -> Self {
                Self {
                    records: Arc::new(Mutex::new(Vec::new())),
                    should_fail: false,
                    delay: None,
                }
            }

            fn with_failure(mut self) -> Self {
                self.should_fail = true;
                self
            }

            fn with_delay(mut self, delay: Duration) -> Self {
                self.delay = Some(delay);
                self
            }

            #[allow(dead_code)]
            fn get_exported_records(
                &self,
            ) -> Vec<sqllog_analysis::sqllog::types::Sqllog> {
                self.records.lock().unwrap().clone()
            }
        }

        impl SyncExporter for TestExporter {
            fn name(&self) -> &str {
                "test_exporter"
            }

            fn export_record(
                &mut self,
                record: &sqllog_analysis::sqllog::types::Sqllog,
            ) -> sqllog_analysis::error::Result<()> {
                if self.should_fail {
                    return Err(
                        sqllog_analysis::error::SqllogError::parse_error(
                            "Test export failure",
                        )
                        .into(),
                    );
                }

                if let Some(delay) = self.delay {
                    std::thread::sleep(delay);
                }

                self.records.lock().unwrap().push(record.clone());
                Ok(())
            }

            fn export_batch(
                &mut self,
                records: &[sqllog_analysis::sqllog::types::Sqllog],
            ) -> sqllog_analysis::error::Result<()> {
                if self.should_fail {
                    return Err(
                        sqllog_analysis::error::SqllogError::parse_error(
                            "Test batch export failure",
                        )
                        .into(),
                    );
                }

                if let Some(delay) = self.delay {
                    std::thread::sleep(delay);
                }

                for record in records {
                    self.records.lock().unwrap().push(record.clone());
                }
                Ok(())
            }

            fn get_stats(&self) -> ExportStats {
                let mut stats = ExportStats::new();
                stats.exported_records = self.records.lock().unwrap().len();
                stats.finish();
                stats
            }
        }

        #[test]
        fn test_parse_and_export_concurrent_empty_files() {
            let exporter = TestExporter::new();
            let exported_records = exporter.records.clone();

            let result = parse_and_export_concurrent(&[], exporter, 10, 2);
            assert!(result.is_ok());
            let results = result.unwrap();
            assert_eq!(results.len(), 0);
            assert_eq!(exported_records.lock().unwrap().len(), 0);
        }

        #[test]
        fn test_parse_and_export_concurrent_single_file() {
            let temp_dir = TempDir::new().unwrap();
            let log_content = create_multi_line_log_content(5, 0);
            let file =
                create_test_log_file(&temp_dir, "single.log", &log_content);

            let exporter = TestExporter::new();
            let exported_records = exporter.records.clone();

            let result = parse_and_export_concurrent(&[file], exporter, 3, 1);
            assert!(result.is_ok());
            let results = result.unwrap();
            assert_eq!(results.len(), 1); // 1个文件
            assert_eq!(results[0].0, 5); // 5条记录
            assert_eq!(results[0].1, 0); // 0个错误
            assert_eq!(exported_records.lock().unwrap().len(), 5); // 导出5条记录
        }

        #[test]
        fn test_parse_and_export_concurrent_multiple_files() {
            let temp_dir = TempDir::new().unwrap();
            let mut files = Vec::new();
            let mut expected_total_records = 0;

            // 创建多个文件
            for i in 0..3 {
                let record_count = (i + 1) * 2; // 2, 4, 6
                let log_content =
                    create_multi_line_log_content(record_count, i * 100);
                let file = create_test_log_file(
                    &temp_dir,
                    &format!("multi_{}.log", i),
                    &log_content,
                );
                files.push(file);
                expected_total_records += record_count;
            }

            let exporter = TestExporter::new();
            let exported_records = exporter.records.clone();

            let result = parse_and_export_concurrent(&files, exporter, 3, 2);
            assert!(result.is_ok());
            let results = result.unwrap();
            assert_eq!(results.len(), 3); // 3个文件

            let total_parsed: usize = results.iter().map(|(r, _)| r).sum();
            assert_eq!(total_parsed, expected_total_records);
            assert_eq!(
                exported_records.lock().unwrap().len(),
                expected_total_records
            );
        }

        #[test]
        fn test_parse_and_export_concurrent_thread_count_zero() {
            let temp_dir = TempDir::new().unwrap();
            let mut files = Vec::new();

            // 创建3个文件
            for i in 0..3 {
                let log_content = create_multi_line_log_content(3, i * 100);
                let file = create_test_log_file(
                    &temp_dir,
                    &format!("auto_{}.log", i),
                    &log_content,
                );
                files.push(file);
            }

            let exporter = TestExporter::new();
            let exported_records = exporter.records.clone();

            // thread_count=0 表示每文件一线程
            let result = parse_and_export_concurrent(&files, exporter, 2, 0);
            assert!(result.is_ok());
            let results = result.unwrap();
            assert_eq!(results.len(), 3);

            let total_parsed: usize = results.iter().map(|(r, _)| r).sum();
            assert_eq!(total_parsed, 9); // 3 * 3
            assert_eq!(exported_records.lock().unwrap().len(), 9);
        }

        #[test]
        fn test_parse_and_export_concurrent_export_failure() {
            let temp_dir = TempDir::new().unwrap();
            let log_content = create_multi_line_log_content(3, 0);
            let file =
                create_test_log_file(&temp_dir, "fail.log", &log_content);

            let exporter = TestExporter::new().with_failure();

            let result = parse_and_export_concurrent(&[file], exporter, 2, 1);
            // 导出失败应该返回错误
            assert!(result.is_err());
        }

        #[test]
        fn test_parse_and_export_concurrent_large_batch() {
            let temp_dir = TempDir::new().unwrap();
            let log_content = create_multi_line_log_content(20, 0);
            let file =
                create_test_log_file(&temp_dir, "large.log", &log_content);

            let exporter = TestExporter::new();
            let exported_records = exporter.records.clone();

            let result = parse_and_export_concurrent(&[file], exporter, 50, 1);
            assert!(result.is_ok());
            let results = result.unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].0, 20); // 20条记录
            assert_eq!(exported_records.lock().unwrap().len(), 20);
        }

        #[test]
        fn test_parse_and_export_concurrent_small_batch() {
            let temp_dir = TempDir::new().unwrap();
            let log_content = create_multi_line_log_content(7, 0);
            let file =
                create_test_log_file(&temp_dir, "small.log", &log_content);

            let exporter = TestExporter::new();
            let exported_records = exporter.records.clone();

            let result = parse_and_export_concurrent(&[file], exporter, 1, 1);
            assert!(result.is_ok());
            let results = result.unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].0, 7);
            assert_eq!(exported_records.lock().unwrap().len(), 7);
        }

        #[test]
        fn test_parse_and_export_concurrent_thread_safety() {
            let temp_dir = TempDir::new().unwrap();
            let mut files = Vec::new();
            let mut expected_total = 0;

            // 创建多个文件进行并发处理
            for i in 0..4 {
                let record_count = 10;
                let log_content =
                    create_multi_line_log_content(record_count, i * 1000);
                let file = create_test_log_file(
                    &temp_dir,
                    &format!("thread_safe_{}.log", i),
                    &log_content,
                );
                files.push(file);
                expected_total += record_count;
            }

            let exporter = TestExporter::new();
            let exported_records = exporter.records.clone();

            let result = parse_and_export_concurrent(&files, exporter, 5, 4);
            assert!(result.is_ok());
            let results = result.unwrap();
            assert_eq!(results.len(), 4);

            let total_parsed: usize = results.iter().map(|(r, _)| r).sum();
            assert_eq!(total_parsed, expected_total);
            assert_eq!(exported_records.lock().unwrap().len(), expected_total);

            // 验证所有记录都是唯一的
            let exported = exported_records.lock().unwrap();
            let mut sql_statements = std::collections::HashSet::new();
            for record in exported.iter() {
                assert!(
                    sql_statements.insert(record.description.clone()),
                    "Found duplicate SQL: {}",
                    record.description
                );
            }
        }

        #[test]
        fn test_parse_and_export_concurrent_with_delays() {
            let temp_dir = TempDir::new().unwrap();
            let log_content = create_multi_line_log_content(3, 0);
            let file =
                create_test_log_file(&temp_dir, "delay.log", &log_content);

            let start_time = std::time::Instant::now();
            let exporter =
                TestExporter::new().with_delay(Duration::from_millis(10));
            let exported_records = exporter.records.clone();

            let result = parse_and_export_concurrent(&[file], exporter, 1, 1);
            let elapsed = start_time.elapsed();

            assert!(result.is_ok());
            let results = result.unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].0, 3);
            assert_eq!(exported_records.lock().unwrap().len(), 3);

            // 应该包含延迟时间
            assert!(elapsed >= Duration::from_millis(30)); // 3 batches × 10ms
        }

        #[test]
        fn test_parse_and_export_concurrent_mixed_file_content() {
            let temp_dir = TempDir::new().unwrap();
            let mixed_content = create_mixed_error_content();
            let mixed_file =
                create_test_log_file(&temp_dir, "mixed.log", &mixed_content);

            let exporter = TestExporter::new();
            let exported_records = exporter.records.clone();

            let result =
                parse_and_export_concurrent(&[mixed_file], exporter, 2, 1);
            assert!(result.is_ok());
            let results = result.unwrap();
            assert_eq!(results.len(), 1);

            // 应该有有效记录被导出
            let exported = exported_records.lock().unwrap();
            assert!(exported.len() >= 3); // 至少应该有有效的记录

            // 验证导出的记录都是有效的
            for record in exported.iter() {
                assert!(!record.description.is_empty());
                assert!(
                    record.description.contains("SELECT")
                        || record.description.contains("UPDATE")
                        || record.description.contains("DELETE")
                );
            }
        }
    }
}
