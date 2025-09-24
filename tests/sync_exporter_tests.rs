//! 同步导出器实现模块测试

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
mod sync_exporter_tests {
    use sqllog_analysis::exporter::ExportStats;
    use sqllog_analysis::exporter::sync_impl::SyncExporter;
    use sqllog_analysis::prelude::*;
    use std::sync::{Arc, Mutex};

    // 测试用导出器实现
    struct MockSyncExporter {
        name: String,
        exported_records: Arc<Mutex<Vec<Sqllog>>>,
        export_calls: Arc<Mutex<usize>>,
        batch_calls: Arc<Mutex<usize>>,
        finalize_calls: Arc<Mutex<usize>>,
        should_fail_export: bool,
        should_fail_finalize: bool,
        stats: ExportStats,
    }

    impl MockSyncExporter {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                exported_records: Arc::new(Mutex::new(Vec::new())),
                export_calls: Arc::new(Mutex::new(0)),
                batch_calls: Arc::new(Mutex::new(0)),
                finalize_calls: Arc::new(Mutex::new(0)),
                should_fail_export: false,
                should_fail_finalize: false,
                stats: ExportStats::new(),
            }
        }

        fn with_export_failure(mut self) -> Self {
            self.should_fail_export = true;
            self
        }

        fn with_finalize_failure(mut self) -> Self {
            self.should_fail_finalize = true;
            self
        }

        fn with_custom_stats(mut self, stats: ExportStats) -> Self {
            self.stats = stats;
            self
        }

        #[allow(dead_code)]
        fn get_exported_records(&self) -> Vec<Sqllog> {
            self.exported_records.lock().unwrap().clone()
        }

        fn get_export_calls(&self) -> usize {
            *self.export_calls.lock().unwrap()
        }

        fn get_batch_calls(&self) -> usize {
            *self.batch_calls.lock().unwrap()
        }

        fn get_finalize_calls(&self) -> usize {
            *self.finalize_calls.lock().unwrap()
        }
    }

    impl SyncExporter for MockSyncExporter {
        fn name(&self) -> &str {
            &self.name
        }

        fn export_record(&mut self, record: &Sqllog) -> Result<()> {
            {
                let mut calls = self.export_calls.lock().unwrap();
                *calls += 1;
            }

            if self.should_fail_export {
                return Err(sqllog_analysis::error::SqllogError::parse_error(
                    "Mock export failure",
                )
                .into());
            }

            self.exported_records.lock().unwrap().push(record.clone());
            Ok(())
        }

        fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
            {
                let mut calls = self.batch_calls.lock().unwrap();
                *calls += 1;
            }

            if self.should_fail_export {
                return Err(sqllog_analysis::error::SqllogError::parse_error(
                    "Mock batch export failure",
                )
                .into());
            }

            for record in records {
                self.exported_records.lock().unwrap().push(record.clone());
            }
            Ok(())
        }

        fn finalize(&mut self) -> Result<()> {
            {
                let mut calls = self.finalize_calls.lock().unwrap();
                *calls += 1;
            }

            if self.should_fail_finalize {
                return Err(sqllog_analysis::error::SqllogError::parse_error(
                    "Mock finalize failure",
                )
                .into());
            }

            Ok(())
        }

        fn get_stats(&self) -> ExportStats {
            self.stats.clone()
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
    fn test_sync_exporter_trait_name() {
        let exporter = MockSyncExporter::new("TestExporter");
        assert_eq!(exporter.name(), "TestExporter");
    }

    #[test]
    fn test_sync_exporter_export_single_record() {
        let mut exporter = MockSyncExporter::new("SingleRecord");
        let record = create_test_record(1, "SELECT * FROM test");

        let result = exporter.export_record(&record);
        assert!(result.is_ok());

        let exported = exporter.get_exported_records();
        assert_eq!(exported.len(), 1);
        assert_eq!(exported[0].description, "SELECT * FROM test");
        assert_eq!(exporter.get_export_calls(), 1);
    }

    #[test]
    fn test_sync_exporter_export_multiple_records() {
        let mut exporter = MockSyncExporter::new("MultipleRecords");
        let records = vec![
            create_test_record(1, "SELECT 1"),
            create_test_record(2, "SELECT 2"),
            create_test_record(3, "SELECT 3"),
        ];

        for record in &records {
            let result = exporter.export_record(record);
            assert!(result.is_ok());
        }

        let exported = exporter.get_exported_records();
        assert_eq!(exported.len(), 3);
        assert_eq!(exporter.get_export_calls(), 3);

        for (i, record) in exported.iter().enumerate() {
            assert_eq!(record.description, format!("SELECT {}", i + 1));
        }
    }

    #[test]
    fn test_sync_exporter_export_batch() {
        let mut exporter = MockSyncExporter::new("BatchExport");
        let records = vec![
            create_test_record(1, "SELECT 1"),
            create_test_record(2, "SELECT 2"),
            create_test_record(3, "SELECT 3"),
        ];

        let result = exporter.export_batch(&records);
        assert!(result.is_ok());

        let exported = exporter.get_exported_records();
        assert_eq!(exported.len(), 3);
        assert_eq!(exporter.get_batch_calls(), 1);
        assert_eq!(exporter.get_export_calls(), 0); // export_record不应被调用

        for (i, record) in exported.iter().enumerate() {
            assert_eq!(record.description, format!("SELECT {}", i + 1));
        }
    }

    #[test]
    fn test_sync_exporter_export_empty_batch() {
        let mut exporter = MockSyncExporter::new("EmptyBatch");
        let records: Vec<Sqllog> = vec![];

        let result = exporter.export_batch(&records);
        assert!(result.is_ok());

        let exported = exporter.get_exported_records();
        assert_eq!(exported.len(), 0);
        assert_eq!(exporter.get_batch_calls(), 1);
    }

    #[test]
    fn test_sync_exporter_default_batch_implementation() {
        // 测试默认的批量导出实现是否正确调用单个记录导出
        struct DefaultBatchExporter {
            exported_count: usize,
        }

        impl SyncExporter for DefaultBatchExporter {
            fn name(&self) -> &str {
                "DefaultBatch"
            }

            fn export_record(&mut self, _record: &Sqllog) -> Result<()> {
                self.exported_count += 1;
                Ok(())
            }

            // 使用默认的 export_batch 实现
        }

        let mut exporter = DefaultBatchExporter { exported_count: 0 };
        let records = vec![
            create_test_record(1, "SELECT 1"),
            create_test_record(2, "SELECT 2"),
        ];

        let result = exporter.export_batch(&records);
        assert!(result.is_ok());
        assert_eq!(exporter.exported_count, 2);
    }

    #[test]
    fn test_sync_exporter_finalize() {
        let mut exporter = MockSyncExporter::new("FinalizeTest");

        let result = exporter.finalize();
        assert!(result.is_ok());
        assert_eq!(exporter.get_finalize_calls(), 1);
    }

    #[test]
    fn test_sync_exporter_multiple_finalize() {
        let mut exporter = MockSyncExporter::new("MultipleFinalizeTest");

        // 多次调用 finalize
        for _ in 0..3 {
            let result = exporter.finalize();
            assert!(result.is_ok());
        }

        assert_eq!(exporter.get_finalize_calls(), 3);
    }

    #[test]
    fn test_sync_exporter_export_record_failure() {
        let mut exporter =
            MockSyncExporter::new("ExportFailure").with_export_failure();
        let record = create_test_record(1, "SELECT * FROM test");

        let result = exporter.export_record(&record);
        assert!(result.is_err());

        // 失败时不应该有记录被添加
        let exported = exporter.get_exported_records();
        assert_eq!(exported.len(), 0);
        assert_eq!(exporter.get_export_calls(), 1);
    }

    #[test]
    fn test_sync_exporter_export_batch_failure() {
        let mut exporter =
            MockSyncExporter::new("BatchFailure").with_export_failure();
        let records = vec![
            create_test_record(1, "SELECT 1"),
            create_test_record(2, "SELECT 2"),
        ];

        let result = exporter.export_batch(&records);
        assert!(result.is_err());

        // 失败时不应该有记录被添加
        let exported = exporter.get_exported_records();
        assert_eq!(exported.len(), 0);
        assert_eq!(exporter.get_batch_calls(), 1);
    }

    #[test]
    fn test_sync_exporter_finalize_failure() {
        let mut exporter =
            MockSyncExporter::new("FinalizeFailure").with_finalize_failure();

        let result = exporter.finalize();
        assert!(result.is_err());
        assert_eq!(exporter.get_finalize_calls(), 1);
    }

    #[test]
    fn test_sync_exporter_get_stats() {
        let mut custom_stats = ExportStats::new();
        custom_stats.exported_records = 42;
        custom_stats.failed_records = 3;
        custom_stats.finish();

        let exporter =
            MockSyncExporter::new("StatsTest").with_custom_stats(custom_stats);

        let stats = exporter.get_stats();
        assert_eq!(stats.exported_records, 42);
        assert_eq!(stats.failed_records, 3);
    }

    #[test]
    fn test_sync_exporter_default_stats() {
        let exporter = MockSyncExporter::new("DefaultStats");
        let stats = exporter.get_stats();

        // 默认统计应该是空的
        assert_eq!(stats.exported_records, 0);
        assert_eq!(stats.failed_records, 0);
    }

    #[test]
    fn test_sync_exporter_partial_batch_failure() {
        // 测试默认批量实现在部分记录失败时的行为
        struct PartialFailureExporter {
            call_count: usize,
            fail_on_call: usize,
        }

        impl SyncExporter for PartialFailureExporter {
            fn name(&self) -> &str {
                "PartialFailure"
            }

            fn export_record(&mut self, _record: &Sqllog) -> Result<()> {
                self.call_count += 1;
                if self.call_count == self.fail_on_call {
                    return Err(
                        sqllog_analysis::error::SqllogError::parse_error(
                            "Partial failure",
                        )
                        .into(),
                    );
                }
                Ok(())
            }
        }

        let mut exporter = PartialFailureExporter {
            call_count: 0,
            fail_on_call: 2, // 第二次调用失败
        };

        let records = vec![
            create_test_record(1, "SELECT 1"),
            create_test_record(2, "SELECT 2"), // 这个会失败
            create_test_record(3, "SELECT 3"), // 这个不会被处理
        ];

        let result = exporter.export_batch(&records);
        assert!(result.is_err());
        assert_eq!(exporter.call_count, 2); // 只处理了前两个
    }

    #[test]
    fn test_sync_exporter_large_batch() {
        let mut exporter = MockSyncExporter::new("LargeBatch");

        // 创建大批量记录
        let mut large_batch = Vec::new();
        for i in 1..=1000 {
            large_batch.push(create_test_record(i, &format!("SELECT {}", i)));
        }

        let result = exporter.export_batch(&large_batch);
        assert!(result.is_ok());

        let exported = exporter.get_exported_records();
        assert_eq!(exported.len(), 1000);
        assert_eq!(exporter.get_batch_calls(), 1);

        // 验证所有记录都正确导出
        for (i, record) in exported.iter().enumerate() {
            assert_eq!(record.description, format!("SELECT {}", i + 1));
        }
    }

    #[test]
    fn test_sync_exporter_thread_safety_requirements() {
        // 验证 SyncExporter 的 Send + Sync 约束
        fn assert_send_sync<T: Send + Sync>() {}

        // 这应该能编译通过，证明我们的 MockSyncExporter 满足线程安全要求
        assert_send_sync::<MockSyncExporter>();

        let exporter = MockSyncExporter::new("ThreadSafety");

        // 验证能在线程间传递
        let handle = std::thread::spawn(move || {
            assert_eq!(exporter.name(), "ThreadSafety");
        });

        handle.join().unwrap();
    }

    #[test]
    fn test_sync_exporter_workflow() {
        // 测试完整的导出工作流程
        let mut exporter = MockSyncExporter::new("Workflow");

        // 1. 导出单个记录
        let single_record = create_test_record(1, "SELECT SINGLE");
        assert!(exporter.export_record(&single_record).is_ok());

        // 2. 导出批量记录
        let batch_records = vec![
            create_test_record(2, "SELECT BATCH 1"),
            create_test_record(3, "SELECT BATCH 2"),
        ];
        assert!(exporter.export_batch(&batch_records).is_ok());

        // 3. 完成导出
        assert!(exporter.finalize().is_ok());

        // 验证所有操作都被正确调用
        assert_eq!(exporter.get_export_calls(), 1);
        assert_eq!(exporter.get_batch_calls(), 1);
        assert_eq!(exporter.get_finalize_calls(), 1);

        // 验证所有记录都被导出
        let exported = exporter.get_exported_records();
        assert_eq!(exported.len(), 3);
        assert_eq!(exported[0].description, "SELECT SINGLE");
        assert_eq!(exported[1].description, "SELECT BATCH 1");
        assert_eq!(exported[2].description, "SELECT BATCH 2");
    }
} // 关闭 sync_exporter_tests 模块
