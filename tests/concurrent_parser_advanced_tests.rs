//! concurrent_parser.rs 的高级单元测试
//! 专注于测试未覆盖的代码路径和边界情况

#[cfg(test)]
mod advanced_concurrent_parser_tests {
    use sqllog_analysis::config::SqllogConfig;
    use sqllog_analysis::sqllog::concurrent::ConcurrentParser;

    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    use sqllog_analysis::exporter::{ExportStats, SyncExporter};

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

    // 创建复杂的日志内容（包含不同类型的SQL语句）
    fn create_complex_log_content(
        record_count: usize,
        base_id: usize,
    ) -> String {
        let mut content = String::new();
        let sql_types = ["SELECT", "INSERT", "UPDATE", "DELETE"];
        let tables = ["users", "orders", "products", "logs"];

        for i in 0..record_count {
            let id = base_id + i;
            let sql_type = sql_types[i % sql_types.len()];
            let table = tables[i % tables.len()];

            let sql_statement = match sql_type {
                "SELECT" => {
                    format!("SELECT * FROM {} WHERE id = {}", table, id)
                }
                "INSERT" => format!(
                    "INSERT INTO {} (id, name) VALUES ({}, 'test_{}')",
                    table, id, id
                ),
                "UPDATE" => format!(
                    "UPDATE {} SET name = 'updated_{}' WHERE id = {}",
                    table, id, id
                ),
                "DELETE" => format!("DELETE FROM {} WHERE id = {}", table, id),
                _ => format!("SELECT count(*) FROM {}", table),
            };

            content.push_str(&format!(
                "2025-09-16 20:02:{:02}.{:03} (EP[{}] sess:0x6da8ccef{:x} thrd:414621{} user:EDM_USER_{} trxid:12215445302{} stmt:0x6da900ef{:x}) {};\n",
                (50 + id) % 60,      // 分钟
                500 + id % 500,      // 毫秒
                id % 10,             // EP
                id,                  // 会话ID
                id,                  // 线程ID
                id % 5,              // 用户ID
                id,                  // 事务ID
                id,                  // 语句ID
                sql_statement        // SQL语句
            ));
        }
        content
    }

    // 创建包含语法错误的混合内容
    fn create_error_mixed_content() -> String {
        r#"2025-09-16 20:02:53.100 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT * FROM valid_table_1;
这不是有效的日志行 - 缺少时间戳和结构
2025-09-16 20:02:53.200 (EP[1] sess:0x6da8ccef1 thrd:4146218 user:EDM_BASE trxid:122154453027 stmt:0x6da900ef1) INSERT INTO valid_table_2 VALUES(1, 'test');
另一个错误行：时间戳格式不正确 25-09-16 20:02:53.300
2025-09-16 20:02:53.400 (EP[2] sess:0x6da8ccef2 thrd:4146219 user:EDM_BASE trxid:122154453028 stmt:0x6da900ef2) UPDATE valid_table_3 SET col1 = 'value';
错误：缺少EP信息 2025-09-16 20:02:53.500 sess:0x6da8ccef3 thrd:4146220 user:EDM_BASE trxid:122154453029 stmt:0x6da900ef3) DELETE FROM table;
2025-09-16 20:02:53.600 (EP[3] sess:0x6da8ccef4 thrd:4146221 user:EDM_BASE trxid:122154453030 stmt:0x6da900ef4) SELECT COUNT(*) FROM valid_table_4;
完全错误的格式 - 没有任何结构
"#.to_string()
    }

    #[test]
    fn test_concurrent_parser_new_with_none_thread_count() {
        let config = SqllogConfig {
            thread_count: None, // None 应该转换为 0
            batch_size: 100,
            queue_buffer_size: 1000,
        };

        let parser = ConcurrentParser::new(config);
        assert_eq!(parser.thread_count, 0); // None -> 0
        assert_eq!(parser.batch_size, 100);
    }

    #[test]
    fn test_concurrent_parser_new_with_some_thread_count() {
        let config = SqllogConfig {
            thread_count: Some(8),
            batch_size: 250,
            queue_buffer_size: 2000,
        };

        let parser = ConcurrentParser::new(config);
        assert_eq!(parser.thread_count, 8);
        assert_eq!(parser.batch_size, 250);
    }

    #[test]
    fn test_concurrent_parser_new_extreme_config_values() {
        // 测试极端配置值
        let config = SqllogConfig {
            thread_count: Some(0), // 最小值
            batch_size: 1,         // 最小批次
            queue_buffer_size: 1,  // 最小队列
        };

        let parser = ConcurrentParser::new(config);
        assert_eq!(parser.thread_count, 0);
        assert_eq!(parser.batch_size, 1);

        let config2 = SqllogConfig {
            thread_count: Some(1000),  // 很大的值
            batch_size: 10000,         // 很大的批次
            queue_buffer_size: 100000, // 很大的队列
        };

        let parser2 = ConcurrentParser::new(config2);
        assert_eq!(parser2.thread_count, 1000);
        assert_eq!(parser2.batch_size, 10000);
    }

    #[test]
    fn test_concurrent_parser_default_values() {
        let parser = ConcurrentParser::default();
        let default_config = SqllogConfig::default();

        assert_eq!(
            parser.thread_count,
            default_config.thread_count.unwrap_or(0)
        );
        assert_eq!(parser.batch_size, default_config.batch_size);
    }

    #[test]
    fn test_concurrent_parser_clone() {
        let config = SqllogConfig {
            thread_count: Some(4),
            batch_size: 150,
            queue_buffer_size: 3000,
        };

        let parser1 = ConcurrentParser::new(config);
        let parser2 = parser1.clone();

        assert_eq!(parser1.thread_count, parser2.thread_count);
        assert_eq!(parser1.batch_size, parser2.batch_size);
    }

    #[test]
    fn test_concurrent_parser_debug_format() {
        let parser = ConcurrentParser::new(SqllogConfig {
            thread_count: Some(2),
            batch_size: 50,
            queue_buffer_size: 1000,
        });

        let debug_str = format!("{:?}", parser);
        assert!(debug_str.contains("ConcurrentParser"));
        assert!(debug_str.contains("thread_count"));
        assert!(debug_str.contains("batch_size"));
    }

    #[test]
    fn test_parse_files_concurrent_complex_content() {
        let temp_dir = TempDir::new().unwrap();

        // 创建包含复杂SQL语句的文件
        let complex_content = create_complex_log_content(20, 0);
        let complex_file =
            create_test_log_file(&temp_dir, "complex.log", &complex_content);

        let parser = ConcurrentParser::new(SqllogConfig {
            thread_count: Some(2),
            batch_size: 5,
            queue_buffer_size: 1000,
        });

        let result = parser.parse_files_concurrent(&[complex_file]);
        assert!(result.is_ok());

        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 20);
        assert_eq!(errors.len(), 0);

        // 验证SQL语句包含不同的操作类型
        let descriptions: Vec<String> =
            records.iter().map(|r| r.description.clone()).collect();

        // 检查是否包含不同类型的SQL操作
        let has_select = descriptions.iter().any(|d| d.contains("SELECT"));
        let has_insert = descriptions.iter().any(|d| d.contains("INSERT"));
        let has_update = descriptions.iter().any(|d| d.contains("UPDATE"));
        let has_delete = descriptions.iter().any(|d| d.contains("DELETE"));

        // 至少应该有2种不同类型的操作
        let type_count = [has_select, has_insert, has_update, has_delete]
            .iter()
            .filter(|&&x| x)
            .count();
        assert!(type_count >= 2, "应该解析出至少2种不同类型的SQL操作");
    }

    #[test]
    fn test_parse_files_concurrent_mixed_valid_invalid() {
        let temp_dir = TempDir::new().unwrap();

        let mixed_content = create_error_mixed_content();
        let mixed_file =
            create_test_log_file(&temp_dir, "mixed.log", &mixed_content);

        let parser = ConcurrentParser::new(SqllogConfig {
            thread_count: Some(1),
            batch_size: 3,
            queue_buffer_size: 1000,
        });

        let result = parser.parse_files_concurrent(&[mixed_file]);
        assert!(result.is_ok());

        let (records, _errors) = result.unwrap();
        // 应该解析出有效的记录（大约3-4条有效记录）
        assert!(records.len() >= 3);
        assert!(records.len() <= 4);

        // 验证解析出的记录都包含有效的SQL语句
        for record in &records {
            assert!(!record.description.is_empty());
            assert!(
                record.description.contains("SELECT")
                    || record.description.contains("INSERT")
                    || record.description.contains("UPDATE")
            );
        }
    }

    #[test]
    fn test_parse_files_concurrent_zero_batch_size() {
        let temp_dir = TempDir::new().unwrap();
        let log_content = create_complex_log_content(10, 0);
        let log_file =
            create_test_log_file(&temp_dir, "zero_batch.log", &log_content);

        let parser = ConcurrentParser::new(SqllogConfig {
            thread_count: Some(1),
            batch_size: 0, // 零批次大小
            queue_buffer_size: 1000,
        });

        let result = parser.parse_files_concurrent(&[log_file]);
        assert!(result.is_ok());

        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 10);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_parse_files_concurrent_file_path_variations() {
        let temp_dir = TempDir::new().unwrap();

        // 创建不同路径深度的文件
        let subdir = temp_dir.path().join("subdir").join("deep");
        fs::create_dir_all(&subdir).unwrap();

        let content1 = create_complex_log_content(5, 0);
        let content2 = create_complex_log_content(3, 100);

        let file1 = create_test_log_file(&temp_dir, "root.log", &content1);
        let file2_path = subdir.join("deep.log");
        let mut file2 = fs::File::create(&file2_path).unwrap();
        writeln!(file2, "{}", content2).unwrap();

        let parser = ConcurrentParser::new(SqllogConfig {
            thread_count: Some(2),
            batch_size: 4,
            queue_buffer_size: 1000,
        });

        let result = parser.parse_files_concurrent(&[file1, file2_path]);
        assert!(result.is_ok());

        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 8); // 5 + 3
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_parse_files_concurrent_unicode_content() {
        let temp_dir = TempDir::new().unwrap();

        // 创建包含Unicode字符的日志
        let unicode_content = format!(
            "2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:用户_测试 trxid:122154453026 stmt:0x6da900ef0) SELECT * FROM 表名_测试 WHERE 字段 = '中文值';\n\
             2025-09-16 20:02:53.563 (EP[1] sess:0x6da8ccef1 thrd:4146218 user:EDM_BASE trxid:122154453027 stmt:0x6da900ef1) INSERT INTO products (name) VALUES ('产品名称_🚀');\n\
             2025-09-16 20:02:53.564 (EP[2] sess:0x6da8ccef2 thrd:4146219 user:EDM_BASE trxid:122154453028 stmt:0x6da900ef2) UPDATE orders SET status = 'completed_完成' WHERE id = 123;\n"
        );

        let unicode_file =
            create_test_log_file(&temp_dir, "unicode.log", &unicode_content);

        let parser = ConcurrentParser::new(SqllogConfig {
            thread_count: Some(1),
            batch_size: 2,
            queue_buffer_size: 1000,
        });

        let result = parser.parse_files_concurrent(&[unicode_file]);
        assert!(result.is_ok());

        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(errors.len(), 0);

        // 验证Unicode内容被正确保存
        assert!(records.iter().any(|r| r.description.contains("中文值")));
        assert!(records.iter().any(|r| r.description.contains("产品名称_🚀")));
        assert!(
            records.iter().any(|r| r.description.contains("completed_完成"))
        );
    }

    #[test]
    fn test_parse_files_concurrent_very_large_files() {
        let temp_dir = TempDir::new().unwrap();

        // 创建大文件
        let large_content = create_complex_log_content(500, 0);
        let large_file =
            create_test_log_file(&temp_dir, "large.log", &large_content);

        let parser = ConcurrentParser::new(SqllogConfig {
            thread_count: Some(2),
            batch_size: 100,
            queue_buffer_size: 5000,
        });

        let start_time = std::time::Instant::now();
        let result = parser.parse_files_concurrent(&[large_file]);
        let elapsed = start_time.elapsed();

        assert!(result.is_ok());

        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 500);
        assert_eq!(errors.len(), 0);

        println!("大文件解析: 500条记录, 耗时: {:?}", elapsed);
        assert!(elapsed < std::time::Duration::from_secs(5));
    }

    #[test]
    fn test_parse_files_concurrent_many_small_files() {
        let temp_dir = TempDir::new().unwrap();
        let mut files = Vec::new();
        let mut expected_total = 0;

        // 创建很多小文件
        for i in 0..20 {
            let record_count = (i % 5) + 1; // 1-5条记录
            let content = create_complex_log_content(record_count, i * 100);
            let file = create_test_log_file(
                &temp_dir,
                &format!("small_{}.log", i),
                &content,
            );
            files.push(file);
            expected_total += record_count;
        }

        let parser = ConcurrentParser::new(SqllogConfig {
            thread_count: Some(8),
            batch_size: 3,
            queue_buffer_size: 1000,
        });

        let result = parser.parse_files_concurrent(&files);
        assert!(result.is_ok());

        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), expected_total);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_parse_files_concurrent_thread_count_optimization() {
        let temp_dir = TempDir::new().unwrap();
        let content = create_complex_log_content(50, 0);
        let file = create_test_log_file(&temp_dir, "optimize.log", &content);

        // 测试不同的线程数配置
        let thread_counts = vec![0, 1, 2, 4, 8];

        for thread_count in thread_counts {
            let parser = ConcurrentParser::new(SqllogConfig {
                thread_count: Some(thread_count),
                batch_size: 10,
                queue_buffer_size: 1000,
            });

            let start_time = std::time::Instant::now();
            let result = parser.parse_files_concurrent(&[file.clone()]);
            let elapsed = start_time.elapsed();

            assert!(result.is_ok(), "Thread count {} failed", thread_count);

            let (records, errors) = result.unwrap();
            assert_eq!(
                records.len(),
                50,
                "Thread count {} wrong record count",
                thread_count
            );
            assert_eq!(
                errors.len(),
                0,
                "Thread count {} has errors",
                thread_count
            );

            println!("线程数 {}: 耗时 {:?}", thread_count, elapsed);
        }
    }

    // 测试导出功能（如果启用了导出feature）
    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    mod streaming_export_tests {
        use super::*;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        // 高级测试导出器
        struct AdvancedStreamingExporter {
            name: String,
            exported_records:
                Arc<Mutex<Vec<sqllog_analysis::sqllog::types::Sqllog>>>,
            batch_count: Arc<Mutex<usize>>,
            finalize_called: Arc<Mutex<bool>>,
            delay_per_batch: Option<Duration>,
            fail_on_batch: Option<usize>,
        }

        impl AdvancedStreamingExporter {
            fn new(name: &str) -> Self {
                Self {
                    name: name.to_string(),
                    exported_records: Arc::new(Mutex::new(Vec::new())),
                    batch_count: Arc::new(Mutex::new(0)),
                    finalize_called: Arc::new(Mutex::new(false)),
                    delay_per_batch: None,
                    fail_on_batch: None,
                }
            }

            fn with_delay(mut self, delay: Duration) -> Self {
                self.delay_per_batch = Some(delay);
                self
            }

            fn with_failure_on_batch(mut self, batch_number: usize) -> Self {
                self.fail_on_batch = Some(batch_number);
                self
            }

            #[allow(dead_code)]
            fn get_exported_records(
                &self,
            ) -> Vec<sqllog_analysis::sqllog::types::Sqllog> {
                self.exported_records.lock().unwrap().clone()
            }

            #[allow(dead_code)]
            fn get_batch_count(&self) -> usize {
                *self.batch_count.lock().unwrap()
            }

            #[allow(dead_code)]
            fn is_finalize_called(&self) -> bool {
                *self.finalize_called.lock().unwrap()
            }
        }

        impl SyncExporter for AdvancedStreamingExporter {
            fn name(&self) -> &str {
                &self.name
            }

            fn export_record(
                &mut self,
                record: &sqllog_analysis::sqllog::types::Sqllog,
            ) -> sqllog_analysis::error::Result<()> {
                self.exported_records.lock().unwrap().push(record.clone());
                Ok(())
            }

            fn export_batch(
                &mut self,
                records: &[sqllog_analysis::sqllog::types::Sqllog],
            ) -> sqllog_analysis::error::Result<()> {
                let mut batch_count = self.batch_count.lock().unwrap();
                *batch_count += 1;
                let current_batch = *batch_count;
                drop(batch_count);

                // 检查失败条件
                if let Some(fail_batch) = self.fail_on_batch {
                    if current_batch == fail_batch {
                        return Err(
                            sqllog_analysis::error::SqllogError::parse_error(
                                &format!(
                                    "Export failed at batch {}",
                                    current_batch
                                ),
                            )
                            .into(),
                        );
                    }
                }

                // 添加延迟
                if let Some(delay) = self.delay_per_batch {
                    std::thread::sleep(delay);
                }

                for record in records {
                    self.exported_records.lock().unwrap().push(record.clone());
                }

                Ok(())
            }

            fn finalize(&mut self) -> sqllog_analysis::error::Result<()> {
                *self.finalize_called.lock().unwrap() = true;
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

        #[test]
        fn test_parse_and_export_streaming_basic() {
            let temp_dir = TempDir::new().unwrap();
            let content = create_complex_log_content(10, 0);
            let file =
                create_test_log_file(&temp_dir, "streaming.log", &content);

            let exporter = AdvancedStreamingExporter::new("streaming_basic");
            let exported_records = exporter.exported_records.clone();
            let finalize_called = exporter.finalize_called.clone();

            let parser = ConcurrentParser::new(SqllogConfig {
                thread_count: Some(1),
                batch_size: 3,
                queue_buffer_size: 1000,
            });

            let result = parser.parse_and_export_streaming(&[file], exporter);
            assert!(result.is_ok());

            let results = result.unwrap();
            assert_eq!(results.len(), 1); // 1个文件
            assert_eq!(results[0].0, 10); // 10条记录
            assert_eq!(results[0].1, 0); // 0个错误

            // 验证导出
            let exported = exported_records.lock().unwrap();
            assert_eq!(exported.len(), 10);

            // 验证finalize被调用
            assert!(*finalize_called.lock().unwrap());
        }

        #[test]
        fn test_parse_and_export_streaming_multiple_files() {
            let temp_dir = TempDir::new().unwrap();
            let mut files = Vec::new();
            let mut expected_total = 0;

            // 创建多个文件
            for i in 0..3 {
                let record_count = (i + 1) * 3; // 3, 6, 9
                let content =
                    create_complex_log_content(record_count, i * 1000);
                let file = create_test_log_file(
                    &temp_dir,
                    &format!("multi_{}.log", i),
                    &content,
                );
                files.push(file);
                expected_total += record_count;
            }

            let exporter = AdvancedStreamingExporter::new("streaming_multi");
            let exported_records = exporter.exported_records.clone();
            let batch_count = exporter.batch_count.clone();

            let parser = ConcurrentParser::new(SqllogConfig {
                thread_count: Some(2),
                batch_size: 4,
                queue_buffer_size: 1000,
            });

            let result = parser.parse_and_export_streaming(&files, exporter);
            assert!(result.is_ok());

            let results = result.unwrap();
            assert_eq!(results.len(), 3);

            let total_records: usize = results.iter().map(|(r, _)| r).sum();
            assert_eq!(total_records, expected_total);

            // 验证导出
            let exported = exported_records.lock().unwrap();
            assert_eq!(exported.len(), expected_total);

            // 验证多个批次
            assert!(*batch_count.lock().unwrap() > 1);
        }

        #[test]
        fn test_parse_and_export_streaming_with_delays() {
            let temp_dir = TempDir::new().unwrap();
            let content = create_complex_log_content(6, 0);
            let file = create_test_log_file(&temp_dir, "delay.log", &content);

            let start_time = std::time::Instant::now();
            let exporter = AdvancedStreamingExporter::new("streaming_delay")
                .with_delay(Duration::from_millis(50));

            let exported_records = exporter.exported_records.clone();
            let batch_count = exporter.batch_count.clone();

            let parser = ConcurrentParser::new(SqllogConfig {
                thread_count: Some(1),
                batch_size: 2, // 3个批次
                queue_buffer_size: 1000,
            });

            let result = parser.parse_and_export_streaming(&[file], exporter);
            let elapsed = start_time.elapsed();

            assert!(result.is_ok());

            let results = result.unwrap();
            assert_eq!(results[0].0, 6);

            // 验证时间包含延迟
            let expected_batches = *batch_count.lock().unwrap();
            let min_expected_delay =
                Duration::from_millis(50) * expected_batches as u32;
            assert!(elapsed >= min_expected_delay);

            // 验证记录
            let exported = exported_records.lock().unwrap();
            assert_eq!(exported.len(), 6);
        }

        #[test]
        fn test_parse_and_export_streaming_export_failure() {
            let temp_dir = TempDir::new().unwrap();
            let content = create_complex_log_content(8, 0);
            let file = create_test_log_file(&temp_dir, "fail.log", &content);

            let exporter = AdvancedStreamingExporter::new("streaming_fail")
                .with_failure_on_batch(2); // 第二个批次失败

            let parser = ConcurrentParser::new(SqllogConfig {
                thread_count: Some(1),
                batch_size: 3,
                queue_buffer_size: 1000,
            });

            let result = parser.parse_and_export_streaming(&[file], exporter);
            // 导出失败应该返回错误
            assert!(result.is_err());
        }

        #[test]
        fn test_parse_and_export_streaming_empty_files() {
            let exporter = AdvancedStreamingExporter::new("streaming_empty");

            let parser = ConcurrentParser::new(SqllogConfig::default());

            let result = parser.parse_and_export_streaming(&[], exporter);
            assert!(result.is_ok());

            let results = result.unwrap();
            assert_eq!(results.len(), 0);

            // 对于空文件列表，不强制要求 finalize 被调用，因为没有实际的处理发生
        }

        #[test]
        fn test_parse_and_export_streaming_thread_count_zero() {
            let temp_dir = TempDir::new().unwrap();
            let mut files = Vec::new();

            // 创建3个文件
            for i in 0..3 {
                let content = create_complex_log_content(4, i * 100);
                let file = create_test_log_file(
                    &temp_dir,
                    &format!("auto_{}.log", i),
                    &content,
                );
                files.push(file);
            }

            let exporter = AdvancedStreamingExporter::new("streaming_auto");
            let exported_records = exporter.exported_records.clone();

            let parser = ConcurrentParser::new(SqllogConfig {
                thread_count: Some(0), // 自动线程数
                batch_size: 2,
                queue_buffer_size: 1000,
            });

            let result = parser.parse_and_export_streaming(&files, exporter);
            assert!(result.is_ok());

            let results = result.unwrap();
            assert_eq!(results.len(), 3);

            let total_records: usize = results.iter().map(|(r, _)| r).sum();
            assert_eq!(total_records, 12); // 3 * 4

            let exported = exported_records.lock().unwrap();
            assert_eq!(exported.len(), 12);
        }
    }
}
