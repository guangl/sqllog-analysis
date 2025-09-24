use sqllog_analysis::sqllog::concurrent::ConcurrentParser;
use sqllog_analysis::config::SqllogConfig;
use std::io::Write;
use tempfile::NamedTempFile;

#[cfg(test)]
mod sync_parser_error_tests {
    use super::*;

    fn create_parser_and_test_file<F>(content: &str, config: SqllogConfig, test_fn: F)
    where
        F: FnOnce(ConcurrentParser, std::path::PathBuf),
    {
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        temp_file.write_all(content.as_bytes()).expect("Failed to write temp file");
        temp_file.flush().expect("Failed to flush temp file");

        let parser = ConcurrentParser::new(config);
        let file_path = temp_file.path().to_path_buf();
        test_fn(parser, file_path);
    }

    #[test]
    fn test_parse_file_with_invalid_utf8() {
        // 创建包含无效UTF-8的临时文件
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");

        // 写入无效的UTF-8字节序列
        let invalid_utf8_bytes = vec![
            // 有效的日志行前缀
            b"2023-06-15 12:30:45.123 (EP[0] sess:0x123 thrd:1 user:test trxid:1 stmt:0x456) [SEL]: ".to_vec(),
            // 无效的UTF-8字节
            vec![0xFF, 0xFE, 0xFD, 0xFC],
        ].concat();

        temp_file.write_all(&invalid_utf8_bytes).expect("Failed to write temp file");
        temp_file.flush().expect("Failed to flush temp file");

        let config = SqllogConfig::default();
        let parser = ConcurrentParser::new(config);
        let result = parser.parse_files_concurrent(&[temp_file.path().to_path_buf()]);

        // 应该能够处理无效UTF-8而不崩溃
        assert!(result.is_ok());
        let (records, errors) = result.unwrap();

        // 可能有错误，但不应该崩溃
        println!("Records: {}, Errors: {}", records.len(), errors.len());
    }

    #[test]
    fn test_parse_file_extremely_long_lines() {
        // 创建一个极长的SQL语句
        let long_sql = "SELECT ".to_string() + &"x".repeat(10000) + " FROM users";
        let content = format!(
            "2023-06-15 12:30:45.123 (EP[0] sess:0x123 thrd:1 user:test trxid:1 stmt:0x456) [SEL]: {}\n",
            long_sql
        );

        create_parser_and_test_file(&content, SqllogConfig::default(), |parser, file_path| {
            let result = parser.parse_files_concurrent(&[file_path]);

            assert!(result.is_ok());
            let (records, errors) = result.unwrap();

            if !records.is_empty() {
                assert!(records[0].description.len() > 10000);
            }

            println!("Records: {}, Errors: {}", records.len(), errors.len());
        });
    }

    #[test]
    fn test_parse_file_mixed_line_endings() {
        let content = "2023-06-15 12:30:45.123 (EP[0] sess:0x123 thrd:1 user:test trxid:1 stmt:0x456) [SEL]: SELECT 1\r\n\
                      2023-06-15 12:30:46.124 (EP[0] sess:0x124 thrd:2 user:test trxid:2 stmt:0x457) [INS]: INSERT INTO test VALUES (1)\n\
                      2023-06-15 12:30:47.125 (EP[0] sess:0x125 thrd:3 user:test trxid:3 stmt:0x458) [UPD]: UPDATE test SET a = 1\r\
                      2023-06-15 12:30:48.126 (EP[0] sess:0x126 thrd:4 user:test trxid:4 stmt:0x459) [DEL]: DELETE FROM test";

        create_parser_and_test_file(content, SqllogConfig::default(), |parser, file_path| {
            let result = parser.parse_files_concurrent(&[file_path]);

            assert!(result.is_ok());
            let (records, errors) = result.unwrap();

            // 应该能够正确处理不同的行结束符
            assert!(records.len() >= 3); // 至少应该解析出3条记录
            println!("Records: {}, Errors: {}", records.len(), errors.len());
        });
    }

    #[test]
    fn test_parse_file_with_zero_batch_size() {
        let content = "2023-06-15 12:30:45.123 (EP[0] sess:0x123 thrd:1 user:test trxid:1 stmt:0x456) [SEL]: SELECT 1\n\
                      2023-06-15 12:30:46.124 (EP[0] sess:0x124 thrd:2 user:test trxid:2 stmt:0x457) [INS]: INSERT INTO test VALUES (1)\n";

        // 使用batch_size = 0的配置
        let config = SqllogConfig {
            thread_count: Some(1),
            batch_size: 0, // 这应该触发整个文件一次性处理的路径
            queue_buffer_size: 1000,
            errors_out: None,
        };

        create_parser_and_test_file(content, config, |parser, file_path| {
            let result = parser.parse_files_concurrent(&[file_path]);

            assert!(result.is_ok());
            let (records, errors) = result.unwrap();

            assert_eq!(records.len(), 2);
            assert_eq!(errors.len(), 0);
        });
    }

    #[test]
    fn test_parse_file_with_small_batch_size() {
        // 创建多行测试内容
        let mut content = String::new();
        for i in 1..=10 {
            content.push_str(&format!(
                "2023-06-15 12:30:{:02}.{:03} (EP[0] sess:0x{:x} thrd:{} user:test trxid:{} stmt:0x{:x}) [SEL]: SELECT {}\n",
                45 + i % 60,
                123 + i,
                0x123 + i,
                i,
                i,
                0x456 + i,
                i
            ));
        }

        // 使用很小的batch_size来强制多次批处理
        let config = SqllogConfig {
            thread_count: Some(1),
            batch_size: 2, // 每次只处理2行
            queue_buffer_size: 1000,
            errors_out: None,
        };

        create_parser_and_test_file(&content, config, |parser, file_path| {
            let result = parser.parse_files_concurrent(&[file_path]);

            assert!(result.is_ok());
            let (records, errors) = result.unwrap();

            assert_eq!(records.len(), 10);
            assert_eq!(errors.len(), 0);

            // 验证所有记录都被正确解析
            for (i, record) in records.iter().enumerate() {
                assert!(record.description.contains(&format!("SELECT {}", i + 1)));
            }
        });
    }

    #[test]
    fn test_parse_empty_file() {
        create_parser_and_test_file("", SqllogConfig::default(), |parser, file_path| {
            let result = parser.parse_files_concurrent(&[file_path]);

            assert!(result.is_ok());
            let (records, errors) = result.unwrap();

            assert_eq!(records.len(), 0);
            assert_eq!(errors.len(), 0);
        });
    }

    #[test]
    fn test_parse_file_only_empty_lines() {
        let content = "\n\n\n\n\n";

        create_parser_and_test_file(content, SqllogConfig::default(), |parser, file_path| {
            let result = parser.parse_files_concurrent(&[file_path]);

            assert!(result.is_ok());
            let (records, errors) = result.unwrap();

            assert_eq!(records.len(), 0);
            assert_eq!(errors.len(), 0);
        });
    }

    #[test]
    fn test_parse_file_only_whitespace_lines() {
        let content = "   \n\t\t\n  \t \n\r\n";

        create_parser_and_test_file(content, SqllogConfig::default(), |parser, file_path| {
            let result = parser.parse_files_concurrent(&[file_path]);

            assert!(result.is_ok());
            let (records, errors) = result.unwrap();

            assert_eq!(records.len(), 0);
            assert_eq!(errors.len(), 0);
        });
    }

    #[test]
    fn test_parse_file_with_malformed_lines() {
        let content = "这不是一个有效的日志格式\n\
                      2023-06-15 12:30:45.123 (EP[0] sess:0x123 thrd:1 user:test trxid:1 stmt:0x456) [SEL]: SELECT 1\n\
                      另一个无效的行\n\
                      [2023-06-15 12:30:46.124] 错误的时间格式\n\
                      2023-06-15 12:30:47.125 (EP[0] sess:0x124 thrd:2 user:test trxid:2 stmt:0x457) [INS]: INSERT INTO test VALUES (1)\n";

        create_parser_and_test_file(content, SqllogConfig::default(), |parser, file_path| {
            let result = parser.parse_files_concurrent(&[file_path]);

            assert!(result.is_ok());
            let (records, errors) = result.unwrap();

            // 应该解析出2条有效记录，其他行产生错误
            assert_eq!(records.len(), 2);
            assert!(errors.len() > 0); // 应该有解析错误

            // 验证有效记录被正确解析
            assert!(records[0].description.contains("SELECT 1"));
            assert!(records[1].description.contains("INSERT INTO test"));
        });
    }

    #[test]
    fn test_parse_nonexistent_file() {
        let config = SqllogConfig::default();
        let parser = ConcurrentParser::new(config);
        let result = parser.parse_files_concurrent(&["nonexistent_file_12345.log".into()]);

        // 对于不存在的文件，系统行为可能不同，这里只验证不会崩溃
        match result {
            Ok((records, errors)) => {
                println!("Records: {}, Errors: {}", records.len(), errors.len());
                // 可以接受任何结果，只要不崩溃
            }
            Err(error) => {
                println!("Error: {}", error);
                // 错误也是可接受的
            }
        }
    }

    #[test]
    fn test_parse_file_with_very_large_batch_size() {
        let content = "2023-06-15 12:30:45.123 (EP[0] sess:0x123 thrd:1 user:test trxid:1 stmt:0x456) [SEL]: SELECT 1\n\
                      2023-06-15 12:30:46.124 (EP[0] sess:0x124 thrd:2 user:test trxid:2 stmt:0x457) [INS]: INSERT INTO test VALUES (1)\n";

        // 使用比文件行数大得多的batch_size
        let config = SqllogConfig {
            thread_count: Some(1),
            batch_size: 10000, // 远大于文件行数
            queue_buffer_size: 1000,
            errors_out: None,
        };

        create_parser_and_test_file(content, config, |parser, file_path| {
            let result = parser.parse_files_concurrent(&[file_path]);

            assert!(result.is_ok());
            let (records, errors) = result.unwrap();

            assert_eq!(records.len(), 2);
            assert_eq!(errors.len(), 0);
        });
    }

    #[test]
    fn test_parse_file_with_binary_content() {
        // 创建包含二进制数据的文件
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");

        // 先写一个有效的日志行
        let valid_line = "2023-06-15 12:30:45.123 (EP[0] sess:0x123 thrd:1 user:test trxid:1 stmt:0x456) [SEL]: SELECT 1\n";
        temp_file.write_all(valid_line.as_bytes()).expect("Failed to write temp file");

        // 然后写一些二进制数据
        let binary_data: Vec<u8> = (0..=255).collect();
        temp_file.write_all(&binary_data).expect("Failed to write temp file");

        // 再写一个有效的日志行
        let valid_line2 = "\n2023-06-15 12:30:46.124 (EP[0] sess:0x124 thrd:2 user:test trxid:2 stmt:0x457) [INS]: INSERT INTO test VALUES (1)\n";
        temp_file.write_all(valid_line2.as_bytes()).expect("Failed to write temp file");

        temp_file.flush().expect("Failed to flush temp file");

        let config = SqllogConfig::default();
        let parser = ConcurrentParser::new(config);
        let result = parser.parse_files_concurrent(&[temp_file.path().to_path_buf()]);

        // 应该能处理包含二进制数据的文件而不崩溃
        assert!(result.is_ok());
        let (records, errors) = result.unwrap();

        // 可能解析出部分有效记录
        println!("Records: {}, Errors: {}", records.len(), errors.len());
        // 至少应该有一条有效记录
        assert!(records.len() >= 1);
    }

    #[test]
    fn test_parse_multiple_files_with_errors() {
        // 创建多个测试文件，一些有效，一些无效
        let valid_content = "2023-06-15 12:30:45.123 (EP[0] sess:0x123 thrd:1 user:test trxid:1 stmt:0x456) [SEL]: SELECT 1\n";
        let invalid_content = "这些都不是有效的日志行\n无效行2\n无效行3\n";

        let mut temp_file1 = NamedTempFile::new().expect("Failed to create temp file 1");
        temp_file1.write_all(valid_content.as_bytes()).expect("Failed to write temp file 1");
        temp_file1.flush().expect("Failed to flush temp file 1");

        let mut temp_file2 = NamedTempFile::new().expect("Failed to create temp file 2");
        temp_file2.write_all(invalid_content.as_bytes()).expect("Failed to write temp file 2");
        temp_file2.flush().expect("Failed to flush temp file 2");

        let config = SqllogConfig::default();
        let parser = ConcurrentParser::new(config);
        let files = vec![temp_file1.path().to_path_buf(), temp_file2.path().to_path_buf()];
        let result = parser.parse_files_concurrent(&files);

        assert!(result.is_ok());
        let (records, errors) = result.unwrap();

        // 应该有1条有效记录和多个错误
        assert_eq!(records.len(), 1);
        assert!(errors.len() > 0);
        assert!(records[0].description.contains("SELECT 1"));
    }
}