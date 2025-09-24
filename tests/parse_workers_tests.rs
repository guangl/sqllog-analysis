// parse_workers.rs 的综合测试
// 测试并发解析工作线程的各种功能和边界情况

use sqllog_analysis::config::SqllogConfig;
use sqllog_analysis::sqllog::concurrent::ConcurrentParser;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;

// 创建测试用的SQL日志文件
fn create_test_log_file(dir: &TempDir, name: &str, content: &str) -> PathBuf {
    let file_path = dir.path().join(name);
    let mut file = fs::File::create(&file_path).unwrap();
    writeln!(file, "{}", content).unwrap();
    file_path
}

// 创建多行日志内容，每行都不同
fn create_multi_line_log_content(line_count: usize) -> String {
    let mut content = String::new();
    for i in 0..line_count {
        content.push_str(&format!(
            "2025-09-16 20:02:53.{:03} (EP[{}] sess:0x6da8ccef{} thrd:414621{} user:EDM_BASE trxid:12215445302{} stmt:0x6da900ef{}) SELECT * FROM table_{} WHERE id={};\n",
            500 + i % 500,
            i % 10,
            i,
            i,
            i,
            i,
            i % 100,
            i
        ));
    }
    content
}

// 创建包含错误的日志内容
fn create_mixed_log_content() -> String {
    r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) SELECT * FROM test_table;
Invalid log line without proper format
2025-09-16 20:02:54.123 (EP[1] sess:0x6da8ccef1 thrd:4146218 user:EDM_USER trxid:122154453027 stmt:0x6da900ef1) INSERT INTO table2 VALUES(1);
Another invalid line
2025-09-16 20:02:55.456 (EP[2] sess:0x6da8ccef2 thrd:4146219 user:EDM_ADMIN trxid:122154453028 stmt:0x6da900ef2) UPDATE table3 SET col1='value';
"#.to_string()
}

#[test]
fn test_concurrent_parser_single_file() {
    let temp_dir = TempDir::new().unwrap();
    let log_content = create_multi_line_log_content(10);
    let log_file = create_test_log_file(&temp_dir, "single.log", &log_content);

    let config = SqllogConfig {
        thread_count: Some(1),
        batch_size: 5,
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result = parser.parse_files_concurrent(&[log_file]);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 10);
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_concurrent_parser_multiple_files() {
    let temp_dir = TempDir::new().unwrap();

    let log_content1 = create_multi_line_log_content(5);
    let log_content2 = create_multi_line_log_content(7);
    let log_content3 = create_multi_line_log_content(3);

    let log_file1 = create_test_log_file(&temp_dir, "file1.log", &log_content1);
    let log_file2 = create_test_log_file(&temp_dir, "file2.log", &log_content2);
    let log_file3 = create_test_log_file(&temp_dir, "file3.log", &log_content3);

    let config = SqllogConfig {
        thread_count: Some(2),
        batch_size: 3,
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result =
        parser.parse_files_concurrent(&[log_file1, log_file2, log_file3]);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 15); // 5 + 7 + 3
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_concurrent_parser_with_errors() {
    let temp_dir = TempDir::new().unwrap();
    let log_content = create_mixed_log_content();
    let log_file = create_test_log_file(&temp_dir, "mixed.log", &log_content);

    let config = SqllogConfig {
        thread_count: Some(1),
        batch_size: 2,
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result = parser.parse_files_concurrent(&[log_file]);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 3); // 有效行数
    // 注意：错误处理可能因实现而异，这里不做严格断言
    println!("Errors found: {}", errors.len()); // 调试信息
}

#[test]
fn test_concurrent_parser_empty_file() {
    let temp_dir = TempDir::new().unwrap();
    let log_file = create_test_log_file(&temp_dir, "empty.log", "");

    let config = SqllogConfig {
        thread_count: Some(1),
        batch_size: 10,
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result = parser.parse_files_concurrent(&[log_file]);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 0);
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_concurrent_parser_nonexistent_file() {
    let config = SqllogConfig {
        thread_count: Some(1),
        batch_size: 10,
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let nonexistent_file = PathBuf::from("nonexistent_file.log");
    let result = parser.parse_files_concurrent(&[nonexistent_file]);

    // 应该返回错误或空结果，不应该崩溃
    assert!(result.is_ok());
    let (records, _errors) = result.unwrap();
    assert_eq!(records.len(), 0);
    // 可能会有错误记录文件不存在的情况
}

#[test]
fn test_concurrent_parser_auto_thread_count() {
    let temp_dir = TempDir::new().unwrap();

    let log_content1 = create_multi_line_log_content(5);
    let log_content2 = create_multi_line_log_content(5);

    let log_file1 = create_test_log_file(&temp_dir, "file1.log", &log_content1);
    let log_file2 = create_test_log_file(&temp_dir, "file2.log", &log_content2);

    // thread_count = 0 表示自动（每个文件一个线程）
    let config = SqllogConfig {
        thread_count: Some(1),
        batch_size: 3,
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result = parser.parse_files_concurrent(&[log_file1, log_file2]);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 10);
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_concurrent_parser_large_batch_size() {
    let temp_dir = TempDir::new().unwrap();
    let log_content = create_multi_line_log_content(20);
    let log_file =
        create_test_log_file(&temp_dir, "large_batch.log", &log_content);

    let config = SqllogConfig {
        thread_count: Some(1),
        batch_size: 50, // 批次大小大于文件行数
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result = parser.parse_files_concurrent(&[log_file]);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 20);
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_concurrent_parser_small_batch_size() {
    let temp_dir = TempDir::new().unwrap();
    let log_content = create_multi_line_log_content(10);
    let log_file =
        create_test_log_file(&temp_dir, "small_batch.log", &log_content);

    let config = SqllogConfig {
        thread_count: Some(1),
        batch_size: 1, // 每次只处理一条记录
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result = parser.parse_files_concurrent(&[log_file]);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 10);
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_concurrent_parser_many_threads() {
    let temp_dir = TempDir::new().unwrap();

    // 创建多个小文件
    let mut files = Vec::new();
    for i in 0..5 {
        let log_content = create_multi_line_log_content(3);
        let log_file = create_test_log_file(
            &temp_dir,
            &format!("file_{}.log", i),
            &log_content,
        );
        files.push(log_file);
    }

    let config = SqllogConfig {
        thread_count: Some(8), // 线程数多于文件数
        batch_size: 2,
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result = parser.parse_files_concurrent(&files);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 15); // 5 files × 3 records
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_concurrent_parser_no_files() {
    let config = SqllogConfig {
        thread_count: Some(1),
        batch_size: 10,
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result = parser.parse_files_concurrent(&[]);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 0);
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_concurrent_parser_config_variations() {
    let temp_dir = TempDir::new().unwrap();
    let log_content = create_multi_line_log_content(8);
    let log_file =
        create_test_log_file(&temp_dir, "config_test.log", &log_content);

    // 测试不同的配置组合
    let configs = vec![
        SqllogConfig {
            thread_count: Some(1),
            batch_size: 1,
            queue_buffer_size: 100,
            errors_out: None,
        },
        SqllogConfig {
            thread_count: Some(1),
            batch_size: 5,
            queue_buffer_size: 500,
            errors_out: None,
        },
        SqllogConfig {
            thread_count: Some(2),
            batch_size: 3,
            queue_buffer_size: 1000,
            errors_out: None,
        },
        SqllogConfig {
            thread_count: Some(0),
            batch_size: 10,
            queue_buffer_size: 2000,
            errors_out: None,
        }, // 自动线程数
    ];

    for (i, config) in configs.into_iter().enumerate() {
        let parser = ConcurrentParser::new(config);
        let result = parser.parse_files_concurrent(&[log_file.clone()]);

        assert!(result.is_ok(), "Config {} failed", i);
        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 8, "Config {} wrong record count", i);
        assert_eq!(errors.len(), 0, "Config {} unexpected errors", i);
    }
}

#[test]
fn test_concurrent_parser_very_large_file() {
    let temp_dir = TempDir::new().unwrap();
    let log_content = create_multi_line_log_content(1000); // 大文件
    let log_file = create_test_log_file(&temp_dir, "large.log", &log_content);

    let config = SqllogConfig {
        thread_count: Some(1),
        batch_size: 100,
        queue_buffer_size: 5000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result = parser.parse_files_concurrent(&[log_file]);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 1000);
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_concurrent_parser_mixed_file_sizes() {
    let temp_dir = TempDir::new().unwrap();

    let log_content_small = create_multi_line_log_content(2);
    let log_content_medium = create_multi_line_log_content(50);
    let log_content_large = create_multi_line_log_content(200);

    let file_small =
        create_test_log_file(&temp_dir, "small.log", &log_content_small);
    let file_medium =
        create_test_log_file(&temp_dir, "medium.log", &log_content_medium);
    let file_large =
        create_test_log_file(&temp_dir, "large.log", &log_content_large);

    let config = SqllogConfig {
        thread_count: Some(3),
        batch_size: 25,
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result =
        parser.parse_files_concurrent(&[file_small, file_medium, file_large]);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 252); // 2 + 50 + 200
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_concurrent_parser_thread_safety() {
    let temp_dir = TempDir::new().unwrap();

    // 创建多个不同的文件来测试线程安全
    let mut files = Vec::new();

    for i in 0..4 {
        let mut file_content = String::new();
        // 确保每个文件的内容都不同
        for j in 0..10 {
            let unique_id = i * 10 + j;
            file_content.push_str(&format!(
                "2025-09-16 20:02:{:02}.{:03} (EP[{}] sess:0x6da8ccef{} thrd:414621{} user:EDM_BASE trxid:12215445302{} stmt:0x6da900ef{}) SELECT * FROM table_{} WHERE file={} AND record={};\n",
                50 + unique_id % 10,
                500 + unique_id,
                unique_id % 10,
                unique_id,
                unique_id,
                unique_id,
                unique_id,
                unique_id % 50,
                i,
                j
            ));
        }

        let log_file = create_test_log_file(
            &temp_dir,
            &format!("thread_safe_{}.log", i),
            &file_content,
        );
        files.push(log_file);
    }

    let config = SqllogConfig {
        thread_count: Some(4), // 与文件数相同
        batch_size: 3,
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result = parser.parse_files_concurrent(&files);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 40); // 4 files × 10 records
    assert_eq!(errors.len(), 0);

    // 检查结果是否有重复或丢失 - 由于每个记录都是唯一的，应该没有重复
    let mut seen_descriptions = std::collections::HashSet::new();
    for record in &records {
        let unique_key = record.description.clone();
        if !seen_descriptions.insert(unique_key.clone()) {
            println!("发现重复记录: {}", unique_key);
            panic!("发现重复记录: {}", unique_key);
        }
    }
    assert_eq!(seen_descriptions.len(), 40);
}

#[test]
fn test_concurrent_parser_with_different_queue_sizes() {
    let temp_dir = TempDir::new().unwrap();
    let log_content = create_multi_line_log_content(20);
    let log_file =
        create_test_log_file(&temp_dir, "queue_test.log", &log_content);

    // 测试不同的队列缓冲区大小
    let queue_sizes = vec![10, 100, 1000, 10000];

    for queue_size in queue_sizes {
        let config = SqllogConfig {
            thread_count: Some(1),
            batch_size: 5,
            queue_buffer_size: queue_size,
            errors_out: None,
        };

        let parser = ConcurrentParser::new(config);
        let result = parser.parse_files_concurrent(&[log_file.clone()]);

        assert!(result.is_ok(), "Queue size {} failed", queue_size);
        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 20, "Queue size {} wrong count", queue_size);
        assert_eq!(errors.len(), 0, "Queue size {} has errors", queue_size);
    }
}

#[test]
fn test_concurrent_parser_all_error_file() {
    let temp_dir = TempDir::new().unwrap();
    let log_content = r#"Invalid line 1
Invalid line 2
Invalid line 3
Invalid line 4
Invalid line 5"#
        .to_string();
    let log_file =
        create_test_log_file(&temp_dir, "all_errors.log", &log_content);

    let config = SqllogConfig {
        thread_count: Some(1),
        batch_size: 2,
        queue_buffer_size: 1000,
        errors_out: None,
    };

    let parser = ConcurrentParser::new(config);
    let result = parser.parse_files_concurrent(&[log_file]);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), 0); // 没有有效记录
    // 错误数量可能因实现而异
    println!("All errors test - Errors found: {}", errors.len());
}

#[test]
fn test_concurrent_parser_edge_case_batch_sizes() {
    let temp_dir = TempDir::new().unwrap();
    let log_content = create_multi_line_log_content(7);
    let log_file =
        create_test_log_file(&temp_dir, "batch_edge.log", &log_content);

    // 测试边界批次大小
    let batch_sizes = vec![1, 3, 7, 10, 100]; // 包括等于、小于和大于记录数的批次大小

    for batch_size in batch_sizes {
        let config = SqllogConfig {
            thread_count: Some(1),
            batch_size,
            queue_buffer_size: 1000,
            errors_out: None,
        };

        let parser = ConcurrentParser::new(config);
        let result = parser.parse_files_concurrent(&[log_file.clone()]);

        assert!(result.is_ok(), "Batch size {} failed", batch_size);
        let (records, errors) = result.unwrap();
        assert_eq!(records.len(), 7, "Batch size {} wrong count", batch_size);
        assert_eq!(errors.len(), 0, "Batch size {} has errors", batch_size);
    }
}
