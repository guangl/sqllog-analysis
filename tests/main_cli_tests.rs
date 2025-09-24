use sqllog_analysis::config::SqllogConfig;
use sqllog_analysis::sqllog::concurrent::ConcurrentParser;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// 创建测试日志文件
fn create_test_log_content() -> String {
    r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) [SEL]: SELECT * FROM users WHERE id = 1
2025-09-16 20:02:54.456 (EP[0] sess:0x6da8ccef1 thrd:4146218 user:EDM_BASE trxid:122154453027 stmt:0x6da900ef1) [UPD]: UPDATE users SET last_login = NOW() WHERE id = 1
2025-09-16 20:02:55.789 (EP[0] sess:0x6da8ccef2 thrd:4146219 user:EDM_BASE trxid:122154453028 stmt:0x6da900ef2) [ERR]: INVALID SYNTAX: SELECT * FRM products
"#.to_string()
}

/// 测试解析功能的核心逻辑
#[test]
fn test_parse_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("test.log");
    fs::write(&log_path, create_test_log_content()).unwrap();

    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);

    let files = vec![log_path];
    let result = parser.parse_files_concurrent(&files);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert!(!records.is_empty());
    println!("成功解析记录: {} 条", records.len());
    println!("解析错误: {} 个", errors.len());
}

/// 测试多个文件解析
#[test]
fn test_parse_multiple_files() {
    let temp_dir = TempDir::new().unwrap();
    let log_path1 = temp_dir.path().join("test1.log");
    let log_path2 = temp_dir.path().join("test2.log");

    fs::write(&log_path1, create_test_log_content()).unwrap();
    fs::write(&log_path2, create_test_log_content()).unwrap();

    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);

    let files = vec![log_path1, log_path2];
    let result = parser.parse_files_concurrent(&files);

    assert!(result.is_ok());
    let (records, errors) = result.unwrap();
    assert!(!records.is_empty());
    println!(
        "多文件解析 - 记录数: {}, 错误数: {}",
        records.len(),
        errors.len()
    );
}

/// 测试自定义配置
#[test]
fn test_parse_with_custom_config() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("test.log");
    fs::write(&log_path, create_test_log_content()).unwrap();

    let mut config = SqllogConfig::default();
    config.batch_size = 1;
    config.thread_count = Some(1);

    let parser = ConcurrentParser::new(config);
    let files = vec![log_path];
    let result = parser.parse_files_concurrent(&files);

    assert!(result.is_ok());
    let (records, _) = result.unwrap();
    assert!(!records.is_empty());
}

/// 测试不存在的文件处理
#[test]
fn test_parse_nonexistent_file() {
    let nonexistent = PathBuf::from("nonexistent_file.log");
    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);

    let files = vec![nonexistent];
    let result = parser.parse_files_concurrent(&files);

    // 应该返回错误或者空结果
    if result.is_ok() {
        let (records, _) = result.unwrap();
        assert_eq!(records.len(), 0);
    } else {
        // 错误也是可以接受的
        assert!(result.is_err());
    }
}

#[cfg(feature = "exporter-csv")]
/// 测试 CSV 导出功能
#[test]
fn test_csv_export_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("test.log");
    let output_path = temp_dir.path().join("output.csv");

    fs::write(&log_path, create_test_log_content()).unwrap();

    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);

    use sqllog_analysis::exporter::sync_impl::SyncCsvExporter;
    let exporter = SyncCsvExporter::new(output_path.to_str().unwrap()).unwrap();

    let files = vec![log_path];
    let result = parser.parse_and_export_streaming(&files, exporter);

    assert!(result.is_ok());
    assert!(output_path.exists());
}

#[cfg(feature = "exporter-json")]
/// 测试 JSON 导出功能
#[test]
fn test_json_export_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("test.log");
    let output_path = temp_dir.path().join("output.json");

    fs::write(&log_path, create_test_log_content()).unwrap();

    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);

    use sqllog_analysis::exporter::sync_impl::SyncJsonExporter;
    let exporter =
        SyncJsonExporter::new(output_path.to_str().unwrap()).unwrap();

    let files = vec![log_path];
    let result = parser.parse_and_export_streaming(&files, exporter);

    assert!(result.is_ok());
    assert!(output_path.exists());
}

/// 测试空文件处理
#[test]
fn test_parse_empty_file() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("empty.log");
    fs::write(&log_path, "").unwrap();

    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);

    let files = vec![log_path];
    let result = parser.parse_files_concurrent(&files);

    assert!(result.is_ok());
    let (records, _) = result.unwrap();
    assert_eq!(records.len(), 0);
}

/// 测试大文件批处理
#[test]
fn test_parse_large_file_with_batch_size() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("large.log");

    // 创建较大的文件内容
    let mut large_content = String::new();
    for i in 0..100 {
        large_content.push_str(&format!(
            "2025-09-16 20:02:{:02}.123 (EP[0] sess:0x6da8ccef{} thrd:414621{} user:EDM_BASE trxid:12215445302{} stmt:0x6da900ef{}) [SEL]: SELECT * FROM table{}\n",
            i % 60, i % 10, i % 10, i % 10, i, i
        ));
    }
    fs::write(&log_path, large_content).unwrap();

    let mut config = SqllogConfig::default();
    config.batch_size = 10;
    config.thread_count = Some(2);

    let parser = ConcurrentParser::new(config);
    let files = vec![log_path];
    let result = parser.parse_files_concurrent(&files);

    assert!(result.is_ok());
    let (records, _) = result.unwrap();
    assert!(!records.is_empty());
}

/// 测试线程数为 None 的情况（自动检测）
#[test]
fn test_parse_with_auto_threads() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("test.log");
    fs::write(&log_path, create_test_log_content()).unwrap();

    let mut config = SqllogConfig::default();
    config.thread_count = None; // 自动检测线程数

    let parser = ConcurrentParser::new(config);
    let files = vec![log_path];
    let result = parser.parse_files_concurrent(&files);

    assert!(result.is_ok());
}

/// 测试错误处理和记录数显示
#[test]
fn test_parse_with_invalid_content() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("invalid.log");

    // 创建包含无效内容的文件
    let invalid_content =
        "这是无效的日志格式\n不符合预期的格式\n完全错误的内容\n";
    fs::write(&log_path, invalid_content).unwrap();

    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);

    let files = vec![log_path];
    let result = parser.parse_files_concurrent(&files);

    assert!(result.is_ok());
    let (_records, errors) = result.unwrap();
    assert!(!errors.is_empty()); // 应该有解析错误
    println!("解析错误数量: {}", errors.len());

    // 测试错误信息
    if !errors.is_empty() {
        for (i, error) in errors.iter().take(3).enumerate() {
            println!("错误 {}: 行 {} - {}", i + 1, error.line, error.error);
        }
    }
}

/// 测试记录输出和显示
#[test]
fn test_parse_shows_sample_records() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("test.log");

    // 创建多条记录
    let content = r#"2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0) [SEL]: SELECT * FROM users WHERE id = 1
2025-09-16 20:02:54.456 (EP[0] sess:0x6da8ccef1 thrd:4146218 user:EDM_BASE trxid:122154453027 stmt:0x6da900ef1) [UPD]: UPDATE users SET last_login = NOW() WHERE id = 1
2025-09-16 20:02:55.789 (EP[0] sess:0x6da8ccef2 thrd:4146219 user:EDM_BASE trxid:122154453028 stmt:0x6da900ef2) [DEL]: DELETE FROM temp_table WHERE created < '2024-01-01'
2025-09-16 20:02:56.012 (EP[0] sess:0x6da8ccef3 thrd:4146220 user:EDM_BASE trxid:122154453029 stmt:0x6da900ef3) [INS]: INSERT INTO logs (message) VALUES ('Test message')
"#;
    fs::write(&log_path, content).unwrap();

    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);

    let files = vec![log_path];
    let result = parser.parse_files_concurrent(&files);

    assert!(result.is_ok());
    let (records, _) = result.unwrap();
    assert!(!records.is_empty());

    // 显示前几条记录
    println!("前几条记录:");
    for (i, record) in records.iter().take(3).enumerate() {
        println!(
            "  {}. {} [{}] {}",
            i + 1,
            record.occurrence_time,
            record.sql_type.as_deref().unwrap_or("UNKNOWN"),
            record.description.chars().take(50).collect::<String>()
        );
    }
}

/// 测试各种配置组合
#[test]
fn test_various_config_combinations() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("test.log");
    fs::write(&log_path, create_test_log_content()).unwrap();

    let configs = vec![
        SqllogConfig {
            batch_size: 1,
            thread_count: Some(1),
            queue_buffer_size: 100,
            errors_out: None,
        },
        SqllogConfig {
            batch_size: 5,
            thread_count: Some(2),
            queue_buffer_size: 1000,
            errors_out: None,
        },
        SqllogConfig {
            batch_size: 10,
            thread_count: None,
            queue_buffer_size: 5000,
            errors_out: None,
        },
    ];

    for config in configs {
        let parser = ConcurrentParser::new(config);
        let files = vec![log_path.clone()];
        let result = parser.parse_files_concurrent(&files);
        assert!(result.is_ok());
    }
}
