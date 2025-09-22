// 错误写入功能的集成测试

use sqllog_analysis::config::RuntimeConfig;
use sqllog_analysis::database::process_files_with_independent_databases;
use std::fs;
use std::io::Write;
use tempfile::{NamedTempFile, tempdir};

#[test]
fn test_error_writing_integration() {
    // 创建临时目录和文件
    let temp_dir = tempdir().unwrap();
    let log_dir = temp_dir.path();

    // 创建带有错误行的测试 sqllog 文件
    let mut log_file = NamedTempFile::new_in(&log_dir).unwrap();

    // 写入测试数据：包含正确和错误的行
    writeln!(log_file, "2025-09-21 12:00:00.000 (EP[1] sess:NULL thrd:1 user:usr trxid:1 stmt:NULL) [SEL]: select 1 EXECTIME: 1(ms) ROWCOUNT: 1 EXEC_ID: 1.").unwrap();
    log_file.write_all(&[0xFF, 0xFE, 0xFD]).unwrap(); // 无效 UTF8 字节
    writeln!(log_file, "").unwrap();
    writeln!(log_file, "2025-09-21 12:00:01.000 (EP[1] sess:NULL thrd:1 user:usr trxid:2 stmt:NULL) [SEL]: select 2 EXECTIME: 2(ms) ROWCOUNT: 1 EXEC_ID: 2.").unwrap();
    log_file.flush().unwrap();

    // 重命名文件为符合规则的名称
    let final_log_path = log_dir.join("dmsql_test_20250921_120000.log");
    fs::copy(log_file.path(), &final_log_path).unwrap();

    // 创建错误输出文件路径
    let error_file_path = temp_dir.path().join("test_errors.jsonl");

    // 创建运行时配置
    let runtime_config = RuntimeConfig {
        db_path: temp_dir
            .path()
            .join("test.duckdb")
            .to_string_lossy()
            .to_string(),
        enable_stdout: true,
        log_dir: Some(temp_dir.path().to_path_buf()),
        log_level: log::LevelFilter::Debug,
        sqllog_dir: Some(log_dir.to_path_buf()),
        sqllog_chunk_size: Some(0),
        parser_threads: 1,
        sqllog_write_errors: true, // 启用错误写入
        sqllog_errors_out_path: Some(error_file_path.clone()),
        export_enabled: false,
        export_format: "csv".to_string(),
        export_out_path: None,
        export_options: sqllog_analysis::config::ExportOptions {
            per_thread_out: false,
            write_flags: sqllog_analysis::config::WriteFlags {
                overwrite_or_ignore: true,
                overwrite: true,
                append: false,
            },
            file_size_bytes: None,
        },
        use_in_memory: true,
    };

    // 处理文件
    let files = vec![final_log_path.clone()];
    println!("处理文件: {:?}", files);
    println!("错误写入配置: {}", runtime_config.sqllog_write_errors);
    println!("错误文件路径: {:?}", runtime_config.sqllog_errors_out_path);

    let result =
        process_files_with_independent_databases(&files, &runtime_config);

    // 验证处理结果
    assert!(result.is_ok());
    let stats = result.unwrap();
    println!("处理统计: {:?}", stats);
    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.records_processed, 2); // 只有 2 行应该成功解析

    // 验证错误文件是否创建并包含预期内容
    assert!(error_file_path.exists(), "错误文件应该被创建");

    let error_content = fs::read_to_string(&error_file_path).unwrap();
    println!("实际错误文件内容:");
    println!("{}", error_content);

    let error_lines: Vec<&str> = error_content.trim().split('\n').collect();

    // 先输出调试信息
    println!("错误行数: {}", error_lines.len());
    for (i, line) in error_lines.iter().enumerate() {
        println!("错误行 {}: {}", i + 1, line);
    }

    // 应该有 1 个或更多错误行（UTF8 错误）
    assert!(
        error_lines.len() >= 1,
        "应该至少有 1 个错误行，实际有 {}",
        error_lines.len()
    );

    // 验证错误的 JSON 格式和内容
    let first_error: serde_json::Value =
        serde_json::from_str(error_lines[0]).unwrap();
    assert_eq!(
        first_error["path"],
        final_log_path.to_string_lossy().to_string()
    );
    assert!(first_error["line"].is_number());
    assert!(first_error["error"].is_string());

    // 检查是否包含 UTF8 相关错误
    let error_msg = first_error["error"].as_str().unwrap();
    assert!(
        error_msg.to_lowercase().contains("utf") || error_msg.contains("解码"),
        "错误信息应该包含 UTF8 相关内容: {}",
        error_msg
    );

    println!("错误写入功能测试通过！");
    println!("错误文件内容:");
    println!("{}", error_content);
}

#[test]
fn test_error_writing_disabled() {
    // 创建临时目录和文件
    let temp_dir = tempdir().unwrap();
    let log_dir = temp_dir.path();

    // 创建带有错误行的测试 sqllog 文件
    let mut log_file = NamedTempFile::new_in(&log_dir).unwrap();
    writeln!(log_file, "这是一个无效的日志行").unwrap();
    log_file.flush().unwrap();

    let final_log_path =
        log_dir.join("dmsql_test_disabled_20250921_120000.log");
    fs::copy(log_file.path(), &final_log_path).unwrap();

    let error_file_path = temp_dir.path().join("should_not_exist.jsonl");

    // 创建运行时配置，禁用错误写入
    let runtime_config = RuntimeConfig {
        db_path: temp_dir
            .path()
            .join("test.duckdb")
            .to_string_lossy()
            .to_string(),
        enable_stdout: true,
        log_dir: Some(temp_dir.path().to_path_buf()),
        log_level: log::LevelFilter::Debug,
        sqllog_dir: Some(log_dir.to_path_buf()),
        sqllog_chunk_size: Some(0),
        parser_threads: 1,
        sqllog_write_errors: false, // 禁用错误写入
        sqllog_errors_out_path: Some(error_file_path.clone()),
        export_enabled: false,
        export_format: "csv".to_string(),
        export_out_path: None,
        export_options: sqllog_analysis::config::ExportOptions {
            per_thread_out: false,
            write_flags: sqllog_analysis::config::WriteFlags {
                overwrite_or_ignore: true,
                overwrite: true,
                append: false,
            },
            file_size_bytes: None,
        },
        use_in_memory: true,
    };

    // 处理文件
    let files = vec![final_log_path];
    let result =
        process_files_with_independent_databases(&files, &runtime_config);

    // 验证处理结果
    assert!(result.is_ok());

    // 验证错误文件不应该被创建
    assert!(!error_file_path.exists(), "错误写入禁用时，错误文件不应该被创建");
}
