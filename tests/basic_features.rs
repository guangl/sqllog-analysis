//! 基础功能集成测试
//!
//! 测试不依赖任何可选 feature 的基础功能

mod common;

use sqllog_analysis::{
    config::{Config, SqllogConfig},
    sqllog::{ConcurrentParser, SyncSqllogParser},
};
use std::path::PathBuf;
use tempfile::TempDir;

#[test]
fn test_sync_parser_basic() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = common::create_test_sqllog(
        &temp_dir,
        "basic_test.log",
        common::SAMPLE_SQLLOG_CONTENT,
    );

    // 测试同步解析器基础功能
    let mut all_records = Vec::new();
    let mut all_errors = Vec::new();

    let result = SyncSqllogParser::parse_with_hooks(
        &file_path,
        1000, // 大批次，一次性处理所有记录
        |records, errors| {
            all_records.extend_from_slice(records);
            all_errors.extend_from_slice(errors);
        },
    );

    assert!(result.is_ok(), "基础解析功能失败: {:?}", result.err());
    assert!(!all_records.is_empty(), "应该解析出记录");
    println!(
        "✅ 同步解析器基础功能测试通过: {} 条记录, {} 个错误",
        all_records.len(),
        all_errors.len()
    );
}

#[test]
fn test_sync_parser_batch() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = common::create_test_sqllog(
        &temp_dir,
        "batch_test.log",
        common::COMPLEX_SQLLOG_CONTENT,
    );

    // 测试批处理解析
    let mut total_records = 0;
    let mut total_errors = 0;
    let mut batch_count = 0;

    let result = SyncSqllogParser::parse_with_hooks(
        &file_path,
        2, // 每批2条记录
        |batch_records, batch_errors| {
            total_records += batch_records.len();
            total_errors += batch_errors.len();
            batch_count += 1;
        },
    );

    assert!(result.is_ok(), "批处理解析失败: {:?}", result.err());
    assert!(total_records > 0, "应该解析出记录");
    assert!(batch_count > 0, "应该有批次处理");

    println!(
        "✅ 同步解析器批处理功能测试通过: {} 条记录, {} 个错误, {} 个批次",
        total_records, total_errors, batch_count
    );
}

#[test]
fn test_concurrent_parser_basic() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let files = common::create_multiple_test_files(&temp_dir, 3);

    // 测试并发解析器基础功能
    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);

    let result = parser.parse_files_concurrent(&files);
    assert!(result.is_ok(), "并发解析功能失败: {:?}", result.err());

    let (records, errors) = result.unwrap();
    assert!(!records.is_empty(), "应该解析出记录");

    println!(
        "✅ 并发解析器基础功能测试通过: {} 条记录, {} 个错误",
        records.len(),
        errors.len()
    );
}

#[test]
fn test_config_loading() {
    // 测试配置加载
    let config = SqllogConfig::default();
    // batch_size 为 0 表示不分块，直接解析整个文件

    // 测试从文件加载配置
    let file_config = Config::from_file("config.toml");
    assert!(file_config.is_ok(), "应该能够加载配置文件");

    if let Ok(cfg) = file_config {
        assert!(cfg.sqllog.batch_size > 0, "文件配置中 batch_size 应该大于 0");
    }

    println!(
        "✅ 配置加载测试通过: batch_size={}, thread_count={:?}",
        config.batch_size, config.thread_count
    );
}

#[test]
fn test_error_handling() {
    // 测试错误处理
    let non_existent_file = PathBuf::from("non_existent_file.log");

    let result = SyncSqllogParser::parse_with_hooks(
        &non_existent_file,
        1000,
        |_records, _errors| {},
    );
    assert!(result.is_err(), "应该返回错误");

    println!("✅ 错误处理测试通过");
}

#[test]
fn test_empty_file_handling() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let file_path = common::create_test_sqllog(&temp_dir, "empty_test.log", "");

    let mut all_records = Vec::new();
    let mut all_errors = Vec::new();

    let result = SyncSqllogParser::parse_with_hooks(
        &file_path,
        1000,
        |records, errors| {
            all_records.extend_from_slice(records);
            all_errors.extend_from_slice(errors);
        },
    );

    assert!(result.is_ok(), "空文件解析应该成功");
    assert!(all_records.is_empty(), "空文件应该没有记录");

    println!("✅ 空文件处理测试通过");
}
