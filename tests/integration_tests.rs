//! 组合功能集成测试
//!
//! 测试多个功能特性组合使用的场景

mod common;

use sqllog_analysis::{config::SqllogConfig, sqllog::ConcurrentParser};
use tempfile::TempDir;

#[test]
fn test_basic_integration() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let files = common::create_multiple_test_files(&temp_dir, 5);

    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);

    let result = parser.parse_files_concurrent(&files);
    assert!(result.is_ok(), "基础集成测试应该成功");

    let (records, errors) = result.unwrap();
    assert_eq!(records.len(), files.len(), "记录数应该等于文件数");

    println!(
        "✅ 基础集成测试通过: {} 个文件, {} 条记录, {} 个错误",
        files.len(),
        records.len(),
        errors.len()
    );
}

#[cfg(feature = "logging")]
#[test]
fn test_logging_integration() {
    use tracing_subscriber;

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let files = common::create_multiple_test_files(&temp_dir, 3);

        let config = SqllogConfig::default();
        let parser = ConcurrentParser::new(config);

        let result = parser.parse_files_concurrent(&files);
        assert!(result.is_ok(), "日志集成测试应该成功");

        let (records, errors) = result.unwrap();
        println!(
            "✅ 日志集成测试通过: {} 条记录, {} 个错误",
            records.len(),
            errors.len()
        );
    });
}

#[cfg(all(feature = "exporter-csv", feature = "exporter-json"))]
#[test]
fn test_multiple_exporters() {
    use sqllog_analysis::exporter::SyncExporter;
    use sqllog_analysis::exporter::sync_impl::{
        csv::SyncCsvExporter, json::SyncJsonExporter,
    };
    use sqllog_analysis::sqllog::SyncSqllogParser;
    use std::fs;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let input_file = common::create_test_sqllog(
        &temp_dir,
        "multi_exporter_input.log",
        common::SAMPLE_SQLLOG_CONTENT,
    );
    let csv_file = temp_dir.path().join("output.csv");
    let json_file = temp_dir.path().join("output.json");

    // 解析记录
    let mut records = Vec::new();
    let result = SyncSqllogParser::parse_with_hooks(
        &input_file,
        1000,
        |batch_records, _errors| {
            records.extend_from_slice(batch_records);
        },
    );
    assert!(result.is_ok(), "解析应该成功");
    assert!(!records.is_empty(), "应该有记录");

    // CSV 导出
    let mut csv_exporter =
        SyncCsvExporter::new(&csv_file).expect("CSV 导出器创建应该成功");
    let csv_result = csv_exporter.export_batch(&records);
    assert!(csv_result.is_ok(), "CSV 导出应该成功");
    csv_exporter.finalize().expect("CSV finalize should succeed");

    // JSON 导出
    let mut json_exporter =
        SyncJsonExporter::new(&json_file).expect("JSON 导出器创建应该成功");
    let json_result = json_exporter.export_batch(&records);
    assert!(json_result.is_ok(), "JSON 导出应该成功");
    json_exporter.finalize().expect("JSON finalize should succeed");

    // 验证文件
    assert!(common::verify_output_file_exists(&csv_file), "CSV 文件应该存在");
    assert!(common::verify_output_file_exists(&json_file), "JSON 文件应该存在");

    let csv_content = fs::read_to_string(&csv_file).expect("读取 CSV 应该成功");
    let _json_content =
        fs::read_to_string(&json_file).expect("读取 JSON 应该成功");

    assert!(csv_content.contains("occurrence_time"), "CSV 应该包含表头");

    println!(
        "✅ 多导出器集成测试通过: {} 条记录同时导出到 CSV 和 JSON",
        records.len()
    );
}

#[cfg(all(feature = "exporter-sqlite", feature = "exporter-csv"))]
#[test]
fn test_database_and_file_export() {
    use rusqlite::Connection;
    use sqllog_analysis::exporter::SyncExporter;
    use sqllog_analysis::exporter::sync_impl::{
        csv::SyncCsvExporter, sqlite::SyncSqliteExporter,
    };

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let input_files = common::create_multiple_test_files(&temp_dir, 5);
    let sqlite_file = temp_dir.path().join("data.db");
    let csv_file = temp_dir.path().join("data.csv");

    // 使用并发解析器
    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);
    let (records, _) =
        parser.parse_files_concurrent(&input_files).expect("解析应该成功");

    // 同时导出到数据库和文件
    let mut sqlite_exporter = SyncSqliteExporter::new(&sqlite_file)
        .expect("SQLite 导出器创建应该成功");
    let mut csv_exporter =
        SyncCsvExporter::new(&csv_file).expect("CSV 导出器创建应该成功");

    let sqlite_result = sqlite_exporter.export_batch(&records);
    let csv_result = csv_exporter.export_batch(&records);

    assert!(sqlite_result.is_ok(), "SQLite 导出应该成功");
    assert!(csv_result.is_ok(), "CSV 导出应该成功");

    // Finalize exporters
    sqlite_exporter.finalize().expect("SQLite finalize should succeed");
    csv_exporter.finalize().expect("CSV finalize should succeed");

    // Drop exporters to release connections
    drop(sqlite_exporter);
    drop(csv_exporter);

    // 验证 SQLite 数据
    let conn = Connection::open(&sqlite_file).expect("打开数据库应该成功");
    let mut stmt =
        conn.prepare("SELECT COUNT(*) FROM sqllogs").expect("查询应该成功");
    let count: i64 =
        stmt.query_row([], |row| row.get(0)).expect("获取计数应该成功");

    assert_eq!(count as usize, records.len(), "数据库记录数应该匹配");

    // 验证 CSV 数据
    assert!(common::verify_output_file_exists(&csv_file), "CSV 文件应该存在");

    println!("✅ 数据库+文件导出集成测试通过: {} 条记录", records.len());
}

#[cfg(all(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
#[test]
fn test_all_sync_exporters() {
    use duckdb::Connection as DuckdbConnection;
    use rusqlite::Connection;
    use sqllog_analysis::exporter::SyncExporter;
    use sqllog_analysis::exporter::sync_impl::{
        csv::SyncCsvExporter, duckdb::SyncDuckdbExporter,
        json::SyncJsonExporter, sqlite::SyncSqliteExporter,
    };
    use sqllog_analysis::sqllog::SyncSqllogParser;
    use std::fs;

    println!("Testing all sync exporters integration...");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let input_file = common::create_test_sqllog(
        &temp_dir,
        "all_exporters_input.log",
        common::SAMPLE_SQLLOG_CONTENT,
    );

    // 解析记录
    let mut records = Vec::new();
    let result = SyncSqllogParser::parse_with_hooks(
        &input_file,
        1000,
        |batch_records, _errors| {
            records.extend_from_slice(batch_records);
        },
    );
    assert!(result.is_ok(), "解析应该成功");
    assert!(!records.is_empty(), "应该有记录");

    // 准备导出文件
    let csv_file = temp_dir.path().join("all_sync.csv");
    let json_file = temp_dir.path().join("all_sync.json");
    let sqlite_file = temp_dir.path().join("all_sync.db");
    let duckdb_file = temp_dir.path().join("all_sync.duckdb");

    // 创建所有导出器
    let mut csv_exporter =
        SyncCsvExporter::new(&csv_file).expect("CSV 导出器创建应该成功");
    let mut json_exporter =
        SyncJsonExporter::new(&json_file).expect("JSON 导出器创建应该成功");
    let mut sqlite_exporter = SyncSqliteExporter::new(&sqlite_file)
        .expect("SQLite 导出器创建应该成功");
    let mut duckdb_exporter = SyncDuckdbExporter::new(&duckdb_file)
        .expect("DuckDB 导出器创建应该成功");

    // 同时导出到所有格式
    let csv_result = csv_exporter.export_batch(&records);
    let json_result = json_exporter.export_batch(&records);
    let sqlite_result = sqlite_exporter.export_batch(&records);
    let duckdb_result = duckdb_exporter.export_batch(&records);

    assert!(csv_result.is_ok(), "CSV 导出应该成功");
    assert!(json_result.is_ok(), "JSON 导出应该成功");
    assert!(sqlite_result.is_ok(), "SQLite 导出应该成功");
    assert!(duckdb_result.is_ok(), "DuckDB 导出应该成功");

    // Finalize all exporters
    csv_exporter.finalize().expect("CSV finalize should succeed");
    json_exporter.finalize().expect("JSON finalize should succeed");
    sqlite_exporter.finalize().expect("SQLite finalize should succeed");
    duckdb_exporter.finalize().expect("DuckDB finalize should succeed");

    // Drop exporters to release connections
    drop(csv_exporter);
    drop(json_exporter);
    drop(sqlite_exporter);
    drop(duckdb_exporter);

    // 验证所有文件
    assert!(csv_file.exists(), "CSV 文件应该存在");
    assert!(json_file.exists(), "JSON 文件应该存在");
    assert!(sqlite_file.exists(), "SQLite 文件应该存在");
    assert!(duckdb_file.exists(), "DuckDB 文件应该存在");

    // 验证 CSV 内容
    let csv_content = fs::read_to_string(&csv_file).expect("读取 CSV 应该成功");
    assert!(csv_content.contains("occurrence_time"), "CSV 应该包含表头");

    // 验证 SQLite 内容
    let sqlite_conn =
        Connection::open(&sqlite_file).expect("打开 SQLite 应该成功");
    let mut stmt = sqlite_conn
        .prepare("SELECT COUNT(*) FROM sqllogs")
        .expect("SQLite 查询应该成功");
    let sqlite_count: i64 =
        stmt.query_row([], |row| row.get(0)).expect("获取 SQLite 计数应该成功");
    assert_eq!(sqlite_count as usize, records.len(), "SQLite 记录数应该匹配");

    // 验证 DuckDB 内容
    let duckdb_conn =
        DuckdbConnection::open(&duckdb_file).expect("打开 DuckDB 应该成功");
    let mut stmt = duckdb_conn
        .prepare("SELECT COUNT(*) FROM sqllogs")
        .expect("DuckDB 查询应该成功");
    let duckdb_count: i64 =
        stmt.query_row([], |row| row.get(0)).expect("获取 DuckDB 计数应该成功");
    assert_eq!(duckdb_count as usize, records.len(), "DuckDB 记录数应该匹配");

    println!(
        "✅ 所有同步导出器集成测试通过: {} 条记录导出到 CSV, JSON, SQLite, DuckDB",
        records.len()
    );
}

#[cfg(all(feature = "exporter-json", feature = "exporter-sqlite"))]
#[test]
fn test_sync_multi_exporter_stats() {
    use rusqlite::Connection;
    use sqllog_analysis::exporter::SyncExporter;
    use sqllog_analysis::exporter::sync_impl::{
        json::SyncJsonExporter, sqlite::SyncSqliteExporter,
    };
    use sqllog_analysis::sqllog::SyncSqllogParser;
    use std::fs;

    println!("Testing sync multi-exporter statistics...");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let input_files = common::create_multiple_test_files(&temp_dir, 3);

    // 使用并发解析器获取更多记录
    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);
    let (records, _) =
        parser.parse_files_concurrent(&input_files).expect("解析应该成功");

    let json_file = temp_dir.path().join("stats.json");
    let sqlite_file = temp_dir.path().join("stats.db");

    // 创建导出器
    let mut json_exporter =
        SyncJsonExporter::new(&json_file).expect("JSON 导出器创建应该成功");
    let mut sqlite_exporter = SyncSqliteExporter::new(&sqlite_file)
        .expect("SQLite 导出器创建应该成功");

    // 分批导出以测试统计信息
    let batch_size = 2;
    for chunk in records.chunks(batch_size) {
        json_exporter.export_batch(chunk).expect("JSON 批次导出应该成功");
        sqlite_exporter.export_batch(chunk).expect("SQLite 批次导出应该成功");
    }

    // 检查统计信息
    let json_stats = json_exporter.get_stats();
    let sqlite_stats = sqlite_exporter.get_stats();

    assert_eq!(
        json_stats.exported_records,
        records.len(),
        "JSON 统计记录数应该匹配"
    );
    assert_eq!(
        sqlite_stats.exported_records,
        records.len(),
        "SQLite 统计记录数应该匹配"
    );

    // Finalize
    json_exporter.finalize().expect("JSON finalize should succeed");
    sqlite_exporter.finalize().expect("SQLite finalize should succeed");

    // Drop to release connections
    drop(json_exporter);
    drop(sqlite_exporter);

    // 验证最终数据
    let sqlite_conn =
        Connection::open(&sqlite_file).expect("打开 SQLite 应该成功");
    let mut stmt = sqlite_conn
        .prepare("SELECT COUNT(*) FROM sqllogs")
        .expect("SQLite 查询应该成功");
    let sqlite_count: i64 =
        stmt.query_row([], |row| row.get(0)).expect("获取 SQLite 计数应该成功");
    assert_eq!(sqlite_count as usize, records.len(), "SQLite 记录数应该匹配");

    println!(
        "✅ 同步多导出器统计测试通过: {} 条记录, JSON统计: {}, SQLite统计: {}",
        records.len(),
        json_stats.exported_records,
        sqlite_stats.exported_records
    );
}
