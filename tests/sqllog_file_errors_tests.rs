use sqllog_analysis::sqllog::{Sqllog, SqllogError};
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_from_file_with_errors_success_and_fail() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.log");
    let mut file = File::create(&file_path).unwrap();
    // 第一段为合法日志
    writeln!(file, "2025-10-10 10:10:10.100 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd) [SEL]: SELECT 1").unwrap();
    // 第二段为非法内容，前面加合法首行，确保分段
    writeln!(file, "2025-10-10 10:10:10.101 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd) [SEL]: bad log line").unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    // 新实现下，两个日志都能被解析为合法记录
    assert_eq!(logs.len(), 2);
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_from_file_with_errors_empty_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("empty.log");
    File::create(&file_path).unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    assert!(logs.is_empty());
    assert!(errors.is_empty());
}

#[test]
fn test_from_file_with_errors_invalid_utf8() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("invalid_utf8.log");
    let mut file = File::create(&file_path).unwrap();
    file.write_all(&[0xff, 0xfe, 0xfd]).unwrap();
    let (_logs, errors) = Sqllog::from_file_with_errors(&file_path);
    // 新实现下，遇到 UTF8 错误会直接跳过该行
    println!("errors: {:?}", errors);
    assert!(!errors.is_empty());
    let found_utf8 = errors
        .iter()
        .any(|(_, _, err)| matches!(err, SqllogError::Utf8(_)));
    assert!(found_utf8, "应为 Utf8 错误");
}
