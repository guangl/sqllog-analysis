use fs::OpenOptions;
use sqllog_analysis::process::{process_sqllog_dir, write_error_files};
use std::{
    env,
    fs::{self, File},
    io::Write,
};
use tempfile::tempdir;

#[test]
fn test_process_sqllog_dir_empty() {
    let dir = tempdir().unwrap();
    let (total_files, total_logs, error_files, _elapsed) = process_sqllog_dir(dir.path()).unwrap();
    assert_eq!(total_files, 0);
    assert_eq!(total_logs, 0);
    assert!(error_files.is_empty());
}

#[test]
fn test_process_sqllog_dir_with_error_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("dmsql_test.log");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "bad line").unwrap();
    let (total_files, total_logs, error_files, _elapsed) = process_sqllog_dir(dir.path()).unwrap();
    assert_eq!(total_files, 1);
    assert_eq!(total_logs, 0);
    assert!(!error_files.is_empty());
    assert!(error_files[0].0.contains("dmsql_test.log"));
}

#[test]
fn test_process_sqllog_dir_non_utf8_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("dmsql_nonutf8.log");
    let mut file = File::create(&file_path).unwrap();
    file.write_all(&[0xff, 0xfe, 0xfd]).unwrap();
    let (total_files, total_logs, error_files, _elapsed) = process_sqllog_dir(dir.path()).unwrap();
    assert_eq!(total_files, 1);
    assert_eq!(total_logs, 0);
    assert!(!error_files.is_empty());
    assert!(error_files[0].0.contains("dmsql_nonutf8.log"));
}

#[test]
fn test_process_sqllog_dir_no_dmsql_files() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("other.log");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "test").unwrap();
    let (total_files, total_logs, error_files, _elapsed) = process_sqllog_dir(dir.path()).unwrap();
    assert_eq!(total_files, 0);
    assert_eq!(total_logs, 0);
    assert!(error_files.is_empty());
}

#[test]
fn test_write_error_files_non_empty() {
    let errors = vec![
        (
            "file1.log".to_string(),
            "行1: 错误内容\n内容: bad".to_string(),
        ),
        (
            "file2.log".to_string(),
            "行2: 错误内容\n内容: bad2".to_string(),
        ),
    ];
    let result = write_error_files(&errors);
    assert!(result.is_ok());
    let content = fs::read_to_string("error_files.txt").unwrap();
    assert!(content.contains("file1.log"));
    assert!(content.contains("file2.log"));
}

#[test]
fn test_write_error_files_empty() {
    let errors: Vec<(String, String)> = vec![];
    let result = write_error_files(&errors);
    assert!(result.is_ok());
}

#[test]
fn test_write_error_files_io_error() {
    // 模拟无法写入 error_files.txt（只读文件权限）
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("error_files.txt");
    let _file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&file_path)
        .unwrap();
    // 设置只读权限（跨平台）
    let mut perms = fs::metadata(&file_path).unwrap().permissions();
    perms.set_readonly(true);
    fs::set_permissions(&file_path, perms).unwrap();
    let errors = vec![(
        "file1.log".to_string(),
        "行1: 错误内容\n内容: bad".to_string(),
    )];
    // 将当前目录切换到临时目录
    let old_dir = env::current_dir().unwrap();
    env::set_current_dir(dir.path()).unwrap();
    let result = write_error_files(&errors);
    // 由于 error_files.txt 只读，写入应报错
    assert!(result.is_err());
    // 切回原目录
    env::set_current_dir(old_dir).unwrap();
}

#[test]
fn test_process_sqllog_dir_mixed_files() {
    let dir = tempdir().unwrap();
    // 合法日志
    let file_valid = dir.path().join("dmsql_valid.log");
    let mut f_valid = File::create(&file_valid).unwrap();
    writeln!(
        f_valid,
        "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) test"
    )
    .unwrap();
    // 非法日志
    let file_invalid = dir.path().join("dmsql_invalid.log");
    let mut f_invalid = File::create(&file_invalid).unwrap();
    writeln!(f_invalid, "bad line").unwrap();
    // 空日志
    let file_empty = dir.path().join("dmsql_empty.log");
    File::create(&file_empty).unwrap();
    // 非UTF8日志
    let file_nonutf8 = dir.path().join("dmsql_nonutf8.log");
    let mut f_nonutf8 = File::create(&file_nonutf8).unwrap();
    f_nonutf8.write_all(&[0xff, 0xfe, 0xfd]).unwrap();
    // 非目标文件
    let file_other = dir.path().join("other.log");
    let mut f_other = File::create(&file_other).unwrap();
    writeln!(f_other, "just for test").unwrap();
    // 执行综合处理
    let (total_files, total_logs, error_files, _elapsed) = process_sqllog_dir(dir.path()).unwrap();
    // 只统计 dmsql*.log 文件
    assert_eq!(total_files, 4);
    // 合法日志能被解析
    assert!(total_logs >= 1);
    // 非法/非UTF8日志会产生错误
    assert!(
        error_files
            .iter()
            .any(|(fname, _)| fname.contains("dmsql_invalid.log"))
    );
    assert!(
        error_files
            .iter()
            .any(|(fname, _)| fname.contains("dmsql_nonutf8.log"))
    );
    // 空日志不产生错误
    assert!(
        !error_files
            .iter()
            .any(|(fname, _)| fname.contains("dmsql_empty.log"))
    );
    // 非目标文件不统计
    assert!(
        !error_files
            .iter()
            .any(|(fname, _)| fname.contains("other.log"))
    );
}
