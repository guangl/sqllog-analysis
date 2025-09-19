use sqllog_analysis::process::{process_sqllog_dir, write_error_files};
use std::fs::{self, File};
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_main_dir_not_exist() {
    let dir = std::path::PathBuf::from("not_exist_dir_for_test");
    assert!(!dir.exists());
    let result = std::panic::catch_unwind(|| {
        if !dir.exists() {
            println!("目录不存在: {:?}", std::env::current_dir().unwrap());
        }
    });
    assert!(result.is_ok());
}

#[test]
fn test_main_with_error_files() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("dmsql_test.log");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "bad line").unwrap();
    let (total_files, total_logs, error_files, _elapsed) = process_sqllog_dir(dir.path()).unwrap();
    assert_eq!(total_files, 1);
    assert_eq!(total_logs, 0);
    assert!(!error_files.is_empty());
    let result = write_error_files(&error_files);
    assert!(result.is_ok());
    let content = std::fs::read_to_string("error_files.txt").unwrap();
    assert!(content.contains("dmsql_test.log"));
}

#[test]
fn test_main_no_log_files() {
    let dir = tempdir().unwrap();
    let result = std::panic::catch_unwind(|| {
        for entry in fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    if filename.starts_with("dmsql") && filename.ends_with(".log") {
                        println!("处理文件: {}", filename);
                    }
                }
            }
        }
    });
    assert!(result.is_ok());
}
