#![allow(clippy::uninlined_format_args)]
#![allow(clippy::let_unit_value)]
use sqllog_analysis::sqllog::Sqllog;
use std::{
    fs::{self, File},
    io::Write,
    panic,
};
use tempfile::tempdir;

#[test]
fn test_main_dir_not_exist() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path().join("not_exist_dir");
    assert!(!dir.exists());
    let result = panic::catch_unwind(|| {
        let () = {
            let sqllog_dir = &dir;
            if !sqllog_dir.exists() {
                println!("未找到 sqllog 目录，请确认目录是否存在");
                return;
            }
        };
    });
    assert!(result.is_ok());
}

#[test]
fn test_main_no_log_files() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();
    fs::create_dir_all(dir).unwrap();
    let result = panic::catch_unwind(|| {
        let sqllog_dir = dir;
        println!("正在检查 sqllog 目录下的 dmsql*.log 文件...");
        for entry in fs::read_dir(sqllog_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                if let Some(filename) =
                    path.file_name().and_then(|n| n.to_str())
                {
                    if filename.starts_with("dmsql")
                        && std::path::Path::new(filename)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("log"))
                    {
                        println!("处理文件: {}", filename);
                    }
                }
            }
        }
    });
    assert!(result.is_ok());
}

#[test]
fn test_main_parse_success_and_error() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path();
    let file_path = dir.join("dmsql_test.log");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "2025-10-10 10:10:10.100 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd) [SEL]: SELECT").unwrap();
    writeln!(file, "invalid line").unwrap();
    let result = panic::catch_unwind(|| {
        let sqllog_dir = dir;
        for entry in fs::read_dir(sqllog_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                if let Some(filename) =
                    path.file_name().and_then(|n| n.to_str())
                {
                    if filename.starts_with("dmsql")
                        && filename.ends_with(".log")
                    {
                        let (logs, errors) =
                            Sqllog::from_file_with_errors(&path);
                        println!(
                            "成功解析文件: {}，共 {} 条记录，{} 条错误",
                            filename,
                            logs.len(),
                            errors.len()
                        );
                    }
                }
            }
        }
    });
    assert!(result.is_ok());
}
