use sqllog_analysis::sqllog::*;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_from_line_invalid_format() {
    let res = Sqllog::from_line("2025-10-10", 1);
    assert!(res.is_err());
    let res = Sqllog::from_line(
        "xxxx-xx-xx xx:xx:xx.xxx (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL)",
        2,
    );
    assert!(res.is_err());
}

#[test]
fn test_from_line_regex_error() {
    let line = "2025-10-10 10:10:10.100 something not match";
    let res = Sqllog::from_line(line, 1);
    assert!(res.is_err());
}

#[test]
fn test_from_line_null_fields() {
    let line = "2025-10-10 10:10:10.100 (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [SEL]: SELECT";
    let res = Sqllog::from_line(line, 1).unwrap();
    let log = res.unwrap();
    assert_eq!(log.session, None);
    assert_eq!(log.thread, None);
    assert_eq!(log.user, None);
    assert_eq!(log.trx_id, None);
    assert_eq!(log.statement, None);
}

#[test]
fn test_from_line_desc_parse_error() {
    let line = "2025-10-10 10:10:10.100 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd) [SEL]: desc without numbers";
    let res = Sqllog::from_line(line, 1).unwrap();
    let log = res.unwrap();
    assert_eq!(log.execute_time, None);
    assert_eq!(log.rowcount, None);
    assert_eq!(log.execute_id, None);
}

#[test]
fn test_from_file_empty_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("empty.log");
    File::create(&file_path).unwrap();
    let logs = Sqllog::from_file(&file_path).unwrap();
    assert_eq!(logs.len(), 0);
}

#[test]
fn test_from_file_only_invalid_lines() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("invalid.log");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "invalid line").unwrap();
    writeln!(file, "another bad line").unwrap();
    let logs = Sqllog::from_file(&file_path).unwrap();
    assert_eq!(logs.len(), 0);
}

#[test]
fn test_from_file_invalid_utf8() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("invalid_utf8.log");
    let mut file = File::create(&file_path).unwrap();
    file.write_all(&[0xff, 0xfe, 0xfd]).unwrap();
    let res = Sqllog::from_file(&file_path);
    assert!(res.is_err());
}

#[test]
fn test_display_method() {
    let log = Sqllog {
        occurrence_time: "2025-10-10 10:10:10.100".to_string(),
        ep: 1,
        session: Some("0x1234".to_string()),
        thread: Some("1234".to_string()),
        user: Some("SYSDBA".to_string()),
        trx_id: Some("5678".to_string()),
        statement: Some("0xabcd".to_string()),
        appname: Some("TEST".to_string()),
        ip: Some("192.168.1.1".to_string()),
        sql_type: Some("SEL".to_string()),
        description: "SELECT * FROM test;".to_string(),
        execute_time: Some(100),
        rowcount: Some(10),
        execute_id: Some(1),
    };
    log.display();
}

#[test]
fn test_sqllog_parsing() {
    let test_log = r#"2025-10-10 10:10:10.100 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd appname:TEST ip:::ffff:192.168.1.1) [SEL]: SELECT * FROM test;
2025-10-10 10:10:11.200 (EP[2] sess:0x5678 thrd:NULL user:USER1 trxid:NULL stmt:0xef12) EXECTIME: 100(ms) ROWCOUNT: 10 EXEC_ID: 1."#;
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.log");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "{}", test_log).unwrap();
    let logs = Sqllog::from_file(&file_path).unwrap();
    assert_eq!(logs.len(), 2);
    let log1 = &logs[0];
    assert_eq!(log1.occurrence_time, "2025-10-10 10:10:10.100");
    assert_eq!(log1.ep, 1);
    assert_eq!(log1.session, Some("0x1234".to_string()));
    assert_eq!(log1.thread, Some("1234".to_string()));
    assert_eq!(log1.user, Some("SYSDBA".to_string()));
    assert_eq!(log1.trx_id, Some("5678".to_string()));
    assert_eq!(log1.statement, Some("0xabcd".to_string()));
    assert_eq!(log1.appname, Some("TEST".to_string()));
    assert_eq!(log1.ip, Some("192.168.1.1".to_string()));
    assert_eq!(log1.sql_type, Some("SEL".to_string()));
    assert_eq!(log1.description, "SELECT * FROM test;");
    assert_eq!(log1.execute_time, None);
    assert_eq!(log1.rowcount, None);
    assert_eq!(log1.execute_id, None);
    let log2 = &logs[1];
    assert_eq!(log2.occurrence_time, "2025-10-10 10:10:11.200");
    assert_eq!(log2.ep, 2);
    assert_eq!(log2.session, Some("0x5678".to_string()));
    assert_eq!(log2.thread, None);
    assert_eq!(log2.user, Some("USER1".to_string()));
    assert_eq!(log2.trx_id, None);
    assert_eq!(log2.statement, Some("0xef12".to_string()));
    assert_eq!(log2.appname, None);
    assert_eq!(log2.ip, None);
    assert_eq!(log2.sql_type, None);
    assert_eq!(
        log2.description,
        "EXECTIME: 100(ms) ROWCOUNT: 10 EXEC_ID: 1."
    );
    assert_eq!(log2.execute_time, Some(100));
    assert_eq!(log2.rowcount, Some(10));
    assert_eq!(log2.execute_id, Some(1));
}

#[test]
fn test_is_first_row() {
    assert!(is_first_row("2025-10-10 10:10:10.100"));
    assert!(is_first_row("2025-12-31 23:59:59.999"));
    assert!(is_first_row("2025-01-01 00:00:00.000"));
    assert!(is_first_row("2025-01-31 00:00:00.000"));
    assert!(is_first_row("2025-04-30 00:00:00.000"));
    assert!(!is_first_row("2025-04-31 00:00:00.000"));
    assert!(!is_first_row("2025-02-29 00:00:00.000"));
    assert!(is_first_row("2024-02-29 00:00:00.000"));
    assert!(!is_first_row("2024-02-30 00:00:00.000"));
    assert!(!is_first_row("0000-01-01 00:00:00.000"));
    assert!(!is_first_row("2025-00-01 00:00:00.000"));
    assert!(!is_first_row("2025-13-10 10:10:10.100"));
    assert!(!is_first_row("2025-10-00 10:10:10.100"));
    assert!(!is_first_row("2025-10-32 10:10:10.100"));
    assert!(!is_first_row("2025-10-10 24:10:10.100"));
    assert!(!is_first_row("2025-10-10 10:60:10.100"));
    assert!(!is_first_row("2025-10-10 10:10:60.100"));
    assert!(!is_first_row("2025-10-10 10:10:10.1000"));
    assert!(!is_first_row("2025-10-1010:10:10.100"));
    assert!(!is_first_row("2025/10/10 10:10:10.100"));
    assert!(!is_first_row(""));
    assert!(!is_first_row("2024-6-12 12:34:56.789"));
    assert!(!is_first_row("Invalid line"));
}

#[test]
fn test_multiline_description() {
    let test_log = r#"2025-10-10 10:10:10.100 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd) [SEL]: 第一行描述
        第二行内容
        第三行内容
        2025-10-10 10:10:11.200 (EP[2] sess:0x5678 thrd:NULL user:USER1 trxid:NULL stmt:0xef12) EXECTIME: 100(ms) ROWCOUNT: 10 EXEC_ID: 1."#;
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("test_multiline.log");
    let mut file = std::fs::File::create(&file_path).unwrap();
    writeln!(file, "{}", test_log).unwrap();
    let logs = Sqllog::from_file(&file_path).unwrap();
    assert_eq!(logs.len(), 2);
    let log1 = &logs[0];
    assert!(log1.description.contains("第一行描述"));
    assert!(log1.description.contains("第二行内容"));
    assert!(log1.description.contains("第三行内容"));
    assert!(log1.description.starts_with("第一行描述"));
    assert!(log1.description.contains('\n'));
}

#[test]
fn test_other_error_display() {
    let err = SqllogError::Other("自定义错误".to_string());
    assert_eq!(format!("{}", err), "未知错误: 自定义错误");
}

#[test]
fn test_from_file_io_error() {
    let res = Sqllog::from_file("not_exist_file.log");
    match res {
        Err(SqllogError::Io(_)) => (),
        _ => panic!("应为IO错误"),
    }
}

#[test]
fn test_display_all_none() {
    let log = Sqllog {
        occurrence_time: "2025-10-10 10:10:10.100".to_string(),
        ep: 1,
        session: None,
        thread: None,
        user: None,
        trx_id: None,
        statement: None,
        appname: None,
        ip: None,
        sql_type: None,
        description: "".to_string(),
        execute_time: None,
        rowcount: None,
        execute_id: None,
    };
    log.display();
}
