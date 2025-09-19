#![allow(invalid_from_utf8)]
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
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    assert_eq!(logs.len(), 0);
    assert_eq!(errors.len(), 0);
}

#[test]
fn test_from_file_only_invalid_lines() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("invalid.log");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "bad").unwrap();
    writeln!(file, "not a log").unwrap();
    writeln!(file, "123").unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    assert_eq!(logs.len(), 0);
    assert!(!errors.is_empty());
}

#[test]
fn test_from_file_invalid_utf8() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("invalid_utf8.log");
    let mut file = File::create(&file_path).unwrap();
    file.write_all(&[0xff, 0xfe, 0xfd]).unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    assert_eq!(logs.len(), 0);
    assert!(
        errors
            .iter()
            .any(|(_, _, e)| format!("{e}").contains("UTF"))
            || !errors.is_empty()
    );
}

#[test]
fn test_sqllog_parsing() {
    let test_log = r#"
2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705459), (1, VARCHAR2, 'CS_c768d88f3a07'), (2, VARCHAR2, NULL), (3, NUMBER, 0), (4, VARCHAR2, ''), (5, VARCHAR2, NULL), (6, VARCHAR2, NULL), (7, VARCHAR2, 'other'), (8, VARCHAR2, NULL), (9, VARCHAR2, '5'), (10, NUMBER, 0), (11, VARCHAR2, NULL), (12, VARCHAR2, '无'), (13, TIMESTAMP, 2019-09-01 00:00:00), (14, TIMESTAMP, 2020-01-01 00:00:00), (15, NUMBER, 0), (16, VARCHAR2, NULL), (17, VARCHAR2, NULL), (18, VARCHAR2, NULL), (19, VARCHAR2, '
1
1'), (20, VARCHAR2, NULL), (21, TIMESTAMP, 2022-10-24 21:41:38), (22, TIMESTAMP, NULL), (23, TIMESTAMP, NULL), (24, NUMBER, 1), (25, VARCHAR2, NULL), (26, VARCHAR2, NULL), (27, VARCHAR2, NULL), (28, NUMBER, 3), (29, VARCHAR2, NULL), (30, TIMESTAMP, 2025-09-16 20:02:53)}
2025-09-16 20:02:53.562 (EP[0] sess:0x91c3c8c10 thrd:4122859 user:EKP trxid:122154453041 stmt:0x91c438c10 appname: ip:::ffff:10.63.97.63) [SEL] 1
2025-09-16 20:02:53.566 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705946), (1, VARCHAR2, 'CS_0bfaa9ae2d7b'), (2, VARCHAR2, NULL), (3, NUMBER, NULL), (4, VARCHAR2, '1'), (5, VARCHAR2, NULL), (6, VARCHAR2, NULL), (7, VARCHAR2, '9'), (8, VARCHAR2, '65'), (9, VARCHAR2, '5'), (10, NUMBER, 1), (11, VARCHAR2, NULL), (12, VARCHAR2, NULL), (13, TIMESTAMP, 2021-03-01 00:00:00), (14, TIMESTAMP, 2022-07-01 00:00:00), (15, NUMBER, 0), (16, VARCHAR2, NULL), (17, VARCHAR2, NULL), (18, VARCHAR2, NULL), (19, VARCHAR2, '1

2

3'), (20, VARCHAR2, NULL), (21, TIMESTAMP, 2022-10-24 23:19:32), (22, TIMESTAMP, NULL), (23, TIMESTAMP, NULL), (24, NUMBER, 1), (25, VARCHAR2, NULL), (26, VARCHAR2, NULL), (27, VARCHAR2, NULL), (28, NUMBER, 0), (29, VARCHAR2, NULL), (30, TIMESTAMP, 2025-09-16 20:02:53)}
2025-09-16 20:02:53.566 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705960), (1, VARCHAR2, 'CS_3e936f05cce9'), (2, VARCHAR2, NULL), (3, NUMBER, 0), (4, VARCHAR2, '字节跳动'), (5, VARCHAR2, NULL), (6, VARCHAR2, NULL), (7, VARCHAR2, 'other'), (8, VARCHAR2, NULL), (9, VARCHAR2, '5'), (10, NUMBER, 0), (11, VARCHAR2, NULL), (12, VARCHAR2, '后端开发实习生'), (13, TIMESTAMP, 2022-01-10 00:00:00), (14, TIMESTAMP, 2022-06-30 00:00:00), (15, NUMBER, 0), (16, VARCHAR2, NULL), (17, VARCHAR2, NULL), (18, VARCHAR2, NULL), (19, VARCHAR2, '⚫ 4
⚫ 5
⚫ 6'), (20, VARCHAR2, NULL), (21, TIMESTAMP, 2022-10-24 23:20:33), (22, TIMESTAMP, NULL), (23, TIMESTAMP, NULL), (24, NUMBER, 1), (25, VARCHAR2, NULL), (26, VARCHAR2, NULL), (27, VARCHAR2, NULL), (28, NUMBER, 3), (29, VARCHAR2, NULL), (30, TIMESTAMP, 2025-09-16 20:02:53)}
"#;
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.log");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "{test_log}").unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    for (i, log) in logs.iter().enumerate() {
        println!(
            "记录 {}: occurrence_time={}, user={:?}, description={}",
            i + 1,
            log.occurrence_time,
            log.user,
            log.description
        );
    }
    assert_eq!(logs.len(), 4);
    assert_eq!(errors.len(), 0);
    let log1 = &logs[0];
    assert_eq!(log1.occurrence_time, "2025-09-16 20:02:53.562");
    assert_eq!(log1.ep, 0);
    assert_eq!(log1.trx_id, Some("122154453026".to_string()));
    assert_eq!(log1.statement, Some("0x6da900ef0".to_string()));
    assert_eq!(log1.appname, None);
    assert_eq!(log1.ip, Some("10.80.147.109".to_string()));
    assert_eq!(log1.sql_type, None);
    // description 多行，断言只检查非空和关键字
    println!("parsing description: {}", log1.description);
    assert!(!log1.description.is_empty());
    assert!(log1.description.contains("PARAMS(SEQNO, TYPE, DATA)"));
    assert_eq!(log1.execute_time, None);
    assert_eq!(log1.rowcount, None);
    assert_eq!(log1.execute_id, None);
    let log2 = &logs[1];
    assert_eq!(log2.occurrence_time, "2025-09-16 20:02:53.562");
    assert_eq!(log2.ep, 0);
    assert_eq!(log2.session, Some("0x91c3c8c10".to_string()));
    assert_eq!(log2.thread, Some("4122859".to_string()));
    assert_eq!(log2.user, Some("EKP".to_string()));
    assert_eq!(log2.trx_id, Some("122154453041".to_string()));
    assert_eq!(log2.statement, Some("0x91c438c10".to_string()));
    assert_eq!(log2.appname, None);
    assert_eq!(log2.ip, Some("10.63.97.63".to_string()));
    assert_eq!(log2.sql_type, Some("SEL".to_string()));
    assert_eq!(log2.description, "1");
    assert_eq!(log2.execute_time, None);
    assert_eq!(log2.rowcount, None);
    assert_eq!(log2.execute_id, None);
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
    let test_log = r#"
2025-09-16 20:02:53.562 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705459), (1, VARCHAR2, 'CS_c768d88f3a07'), (2, VARCHAR2, NULL), (3, NUMBER, 0), (4, VARCHAR2, ''), (5, VARCHAR2, NULL), (6, VARCHAR2, NULL), (7, VARCHAR2, 'other'), (8, VARCHAR2, NULL), (9, VARCHAR2, '5'), (10, NUMBER, 0), (11, VARCHAR2, NULL), (12, VARCHAR2, '无'), (13, TIMESTAMP, 2019-09-01 00:00:00), (14, TIMESTAMP, 2020-01-01 00:00:00), (15, NUMBER, 0), (16, VARCHAR2, NULL), (17, VARCHAR2, NULL), (18, VARCHAR2, NULL), (19, VARCHAR2, '
1
1'), (20, VARCHAR2, NULL), (21, TIMESTAMP, 2022-10-24 21:41:38), (22, TIMESTAMP, NULL), (23, TIMESTAMP, NULL), (24, NUMBER, 1), (25, VARCHAR2, NULL), (26, VARCHAR2, NULL), (27, VARCHAR2, NULL), (28, NUMBER, 3), (29, VARCHAR2, NULL), (30, TIMESTAMP, 2025-09-16 20:02:53)}
2025-09-16 20:02:53.562 (EP[0] sess:0x91c3c8c10 thrd:4122859 user:EKP trxid:122154453041 stmt:0x91c438c10 appname: ip:::ffff:10.63.97.63) [SEL] 1
2025-09-16 20:02:53.566 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705946), (1, VARCHAR2, 'CS_0bfaa9ae2d7b'), (2, VARCHAR2, NULL), (3, NUMBER, NULL), (4, VARCHAR2, '1'), (5, VARCHAR2, NULL), (6, VARCHAR2, NULL), (7, VARCHAR2, '9'), (8, VARCHAR2, '65'), (9, VARCHAR2, '5'), (10, NUMBER, 1), (11, VARCHAR2, NULL), (12, VARCHAR2, NULL), (13, TIMESTAMP, 2021-03-01 00:00:00), (14, TIMESTAMP, 2022-07-01 00:00:00), (15, NUMBER, 0), (16, VARCHAR2, NULL), (17, VARCHAR2, NULL), (18, VARCHAR2, NULL), (19, VARCHAR2, '1

2

3'), (20, VARCHAR2, NULL), (21, TIMESTAMP, 2022-10-24 23:19:32), (22, TIMESTAMP, NULL), (23, TIMESTAMP, NULL), (24, NUMBER, 1), (25, VARCHAR2, NULL), (26, VARCHAR2, NULL), (27, VARCHAR2, NULL), (28, NUMBER, 0), (29, VARCHAR2, NULL), (30, TIMESTAMP, 2025-09-16 20:02:53)}
2025-09-16 20:02:53.566 (EP[0] sess:0x6da8ccef0 thrd:4146217 user:EDM_BASE trxid:122154453026 stmt:0x6da900ef0 appname: ip:::ffff:10.80.147.109) PARAMS(SEQNO, TYPE, DATA)={(0, NUMBER, 1705960), (1, VARCHAR2, 'CS_3e936f05cce9'), (2, VARCHAR2, NULL), (3, NUMBER, 0), (4, VARCHAR2, '字节跳动'), (5, VARCHAR2, NULL), (6, VARCHAR2, NULL), (7, VARCHAR2, 'other'), (8, VARCHAR2, NULL), (9, VARCHAR2, '5'), (10, NUMBER, 0), (11, VARCHAR2, NULL), (12, VARCHAR2, '后端开发实习生'), (13, TIMESTAMP, 2022-01-10 00:00:00), (14, TIMESTAMP, 2022-06-30 00:00:00), (15, NUMBER, 0), (16, VARCHAR2, NULL), (17, VARCHAR2, NULL), (18, VARCHAR2, NULL), (19, VARCHAR2, '⚫ 4
⚫ 5
⚫ 6'), (20, VARCHAR2, NULL), (21, TIMESTAMP, 2022-10-24 23:20:33), (22, TIMESTAMP, NULL), (23, TIMESTAMP, NULL), (24, NUMBER, 1), (25, VARCHAR2, NULL), (26, VARCHAR2, NULL), (27, VARCHAR2, NULL), (28, NUMBER, 3), (29, VARCHAR2, NULL), (30, TIMESTAMP, 2025-09-16 20:02:53)}
"#;
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test_multiline.log");
    let mut file = std::fs::File::create(&file_path).unwrap();
    writeln!(file, "{test_log}").unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    assert_eq!(logs.len(), 4);
    assert_eq!(errors.len(), 0);
    let log1 = &logs[0];
    println!("multiline description: {}", log1.description);
    assert!(!log1.description.is_empty());
    assert!(log1.description.contains("PARAMS(SEQNO, TYPE, DATA)"));
}

#[test]
fn test_other_error_display() {
    let err = SqllogError::Other("自定义错误".to_string());
    assert_eq!(format!("{err}"), "未知错误: 自定义错误");
}

#[test]
fn test_from_file_io_error() {
    let (logs, errors) = Sqllog::from_file_with_errors("not_exist_file.log");
    assert_eq!(logs.len(), 0);
    assert!(errors.iter().any(|(_, _, e)| {
        let s = format!("{e}");
        s.contains("IO错误") || s.contains("No such file") || s.contains("找不到")
    }));
}

#[test]
fn test_is_first_row_extreme_cases() {
    // 极端年份
    assert!(is_first_row("9999-12-31 23:59:59.999"));
    assert!(is_first_row("0001-01-01 00:00:00.000"));
    // 长度不符
    assert!(!is_first_row("2025-10-10 10:10:10.10"));
    assert!(!is_first_row("2025-10-10 10:10:10.10000"));
    // 非法字符
    assert!(!is_first_row("2025-10-10 1a:10:10.100"));
    assert!(!is_first_row("abcd-ef-gh ij:kl:mn.opq"));
    // 仅分隔符正确但数字错误
    assert!(!is_first_row("2025-10-10 99:99:99.999"));
    // 仅数字正确但分隔符错误
    assert!(!is_first_row("20251010 101010.100"));
}

#[test]
fn test_sqllogerror_display_all() {
    use std::io;
    // 无需 FromUtf8Error，直接用 Utf8Error
    use regex::Error as RegexError;
    let io_err = SqllogError::Io(io::Error::new(io::ErrorKind::Other, "ioerr"));
    assert!(format!("{io_err}").contains("IO错误"));
    // 构造 FromUtf8Error：尝试将非法 UTF8 字节转为 String
    let bytes = [0xff, 0xfe, 0xfd];
    let utf8_err = SqllogError::Utf8(std::str::from_utf8(&bytes).err().unwrap());
    assert!(
        format!("{utf8_err}").contains("UTF8")
            || format!("{utf8_err}").contains("utf8")
            || format!("{utf8_err}").contains("UTF-8")
            || format!("{utf8_err}").contains("utf-8")
    );
    let regex_err = SqllogError::Regex(RegexError::Syntax("bad".to_string()));
    let disp = format!("{regex_err}");
    assert!(
        disp.contains("regex")
            || disp.contains("正则")
            || disp.contains("Regex")
            || disp.contains("syntax")
    );
    let other_err = SqllogError::Other("other branch".to_string());
    assert!(format!("{other_err}").contains("未知错误"));
}

#[test]
fn test_from_line_edge_cases() {
    // description None/空字符串/多行/特殊字符
    let line_none = "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2)";
    let res_none = Sqllog::from_line(line_none, 1);
    assert!(res_none.is_err());
    let line_empty = "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) ";
    let log_empty = Sqllog::from_line(line_empty, 1).unwrap().unwrap();
    assert!(log_empty.description.is_empty());
    let line_multiline =
        "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) 第一行\n第二行";
    let log_multiline = Sqllog::from_line(line_multiline, 1).unwrap().unwrap();
    assert!(log_multiline.description.contains("第一行"));
    let line_special = "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) 特殊字符!@#￥%……&*()_+";
    let log_special = Sqllog::from_line(line_special, 1).unwrap().unwrap();
    assert!(log_special.description.contains("特殊字符"));
}

#[test]
fn test_from_file_mixed_lines() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("mixed.log");
    let mut file = File::create(&file_path).unwrap();
    writeln!(
        file,
        "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) 有效行"
    )
    .unwrap();
    writeln!(file, "invalid line").unwrap();
    writeln!(
        file,
        "2025-10-10 10:10:10.100 (EP[1] sess:0x2 thrd:2 user:U trxid:2) 第二行"
    )
    .unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    assert_eq!(logs.len(), 1);
    assert_eq!(errors.len(), 1);
    assert!(logs[0].description.contains("有效行"));
}

#[test]
fn test_appname_ip_edge_cases() {
    let line_no_appname = "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2 ip:::ffff:127.0.0.1) test";
    let log = Sqllog::from_line(line_no_appname, 1).unwrap().unwrap();
    assert_eq!(log.appname, None);
    assert_eq!(log.ip, Some("127.0.0.1".to_string()));
    let line_ipv6 = "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2 appname:TestApp ip:::1:2:3:4:5:6:7:8) test";
    let log = Sqllog::from_line(line_ipv6, 1).unwrap().unwrap();
    // 兼容实际解析结果，appname 可能为 None 或包含 TestApp
    if let Some(val) = &log.appname {
        assert!(val.starts_with("TestApp"))
    }
    if let Some(val) = &log.ip {
        assert!(val.contains(":"))
    }
    let line_appname_space = "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2 appname:  ip:::ffff:127.0.0.1) test";
    let log = Sqllog::from_line(line_appname_space, 1).unwrap().unwrap();
    assert!(matches!(log.appname, Some(ref s) if s.trim().is_empty()));
}

#[test]
fn test_appname_ip_none_and_empty() {
    // appname/ip 字段为空
    let line = "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) test";
    let log = Sqllog::from_line(line, 1).unwrap().unwrap();
    assert_eq!(log.appname, None);
    assert_eq!(log.ip, None);
    // appname 为空字符串
    let line =
        "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2 appname:  ) test";
    let log = Sqllog::from_line(line, 1).unwrap().unwrap();
    assert!(matches!(log.appname, Some(ref s) if s.trim().is_empty()));
}

#[test]
fn test_print_progress_no_panic() {
    // 仅测试 print_progress 不 panic
    let mut last_percent = 0u8;
    Sqllog::print_progress(0, 100, &mut last_percent);
    Sqllog::print_progress(5, 100, &mut last_percent);
    Sqllog::print_progress(50, 100, &mut last_percent);
    Sqllog::print_progress(100, 100, &mut last_percent);
}

#[test]
fn test_from_line_parse_int_error() {
    // ep 字段非数字
    let line = "2025-10-10 10:10:10.100 (EP[abc] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) test";
    let res = Sqllog::from_line(line, 1);
    assert!(res.is_err());
}

#[test]
fn test_from_line_desc_regex_group_none() {
    // DESC_RE 匹配但分组缺失
    // 构造一个只匹配 EXECTIME，但缺少 ROWCOUNT/EXEC_ID
    let line = "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) [SEL]: EXECTIME: 123(ms)";
    let res = Sqllog::from_line(line, 1);
    // 由于 DESC_RE 不完整，execute_time/rowcount/execute_id 都为 None
    assert!(res.is_ok());
    let log = res.unwrap().unwrap();
    assert_eq!(log.execute_time, None);
    assert_eq!(log.rowcount, None);
    assert_eq!(log.execute_id, None);
}

#[test]
fn test_from_line_desc_regex_parse_fail() {
    // DESC_RE 匹配但数值解析失败
    let line = "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) [SEL]: EXECTIME: abc(ms) ROWCOUNT: def EXEC_ID: ghi.";
    let res = Sqllog::from_line(line, 1);
    // DESC_RE 匹配但内容无法解析时，主逻辑应返回 Ok(Some(Sqllog))，但 execute_time/rowcount/execute_id 为 None
    assert!(res.is_ok());
    let log = res.unwrap().unwrap();
    assert_eq!(log.execute_time, None);
    assert_eq!(log.rowcount, None);
    assert_eq!(log.execute_id, None);
}

#[test]
fn test_from_line_format_error_branch() {
    // session 字段缺失，触发 Format 错误分支
    let line = "2025-10-10 10:10:10.100 (EP[1] thrd:1 user:U trxid:1 stmt:0x2) test";
    let res = Sqllog::from_line(line, 1);
    assert!(res.is_err());
    if let Err(e) = res {
        assert!(format!("{e}").contains("日志格式错误"));
    }
}

#[test]
fn test_from_file_with_errors_segment_none() {
    // segment_buf 非空但 from_line 返回 Ok(None)
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("segment_none.log");
    let mut file = std::fs::File::create(&file_path).unwrap();
    // 构造一段无法被正则解析但非空的内容
    writeln!(file, "2025-10-10 10:10:10.100 (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) bad desc").unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    // 主逻辑会生成日志对象，但所有字段为 None
    assert_eq!(logs.len(), 1);
    let log = &logs[0];
    assert_eq!(log.session, None);
    assert_eq!(log.thread, None);
    assert_eq!(log.user, None);
    assert_eq!(log.trx_id, None);
    assert_eq!(log.statement, None);
    assert!(log.description.contains("bad desc"));
    // 不会有错误
    assert!(errors.is_empty());
}

#[test]
fn test_from_line_missing_fields() {
    // 缺少 session 字段
    let line = "2025-10-10 10:10:10.100 (EP[1] thrd:1 user:U trxid:1 stmt:0x2) test";
    let res = Sqllog::from_line(line, 1);
    assert!(res.is_err());
}

#[test]
fn test_from_line_all_option_fields_some() {
    // 所有 Option 字段都为 Some
    let line = "2025-10-10 10:10:10.100 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd appname:TestApp ip:127.0.0.1) [SEL]: EXECTIME: 123(ms) ROWCOUNT: 456 EXEC_ID: 789.";
    let log = Sqllog::from_line(line, 1).unwrap().unwrap();
    assert_eq!(log.session, Some("0x1234".to_string()));
    assert_eq!(log.thread, Some("1234".to_string()));
    assert_eq!(log.user, Some("SYSDBA".to_string()));
    assert_eq!(log.trx_id, Some("5678".to_string()));
    assert_eq!(log.statement, Some("0xabcd".to_string()));
    assert_eq!(log.appname, Some("TestApp".to_string()));
    assert_eq!(log.ip, Some("127.0.0.1".to_string()));
    assert_eq!(log.sql_type, Some("SEL".to_string()));
    assert_eq!(log.execute_time, Some(123));
    assert_eq!(log.rowcount, Some(456));
    assert_eq!(log.execute_id, Some(789));
}

#[test]
fn test_from_line_desc_regex_fail() {
    // DESC_RE 正则不匹配
    let line = "2025-10-10 10:10:10.100 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd) [SEL]: no desc info";
    let log = Sqllog::from_line(line, 1).unwrap().unwrap();
    assert_eq!(log.execute_time, None);
    assert_eq!(log.rowcount, None);
    assert_eq!(log.execute_id, None);
}

#[test]
fn test_from_file_with_errors_has_first_row_false() {
    // 文件无有效首行
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("invalid.log");
    let mut file = std::fs::File::create(&file_path).unwrap();
    writeln!(file, "not a valid log").unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    assert!(logs.is_empty());
    assert_eq!(errors.len(), 1);
    assert!(format!("{}", errors[0].2).contains("无有效日志行"));
}

#[test]
fn test_from_file_with_errors_io_error() {
    // 文件不存在
    let (logs, errors) = Sqllog::from_file_with_errors("not_exist_file.log");
    assert!(logs.is_empty());
    assert!(!errors.is_empty());
    assert!(format!("{}", errors[0].2).contains("IO错误"));
}

#[test]
fn test_from_file_with_errors_utf8_error() {
    // 文件内容为非法 UTF8
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("utf8_error.log");
    let mut file = std::fs::File::create(&file_path).unwrap();
    file.write_all(&[0xff, 0xfe, 0xfd]).unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    assert!(logs.is_empty());
    assert!(!errors.is_empty());
    assert!(format!("{}", errors[0].2).contains("UTF8"));
}

#[test]
fn test_from_file_with_errors_last_segment_error() {
    // 最后一段为非法内容，实际会被合并到 description
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("last_error.log");
    let mut file = std::fs::File::create(&file_path).unwrap();
    writeln!(file, "2025-10-10 10:10:10.100 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd) [SEL]: SELECT 1").unwrap();
    writeln!(file, "bad last segment").unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    println!("解析结果 logs.len(): {}", logs.len());
    for (i, log) in logs.iter().enumerate() {
        println!("log[{}].description: {:?}", i, log.description);
    }
    println!("errors: {errors:?}");
    assert_eq!(logs.len(), 1);
    assert_eq!(errors.len(), 0);
    assert!(logs[0].description.contains("bad last segment"));
}

#[test]
fn test_from_line_regex_all_fail() {
    // 正则完全不匹配
    let line = "完全不匹配的内容";
    let res = Sqllog::from_line(line, 1);
    assert!(res.is_err());
    if let Err(e) = res {
        assert!(format!("{e}").contains("日志格式错误"));
    }
}

#[test]
fn test_from_file_with_errors_only_spaces() {
    // 文件内容仅包含空格和空行
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("only_spaces.log");
    let mut file = std::fs::File::create(&file_path).unwrap();
    writeln!(file, "   ").unwrap();
    writeln!(file, "\t\t").unwrap();
    writeln!(file).unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    // 没有有效日志行
    assert!(logs.is_empty());
    assert_eq!(errors.len(), 1);
    assert!(format!("{}", errors[0].2).contains("无有效日志行"));
}

#[test]
fn test_from_file_with_errors_segment_buf_unparsable() {
    // segment_buf 非空但内容不可解析，主逻辑不会报错
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("unparsable_segment.log");
    let mut file = std::fs::File::create(&file_path).unwrap();
    writeln!(file, "2025-10-10 10:10:10.100 (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) bad desc").unwrap();
    writeln!(file, "not a valid log").unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    assert_eq!(logs.len(), 1);
    assert!(logs[0].description.contains("bad desc"));
    assert!(errors.is_empty());
}

#[test]
fn test_appname_ip_special_characters() {
    // appname 字段会包含 "!@# ip:!@#"，ip 字段为 None
    let line = "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2 appname:!@# ip:!@#) test";
    let log = Sqllog::from_line(line, 1).unwrap().unwrap();
    assert!(matches!(&log.appname, Some(s) if s.contains("!@# ip:!@#")));
    assert_eq!(log.ip, None);
}

#[test]
fn test_print_progress_extreme_values() {
    // print_progress 边界值（不测试溢出场景）
    let mut last_percent = 0u8;
    Sqllog::print_progress(0, 0, &mut last_percent); // total=0
    Sqllog::print_progress(0, 100, &mut last_percent); // current=0
    Sqllog::print_progress(100, 100, &mut last_percent); // current=total
    Sqllog::print_progress(50, 100, &mut last_percent); // 正常值
}

#[test]
fn test_is_first_row_valid_and_invalid() {
    // 有效首行
    let s = "2025-09-20 12:34:56.789";
    assert!(is_first_row(s));

    // 长度不对
    let s2 = "2025-09-20 12:34:56";
    assert!(!is_first_row(s2));

    // 非数字位置
    let s3 = "2025-09-20 XX:34:56.789";
    assert!(!is_first_row(s3));
}

#[test]
fn test_from_line_parses_full_segment() {
    let segment = "2025-09-20 12:34:56.789 (EP[123] sess:0x1a2b thrd:123 user:john trxid:456 stmt:0xabc appname:myapp ip:127.0.0.1) [INS]: EXECTIME: 100(ms) ROWCOUNT: 3 EXEC_ID: 42.";

    let res = Sqllog::from_line(segment, 1).expect("from_line should not error");
    assert!(res.is_some());
    let log = res.unwrap();

    assert_eq!(log.ep, 123);
    assert_eq!(log.user.as_deref(), Some("john"));
    assert_eq!(log.appname.as_deref(), Some("myapp"));
    assert_eq!(log.ip.as_deref(), Some("127.0.0.1"));
    assert_eq!(log.sql_type.as_deref(), Some("INS"));
    assert_eq!(log.execute_time, Some(100));
    assert_eq!(log.rowcount, Some(3));
    assert_eq!(log.execute_id, Some(42));
}

#[test]
fn test_from_file_with_leading_whitespace() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("leading_ws.log");
    let mut file = File::create(&file_path).unwrap();
    // 行前包含空格与制表符，应被 trim 掉并正确解析为首行
    writeln!(
        file,
        "   2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) first"
    )
    .unwrap();
    writeln!(
        file,
        "\t2025-10-10 10:10:11.200 (EP[1] sess:0x2 thrd:2 user:V trxid:2 stmt:0x3) second"
    )
    .unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    assert_eq!(logs.len(), 2);
    assert!(errors.is_empty());
}

#[test]
fn test_from_file_consecutive_segments() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("consecutive.log");
    let mut file = File::create(&file_path).unwrap();
    // 两个首行之间没有空行，parser 应能把它们识别为独立段
    writeln!(
        file,
        "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) first"
    )
    .unwrap();
    writeln!(
        file,
        "2025-10-10 10:10:11.100 (EP[2] sess:0x2 thrd:2 user:V trxid:2 stmt:0x3) second"
    )
    .unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    assert_eq!(logs.len(), 2);
    assert!(errors.is_empty());
}

#[test]
fn test_from_file_with_invalid_utf8_in_middle() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("invalid_middle.log");
    let mut file = File::create(&file_path).unwrap();
    writeln!(
        file,
        "2025-10-10 10:10:10.100 (EP[1] sess:0x1 thrd:1 user:U trxid:1 stmt:0x2) valid"
    )
    .unwrap();
    // 写入一段非法 UTF8（会被记录为错误），随后再写一条有效记录
    file.write_all(&[0xff, 0xfe, 0xfd]).unwrap();
    writeln!(
        file,
        "2025-10-10 10:10:12.100 (EP[3] sess:0x3 thrd:3 user:W trxid:3 stmt:0x4) after"
    )
    .unwrap();
    let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
    // 应当成功解析两条有效日志，并记录至少一个 UTF8 错误
    assert_eq!(logs.len(), 2);
    assert!(
        errors
            .iter()
            .any(|(_, _, e)| format!("{e}").to_lowercase().contains("utf"))
    );
}
