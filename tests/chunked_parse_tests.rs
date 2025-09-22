use sqllog_analysis::sqllog::Sqllog;
use std::fs::File;
use std::io::Write;

fn write_tmp(content: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let uniq = format!("{nanos}");
    let path = dir.join(format!("sqllog_test_{uniq}.log"));
    let mut f = File::create(&path).expect("create tmp file");
    f.write_all(content.as_bytes()).expect("write tmp");
    path
}

#[test]
fn chunked_happy_path() {
    let sample = "2025-09-21 12:00:00.000 (EP[1] sess:NULL thrd:1 user:usr trxid:1 stmt:NULL) [SEL]: select 1 EXECTIME: 1(ms) ROWCOUNT: 1 EXEC_ID: 1.\n";
    let mut data = String::new();
    for _ in 0..5 {
        data.push_str(sample);
    }
    let path = write_tmp(&data);

    let mut calls = 0usize;
    let mut total = 0usize;
    let res = Sqllog::parse_in_chunks(
        path.clone(),
        2,
        |chunk: &[sqllog_analysis::sqllog::Sqllog]| {
            calls += 1;
            total += chunk.len();
        },
        |_: &[(usize, String, sqllog_analysis::sqllog::SqllogError)]| {},
    );
    assert!(res.is_ok());
    assert_eq!(calls, 3); // 5 records -> chunks of 2,2,1
    assert_eq!(total, 5);
    let _ = std::fs::remove_file(path);
}

#[test]
fn chunked_early_stop() {
    let sample = "2025-09-21 12:00:00.000 (EP[1] sess:NULL thrd:1 user:usr trxid:1 stmt:NULL) [SEL]: select 1 EXECTIME: 1(ms) ROWCOUNT: 1 EXEC_ID: 1.\n";
    let mut data = String::new();
    for _ in 0..10 {
        data.push_str(sample);
    }
    let path = write_tmp(&data);

    let mut calls = 0usize;
    let res = Sqllog::parse_in_chunks(
        path.clone(),
        3,
        |_chunk: &[sqllog_analysis::sqllog::Sqllog]| {
            calls += 1;
        },
        |_: &[(usize, String, sqllog_analysis::sqllog::SqllogError)]| {},
    );
    assert!(res.is_ok());
    // 10 records -> chunks of 3,3,3,1 => 4 calls
    assert_eq!(calls, 4);
    let _ = std::fs::remove_file(path);
}

#[test]
fn chunk_size_greater_than_total() {
    let sample = "2025-09-21 12:00:00.000 (EP[1] sess:NULL thrd:1 user:usr trxid:1 stmt:NULL) [SEL]: select 1 EXECTIME: 1(ms) ROWCOUNT: 1 EXEC_ID: 1.\n";
    let mut data = String::new();
    for _ in 0..5 {
        data.push_str(sample);
    }
    let path = write_tmp(&data);

    let mut calls = 0usize;
    let res = Sqllog::parse_in_chunks(
        path.clone(),
        10,
        |_chunk: &[sqllog_analysis::sqllog::Sqllog]| {
            calls += 1;
        },
        |_: &[(usize, String, sqllog_analysis::sqllog::SqllogError)]| {},
    );
    assert!(res.is_ok());
    assert_eq!(calls, 1);
    let _ = std::fs::remove_file(path);
}

#[test]
fn chunk_size_zero_no_chunking() {
    let sample = "2025-09-21 12:00:00.000 (EP[1] sess:NULL thrd:1 user:usr trxid:1 stmt:NULL) [SEL]: select 1 EXECTIME: 1(ms) ROWCOUNT: 1 EXEC_ID: 1.\n";
    let mut data = String::new();
    for _ in 0..5 {
        data.push_str(sample);
    }
    let path = write_tmp(&data);

    // chunk_size == 0 表示不走分块逻辑；parse_all 会在解析完成后调用 hook
    let mut calls = 0usize;
    let res = Sqllog::parse_all(
        path.clone(),
        0, // chunk_size 为 0 表示不分块
        |_all: &[sqllog_analysis::sqllog::Sqllog]| {
            calls += 1;
        },
        |_: &[(usize, String, sqllog_analysis::sqllog::SqllogError)]| {},
    );
    assert!(res.is_ok());
    assert_eq!(calls, 1);
    let _ = std::fs::remove_file(path);
}
