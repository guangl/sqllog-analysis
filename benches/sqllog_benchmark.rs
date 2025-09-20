#![allow(clippy::uninlined_format_args)]
use criterion::{Criterion, criterion_group, criterion_main};
use sqllog_analysis::sqllog::Sqllog;
use std::{fs::File, io::Write};

fn bench_sqllog_from_file_1m(c: &mut Criterion) {
    // 构造 100 万条日志，每条 description 多行
    let mut log_content = String::new();
    for i in 0..1_000_000 {
        log_content.push_str(&format!(
            "2025-10-10 10:10:10.100 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd) [SEL]: 第一行描述{}\n第二行内容{}\n第三行内容{}\n\n",
            i, i, i
        ));
    }
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("bench_1m.log");
    let mut file = File::create(&file_path).unwrap();
    file.write_all(log_content.as_bytes()).unwrap();
    c.bench_function("sqllog_from_file_1m", |b| {
        b.iter(|| {
            let (logs, errors) = Sqllog::from_file_with_errors(&file_path);
            assert_eq!(logs.len(), 1_000_000);
            assert!(errors.is_empty());
        })
    });
}

criterion_group!(benches, bench_sqllog_from_file_1m);
criterion_main!(benches);
