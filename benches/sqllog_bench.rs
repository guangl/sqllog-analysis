#![allow(clippy::uninlined_format_args)]
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use sqllog_analysis::sqllog::Sqllog;
use std::{fs::File, io::Write, time::Duration};

fn write_log_file(n: usize, path: &std::path::Path) {
    // Stream-writing to avoid holding the entire content in memory
    let mut file = File::create(path).unwrap();
    for i in 0..n {
        let _ = write!(
            file,
            "2025-10-10 10:10:10.100 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd) [SEL]: 第一行描述{}\n第二行内容{}\n第三行内容{}\n\n",
            i, i, i
        );
    }
}

fn bench_sqllog_varied(c: &mut Criterion) {
    // finer-grained sizes for the benchmark
    let sizes = [100_000usize, 200_000usize, 500_000usize, 1_000_000usize];

    // Group A: measure file generation (write)
    let mut g_write = c.benchmark_group("sqllog_write_file");
    // Reduce samples and increase per-sample time to keep runtime reasonable
    g_write.sample_size(10);
    g_write.measurement_time(Duration::from_secs(10));
    for &n in &sizes {
        g_write.bench_with_input(
            BenchmarkId::from_parameter(n),
            &n,
            |b, &size| {
                b.iter_batched(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let p = dir.path().join(format!("bench_{}.log", size));
                        (dir, p)
                    },
                    |(dir, path)| {
                        write_log_file(size, &path);
                        // keep dir alive until teardown
                        drop(path);
                        drop(dir);
                    },
                    criterion::BatchSize::PerIteration,
                )
            },
        );
    }
    g_write.finish();

    // Group B: measure parsing performance (from_file_with_errors)
    let mut g_parse = c.benchmark_group("sqllog_parse_file");
    g_parse.sample_size(10);
    g_parse.measurement_time(Duration::from_secs(10));
    for &n in &sizes {
        g_parse.bench_with_input(
            BenchmarkId::from_parameter(n),
            &n,
            |b, &size| {
                b.iter_batched(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let p = dir.path().join(format!("bench_{}.log", size));
                        write_log_file(size, &p);
                        (dir, p)
                    },
                    |(dir, path)| {
                        let (logs, errors) =
                            Sqllog::from_file_with_errors(&path);
                        assert_eq!(logs.len(), size);
                        assert!(errors.is_empty());
                        drop(path);
                        drop(dir);
                    },
                    criterion::BatchSize::PerIteration,
                )
            },
        );
    }
    g_parse.finish();
}

criterion_group!(benches, bench_sqllog_varied);
criterion_main!(benches);
