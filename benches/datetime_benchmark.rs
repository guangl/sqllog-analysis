use criterion::{Criterion, criterion_group, criterion_main};
use regex::Regex;
use sqllog_analysis::sqllog::is_first_row;
use std::hint;

fn validate_with_regex(s: &str) -> bool {
    // 编译正则表达式
    lazy_static::lazy_static! {
        static ref DATE_REGEX: Regex = Regex::new(
            r"^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01]) (?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d\.\d{3}$"
        ).unwrap();
    }
    DATE_REGEX.is_match(s)
}

fn validate_with_chrono(s: &str) -> bool {
    use chrono::NaiveDateTime;
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f").is_ok()
}

fn datetime_benchmark(c: &mut Criterion) {
    let valid_date = "2025-10-10 10:10:10.100";
    let invalid_date = "2025-13-10 10:10:10.100";

    let mut group = c.benchmark_group("datetime_validation");

    // 测试自定义实现 - 有效日期
    group.bench_function("custom_valid", |b| {
        b.iter(|| is_first_row(hint::black_box(valid_date)));
    });

    // 测试自定义实现 - 无效日期
    group.bench_function("custom_invalid", |b| {
        b.iter(|| is_first_row(hint::black_box(invalid_date)));
    });

    // 测试正则表达式实现 - 有效日期
    group.bench_function("regex_valid", |b| {
        b.iter(|| validate_with_regex(hint::black_box(valid_date)));
    });

    // 测试正则表达式实现 - 无效日期
    group.bench_function("regex_invalid", |b| {
        b.iter(|| validate_with_regex(hint::black_box(invalid_date)));
    });

    // 测试 chrono 实现 - 有效日期
    group.bench_function("chrono_valid", |b| {
        b.iter(|| validate_with_chrono(hint::black_box(valid_date)));
    });

    // 测试 chrono 实现 - 无效日期
    group.bench_function("chrono_invalid", |b| {
        b.iter(|| validate_with_chrono(hint::black_box(invalid_date)));
    });

    group.finish();
}

criterion_group!(benches, datetime_benchmark);
criterion_main!(benches);
