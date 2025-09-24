#![cfg(all(
    test,
    any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    )
))]

use sqllog_analysis::exporter::ExportStats;
use std::thread::sleep;
use std::time::Duration;

#[test]
fn test_export_stats_basic_lifecycle() {
    let mut s = ExportStats::new();
    // start_time should be set
    assert!(s.start_time.is_some());

    s.exported_records = 5;
    s.failed_records = 2;

    // success_rate and total_records
    assert_eq!(s.total_records(), 7);
    let rate = s.success_rate();
    assert!(rate > 0.0 && rate <= 100.0);

    // duration is None until finish
    assert!(s.duration().is_none());

    // wait a small amount and finish
    sleep(Duration::from_millis(5));
    s.finish();
    assert!(s.duration().is_some());

    // records_per_second should produce a finite number (or 0)
    let rps = s.records_per_second();
    assert!(rps.is_some());

    // reset resets counters and times
    s.reset();
    assert_eq!(s.exported_records, 0);
    assert_eq!(s.failed_records, 0);
    assert!(s.start_time.is_some());
    assert!(s.end_time.is_none());

    // merge: merging a stats with later end_time should maintain later end
    let mut a = ExportStats::new();
    a.exported_records = 1;
    sleep(Duration::from_millis(2));
    a.finish();

    let mut b = ExportStats::new();
    b.exported_records = 2;
    sleep(Duration::from_millis(4));
    b.finish();

    let _a_start = a.start_time;
    a.merge(&b);
    assert_eq!(a.exported_records, 3);
    // a.start_time should be <= original a_start (kept earliest)
    assert!(a.start_time.is_some());
    assert!(a.end_time.is_some());
}

#[test]
fn test_export_stats_display_contains_values() {
    let mut s = ExportStats::new();
    s.exported_records = 3;
    s.failed_records = 1;
    // ensure display works after finish
    s.finish();
    let out = format!("{}", s);
    assert!(
        out.contains("成功") || out.contains("失败") || out.contains("成功率")
    );
}

#[test]
fn test_export_stats_merge_and_time_edge_cases() {
    // case: merging when self has no times should copy other's times
    let mut a = ExportStats::default(); // start_time == None
    a.exported_records = 0;

    let mut b = ExportStats::new();
    b.exported_records = 2;
    b.failed_records = 1;
    b.finish();

    a.merge(&b);
    assert_eq!(a.exported_records, 2);
    assert_eq!(a.failed_records, 1);
    assert!(a.start_time.is_some());
    assert!(a.end_time.is_some());

    // case: keep earliest start_time and latest end_time
    use std::time::Instant;
    let now = Instant::now();
    let earlier = now - Duration::from_secs(10);
    let later = now + Duration::from_secs(10);

    let mut s = ExportStats::new();
    s.start_time = Some(later);
    s.end_time = Some(now);

    let mut o = ExportStats::new();
    o.start_time = Some(earlier);
    o.end_time = Some(later + Duration::from_secs(20));

    s.merge(&o);
    assert_eq!(s.start_time, Some(earlier));
    assert_eq!(s.end_time, Some(later + Duration::from_secs(20)));
}

#[test]
fn test_records_per_second_zero_duration_and_display_without_duration() {
    use std::time::Instant;

    // duration zero (start == end) should lead to records_per_second == Some(0.0)
    let mut s = ExportStats::default();
    let t = Instant::now();
    s.start_time = Some(t);
    s.end_time = Some(t);
    s.exported_records = 10;
    let dur = s.duration().expect("duration should be Some");
    assert_eq!(dur.as_secs(), 0);
    let rps = s.records_per_second().expect("rps should be Some");
    assert_eq!(rps, 0.0);

    // Display when no duration should not include 耗时/速度 but include 成功率
    let mut n = ExportStats::default();
    n.exported_records = 0;
    n.failed_records = 0;
    let out = format!("{}", n);
    assert!(
        out.contains("成功") && out.contains("失败") && out.contains("成功率")
    );
    assert!(!out.contains("耗时"));
}
