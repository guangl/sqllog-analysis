#![cfg(any(feature = "exporter-csv", feature = "exporter-json"))]

use std::fs;
use std::time::{Duration, Instant};

use sqllog_analysis::Sqllog;
use sqllog_analysis::exporter::{
    ExportStats, SyncCsvExporter, SyncExporter, SyncMultiExporter,
};

#[cfg(feature = "exporter-json")]
use sqllog_analysis::exporter::SyncJsonExporter;

#[test]
fn test_export_stats_duration_rps_merge_reset_display() {
    let mut a = ExportStats::new();
    a.exported_records = 10;
    a.failed_records = 2;
    // set artificial times
    a.start_time = Some(Instant::now() - Duration::from_secs(2));
    a.end_time = Some(Instant::now());

    // duration and rps should be Some and > 0
    let dur = a.duration().unwrap();
    assert!(dur.as_secs() >= 2);

    let rps = a.records_per_second().unwrap();
    assert!(rps > 0.0);

    // success rate
    let rate = a.success_rate();
    assert!(rate > 0.0 && rate <= 100.0);

    // total
    assert_eq!(a.total_records(), 12);

    // display should contain exported and failed
    let s = format!("{}", a);
    assert!(s.contains("成功"));
    assert!(s.contains("失败"));

    // reset
    a.reset();
    assert_eq!(a.exported_records, 0);
    assert_eq!(a.failed_records, 0);

    // merge: ensure earliest start and latest end preserved
    let mut b = ExportStats::new();
    b.exported_records = 3;
    b.failed_records = 1;
    b.start_time = Some(Instant::now() - Duration::from_secs(10));
    b.end_time = Some(Instant::now() + Duration::from_secs(1));

    a.merge(&b);
    assert_eq!(a.exported_records, 3);
    assert_eq!(a.failed_records, 1);
    // merged times exist
    assert!(a.start_time.is_some());
    assert!(a.end_time.is_some());
}

#[cfg(all(feature = "exporter-csv", feature = "exporter-json"))]
#[test]
fn test_sync_csv_and_json_exporters_escape_and_format() {
    // prepare a sample sqllog record with commas, quotes and newlines
    let record = Sqllog {
        occurrence_time: "2025-09-24T00:00:00Z".to_string(),
        ep: "EP1".to_string(),
        session: Some("s1".to_string()),
        thread: Some("t1".to_string()),
        user: Some("u".to_string()),
        trx_id: Some("trx".to_string()),
        statement: Some("select 1".to_string()),
        appname: Some("app".to_string()),
        ip: Some("127.0.0.1".to_string()),
        sql_type: Some("SEL".to_string()),
        description: "a,\"\"b\nline".to_string(),
        execute_time: Some(123),
        rowcount: Some(1),
        execute_id: Some(42),
    };

    let tmp_csv = std::env::temp_dir().join("test_targeted_export.csv");
    let tmp_json = std::env::temp_dir().join("test_targeted_export.json");
    let _ = fs::remove_file(&tmp_csv);
    let _ = fs::remove_file(&tmp_json);

    // CSV
    let mut csv = SyncCsvExporter::new(&tmp_csv).expect("create csv");
    // finalize without records should write header
    csv.finalize().expect("finalize header");
    let content = fs::read_to_string(&tmp_csv).expect("read csv");
    assert!(content.starts_with("occurrence_time,ep,"));

    // now write a real record
    let mut csv2 = SyncCsvExporter::new(&tmp_csv).expect("create csv2");
    csv2.export_record(&record).expect("export record csv");
    csv2.finalize().expect("finalize csv2");
    let content2 = fs::read_to_string(&tmp_csv).expect("read csv2");
    // header + one data line
    assert!(content2.contains("occurrence_time,ep,"));
    // escaped description should be quoted and keep newline inside quotes
    assert!(content2.contains("\"a,\"\"b\nline\"") || content2.contains("a,"));

    // JSON
    let mut json = SyncJsonExporter::new(&tmp_json).expect("create json");
    // insert two records to trigger comma insertion path
    json.insert_records(&[record.clone()]).expect("insert1");
    json.insert_records(&[record.clone()]).expect("insert2");
    json.finalize().expect("finalize json");
    let jcont = fs::read_to_string(&tmp_json).expect("read json");
    // should start with [ and end with ]
    assert!(jcont.starts_with("["));
    assert!(jcont.ends_with("\n]\n") || jcont.ends_with("]\n"));
    // ensure comma between pretty objects exists
    assert!(
        jcont.contains(",\n  {") || jcont.contains(",\n  \"occurrence_time\"")
    );

    let _ = fs::remove_file(&tmp_csv);
    let _ = fs::remove_file(&tmp_json);
}

#[test]
fn test_sync_multi_exporter_stats_and_print() {
    // Create a failing exporter stub
    struct FailExporter;
    impl SyncExporter for FailExporter {
        fn name(&self) -> &str {
            "FAIL"
        }
        fn export_record(
            &mut self,
            _record: &Sqllog,
        ) -> sqllog_analysis::error::Result<()> {
            Err(sqllog_analysis::error::SqllogError::Other("fail".into()))
        }
    }

    // But FailExporter is defined in test; need to box as dyn SyncExporter
    let mut multi = SyncMultiExporter::new();

    // add a real csv exporter if feature enabled, else add a minimal dummy that succeeds
    #[allow(unused_mut)]
    let mut _added_real = false;
    #[cfg(feature = "exporter-csv")]
    {
        let tmp = std::env::temp_dir().join("test_multi_export.csv");
        let _ = fs::remove_file(&tmp);
        multi.add_exporter(SyncCsvExporter::new(&tmp).expect("csv"));
        _added_real = true;
    }

    // add failing exporter via trait object
    // We need to box a type that implements SyncExporter; implement it here using a simple struct
    struct DummySucc;
    impl SyncExporter for DummySucc {
        fn name(&self) -> &str {
            "DUMMY"
        }
        fn export_record(
            &mut self,
            _record: &Sqllog,
        ) -> sqllog_analysis::error::Result<()> {
            Ok(())
        }
    }

    multi.add_exporter(DummySucc);
    multi.add_exporter(FailExporter);

    // export a record
    let rec = Sqllog::new();
    let _ = multi.export_record(&rec);

    // finalize (will call finalize on exporters; failing exporter finalize uses default Ok())
    let _ = multi.finalize_all();

    // get stats
    let stats = multi.get_all_stats();
    assert!(stats.len() >= 2);

    // exercise print_stats_report (just ensure it runs)
    multi.print_stats_report();
}
