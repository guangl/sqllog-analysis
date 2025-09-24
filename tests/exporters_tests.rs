use std::io::Read;
use tempfile::NamedTempFile;

use sqllog_analysis::prelude::*;

fn make_sample_record() -> Sqllog {
    Sqllog {
        occurrence_time: "2025-09-24T00:00:00Z".to_string(),
        ep: "ep1".to_string(),
        session: Some("s1".to_string()),
        thread: Some("t1".to_string()),
        user: Some("u1".to_string()),
        trx_id: Some("trx1".to_string()),
        statement: Some("SELECT 1".to_string()),
        appname: Some("app".to_string()),
        ip: Some("127.0.0.1".to_string()),
        sql_type: Some("QUERY".to_string()),
        description: "desc".to_string(),
        execute_time: Some(123),
        rowcount: Some(1),
        execute_id: Some(42),
    }
}

#[cfg(feature = "exporter-csv")]
#[test]
fn test_sync_csv_exporter_basic() {
    let mut tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let mut exporter = SyncCsvExporter::new(&path).unwrap();
    let r1 = make_sample_record();
    let r2 = make_sample_record();

    exporter.export_record(&r1).unwrap();
    exporter.export_batch(&[r2.clone()]).unwrap();
    exporter.finalize().unwrap();

    let stats = exporter.get_stats();
    assert_eq!(stats.exported_records, 2);

    // read file and check header exists
    let mut s = String::new();
    tmp.read_to_string(&mut s).unwrap();
    assert!(s.contains("occurrence_time,ep,session"));
    assert!(s.contains("SELECT 1"));
}

#[cfg(feature = "exporter-json")]
#[test]
fn test_sync_json_exporter_basic() {
    let mut tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let mut exporter = SyncJsonExporter::new(&path).unwrap();
    let r1 = make_sample_record();
    let r2 = make_sample_record();

    exporter.export_record(&r1).unwrap();
    exporter.export_batch(&[r2.clone()]).unwrap();
    exporter.finalize().unwrap();

    let stats = exporter.get_stats();
    assert_eq!(stats.exported_records, 2);

    // read file and check it starts with [ and contains description
    let mut s = String::new();
    tmp.read_to_string(&mut s).unwrap();
    assert!(s.trim_start().starts_with("["));
    assert!(s.contains("desc"));
}
