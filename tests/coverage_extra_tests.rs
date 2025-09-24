use std::io::Read;
use tempfile::NamedTempFile;

use sqllog_analysis::prelude::*;

fn make_escape_record() -> Sqllog {
    let mut r = Sqllog::new();
    r.occurrence_time = "2025-09-24T00:00:00Z".to_string();
    r.ep = "ep".to_string();
    // field with comma, quote, newline and carriage return to exercise CSV escaping
    r.statement =
        Some("He said \"Hi, world\", then newline\nand CR\rend".to_string());
    r.description = "desc with,comma".to_string();
    r
}

#[cfg(feature = "exporter-csv")]
#[test]
fn test_csv_escape_and_finalize()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    // create exporter but do not write any records, finalize should still write header
    let mut exporter = SyncCsvExporter::new(&path)?;
    exporter.finalize()?;

    let mut s = String::new();
    tmp.read_to_string(&mut s)?;
    assert!(s.contains("occurrence_time,ep,session"));

    // now create exporter and write an escaping record
    let mut tmp2 = NamedTempFile::new()?;
    let p2 = tmp2.path().to_path_buf();
    let mut exporter2 = SyncCsvExporter::new(&p2)?;
    let r = make_escape_record();
    exporter2.export_record(&r)?;
    exporter2.finalize()?;

    let mut out = String::new();
    tmp2.read_to_string(&mut out)?;

    // statement should be quoted and internal quotes doubled
    assert!(out.contains("\"He said \"\"Hi, world\"\", then newline"));
    // description contains a comma and should be quoted
    assert!(out.contains("\"desc with,comma\""));

    Ok(())
}

#[cfg(feature = "exporter-json")]
#[test]
fn test_json_first_record_and_finalize()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut exporter = SyncJsonExporter::new(&path)?;

    // first record (will not write leading comma)
    let mut r1 = Sqllog::new();
    r1.occurrence_time = "t1".to_string();
    r1.ep = "ep1".to_string();
    exporter.export_record(&r1)?;

    // second record (should trigger the ",\n" path)
    let mut r2 = Sqllog::new();
    r2.occurrence_time = "t2".to_string();
    r2.ep = "ep2".to_string();
    exporter.export_record(&r2)?;

    exporter.finalize()?;

    let mut s = String::new();
    tmp.read_to_string(&mut s)?;

    // file should start with array open and contain a comma between pretty objects
    assert!(s.trim_start().starts_with("["));
    assert!(s.contains(",\n  {"));

    Ok(())
}

#[cfg(feature = "exporter-csv")]
#[test]
fn test_multi_exporter_handles_failures()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use sqllog_analysis::exporter::sync_impl::SyncExporter as TestSyncExporter;

    // A simple exporter that always fails on export_record and finalize
    struct FailExporter;
    impl TestSyncExporter for FailExporter {
        fn name(&self) -> &str {
            "FAIL"
        }
        fn export_record(&mut self, _record: &Sqllog) -> crate::Result<()> {
            Err(crate::SqllogError::other("export fail"))
        }
        fn finalize(&mut self) -> crate::Result<()> {
            Err(crate::SqllogError::other("finalize fail"))
        }
    }

    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut multi = SyncMultiExporter::new();
    multi.add_exporter(FailExporter);
    multi.add_exporter(SyncCsvExporter::new(&path)?);

    let mut r = Sqllog::new();
    r.occurrence_time = "time".to_string();
    r.ep = "ep".to_string();

    // export_record should swallow errors and continue
    multi.export_record(&r)?;

    let stats = multi.get_all_stats();
    // first exporter failed once, second succeeded once
    assert_eq!(stats[0].1.failed_records, 1);
    assert_eq!(stats[0].1.exported_records, 0);
    assert_eq!(stats[1].1.exported_records, 1);

    // finalize_all: first finalize will fail (increment failed_records), second will finish
    multi.finalize_all()?;
    let stats2 = multi.get_all_stats();
    assert_eq!(stats2[0].1.failed_records, 2);
    // second exporter should have end_time set (finish called)
    assert!(stats2[1].1.end_time.is_some());

    Ok(())
}
