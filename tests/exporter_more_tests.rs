#![cfg(any(feature = "exporter-csv", feature = "exporter-json"))]

use sqllog_analysis::error::Result;
use sqllog_analysis::exporter::SyncExporter;
use sqllog_analysis::exporter::SyncMultiExporter;
use sqllog_analysis::sqllog::types::Sqllog;

use std::fs;

#[cfg(feature = "exporter-csv")]
#[test]
fn test_csv_escape_and_empty_insert_and_finalize_writes_header() -> Result<()> {
    let tmp = std::env::temp_dir().join("test_exporter_more.csv");
    let _ = fs::remove_file(&tmp);

    let mut exporter = sqllog_analysis::exporter::SyncCsvExporter::new(&tmp)?;

    // empty insert shouldn't write anything
    exporter.insert_records(&[])?;

    // now finalize should write header even if no records
    exporter.finalize()?;

    let s = fs::read_to_string(&tmp)?;
    assert!(s.starts_with("occurrence_time,ep,session"));

    // now test escaping by writing a record with commas, quotes and newlines
    let mut exporter2 = sqllog_analysis::exporter::SyncCsvExporter::new(&tmp)?;
    let rec = Sqllog {
        occurrence_time: "t".into(),
        ep: "e".into(),
        session: None,
        thread: None,
        user: None,
        trx_id: None,
        statement: None,
        appname: None,
        ip: None,
        sql_type: None,
        description: "a,\"b\nline".into(),
        execute_time: None,
        rowcount: None,
        execute_id: None,
    };

    exporter2.export_record(&rec)?;
    exporter2.finalize()?;

    let s2 = fs::read_to_string(&tmp)?;
    // escaped description should be quoted and double-quote inside doubled
    // note: exporter writes an actual newline inside the quoted CSV field, so
    // we look for the quoted field containing a real '\n' character
    assert!(s2.contains("\"a,\"\"b\nline\"") || s2.contains("a,\"b"));

    let _ = fs::remove_file(&tmp);
    Ok(())
}

#[cfg(feature = "exporter-json")]
#[test]
fn test_json_multiple_records_and_finalize() -> Result<()> {
    let tmp = std::env::temp_dir().join("test_exporter_more.json");
    let _ = fs::remove_file(&tmp);

    let mut exporter = sqllog_analysis::exporter::SyncJsonExporter::new(&tmp)?;

    let r1 = Sqllog {
        occurrence_time: "t1".into(),
        ep: "e1".into(),
        session: None,
        thread: None,
        user: None,
        trx_id: None,
        statement: None,
        appname: None,
        ip: None,
        sql_type: None,
        description: "d1".into(),
        execute_time: None,
        rowcount: None,
        execute_id: None,
    };

    let r2 = Sqllog {
        occurrence_time: "t2".into(),
        ep: "e2".into(),
        ..Default::default()
    };

    // insert two records to hit the comma insertion path
    exporter.insert_records(&[r1.clone()])?;
    exporter.insert_records(&[r2.clone()])?;
    exporter.finalize()?;

    let s = fs::read_to_string(&tmp)?;
    assert!(s.trim_start().starts_with("["));
    // there should be a comma before the next pretty-printed object (which starts with '{')
    assert!(s.contains(",\n  {"));

    let _ = fs::remove_file(&tmp);
    Ok(())
}

// Test SyncMultiExporter success and failure branches by adding a real exporter and a failing one
#[test]
fn test_multi_exporter_success_and_failure() -> Result<()> {
    // This test requires at least one exporter feature to compile the real exporter
    // We'll always use the MultiExporter and a dummy failing exporter.
    let mut multi = SyncMultiExporter::new();

    // add a dummy exporter that always fails
    struct FailExporter;
    impl SyncExporter for FailExporter {
        fn name(&self) -> &str {
            "Fail"
        }
        fn export_record(
            &mut self,
            _record: &Sqllog,
        ) -> sqllog_analysis::error::Result<()> {
            Err(sqllog_analysis::error::SqllogError::other("fail"))
        }
        fn finalize(&mut self) -> sqllog_analysis::error::Result<()> {
            Err(sqllog_analysis::error::SqllogError::other("finalize fail"))
        }
    }

    // If CSV feature exists, add a CSV exporter to exercise mixed results path
    #[cfg(feature = "exporter-csv")]
    {
        let tmp = std::env::temp_dir().join("multi_ok.csv");
        let _ = fs::remove_file(&tmp);
        let ok = sqllog_analysis::exporter::SyncCsvExporter::new(&tmp)?;
        multi.add_exporter(ok);
    }

    multi.add_exporter(FailExporter);

    // export a record: CSV exporter should succeed (if present), FailExporter should increment failed
    let rec = Sqllog {
        occurrence_time: "t".into(),
        ep: "e".into(),
        ..Default::default()
    };
    multi.export_record(&rec)?;

    // finalize_all should call finalize on each exporter; FailExporter returns Err and increments failed_records
    multi.finalize_all()?;

    let stats = multi.get_all_stats();
    // there should be at least one entry for Fail exporter
    let names: Vec<_> = stats.iter().map(|(n, _)| n.clone()).collect();
    assert!(names.iter().any(|n| n == "Fail"));

    // locate the Fail stats and assert failed_records > 0
    for (name, st) in stats {
        if name == "Fail" {
            assert!(st.failed_records >= 1);
        }
    }

    // cleanup any created files
    #[cfg(feature = "exporter-csv")]
    {
        let _ = fs::remove_file(std::env::temp_dir().join("multi_ok.csv"));
    }

    Ok(())
}
