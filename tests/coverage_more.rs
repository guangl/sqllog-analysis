#![cfg(all(
    test,
    any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    )
))]

use std::io::Read;
use tempfile::NamedTempFile;

use sqllog_analysis::exporter::SyncExporter;
use sqllog_analysis::sqllog::types::Sqllog;

// CSV tests require exporter-csv feature
#[cfg(feature = "exporter-csv")]
#[test]
fn test_csv_escape_and_finalize_more()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    // use concrete exporter type
    use sqllog_analysis::exporter::SyncCsvExporter;

    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    // Case 1: finalize without any records should still write header
    {
        let mut exporter = SyncCsvExporter::new(&path)?;
        exporter.finalize()?;

        let mut s = String::new();
        tmp.reopen()?.read_to_string(&mut s)?;
        assert!(
            s.contains("occurrence_time,ep,session"),
            "header must be present"
        );
    }

    // Case 2: export a record with commas/quotes/newlines to exercise escape
    {
        let mut exporter = SyncCsvExporter::new(&path)?;

        let record = Sqllog {
            occurrence_time: "2025-09-24T00:00:00Z".to_string(),
            ep: "EP1".to_string(),
            session: Some("sess,ion".to_string()),
            thread: Some("th".to_string()),
            user: Some("user\"name".to_string()),
            trx_id: None,
            statement: Some("SELECT 1\nFROM dual".to_string()),
            appname: None,
            ip: None,
            sql_type: None,
            description: "desc".to_string(),
            execute_time: None,
            rowcount: None,
            execute_id: None,
        };

        exporter.export_record(&record)?;
        exporter.finalize()?;

        let mut s = String::new();
        tmp.reopen()?.read_to_string(&mut s)?;

        // header and escaped fields
        assert!(s.contains("occurrence_time,ep,session"));
        assert!(
            s.contains("\"sess,ion\""),
            "comma-containing field should be quoted"
        );
        assert!(
            s.contains("\"user\"\"name\""),
            "quote inside field should be doubled and wrapped"
        );
        assert!(
            s.contains("SELECT 1\nFROM dual"),
            "newline in field should be preserved inside quotes"
        );
    }

    Ok(())
}

// JSON tests require exporter-json feature
#[cfg(feature = "exporter-json")]
#[test]
fn test_json_first_record_and_finalize_more()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use sqllog_analysis::exporter::SyncJsonExporter;

    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut exporter = SyncJsonExporter::new(&path)?;

    let r1 = Sqllog {
        occurrence_time: "t1".into(),
        ep: "e".into(),
        ..Default::default()
    };
    let r2 = Sqllog {
        occurrence_time: "t2".into(),
        ep: "e".into(),
        ..Default::default()
    };

    // export two records to force the comma insertion path
    exporter.export_record(&r1)?;
    exporter.export_record(&r2)?;
    exporter.finalize()?;

    let mut s = String::new();
    tmp.reopen()?.read_to_string(&mut s)?;

    assert!(s.starts_with("[\n"));
    // there should be a comma separator between pretty-printed objects
    assert!(
        s.contains(",\n  {"),
        "there must be a comma+newline before second object"
    );
    assert!(s.contains("\n]\n"), "closing bracket must be present");

    Ok(())
}

// Multi exporter and stats tests (multi_exporter is always available)
#[test]
fn test_multi_exporter_stats_and_print()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use sqllog_analysis::exporter::SyncMultiExporter;
    // Add two real exporters using temp files when features are present.
    let mut multi = SyncMultiExporter::new();

    #[cfg(feature = "exporter-csv")]
    {
        use sqllog_analysis::exporter::SyncCsvExporter;
        let tmp = NamedTempFile::new()?;
        let exporter = SyncCsvExporter::new(tmp.path())?;
        multi.add_exporter(exporter);
    }

    #[cfg(feature = "exporter-json")]
    {
        use sqllog_analysis::exporter::SyncJsonExporter;
        let tmp = NamedTempFile::new()?;
        let exporter = SyncJsonExporter::new(tmp.path())?;
        multi.add_exporter(exporter);
    }

    // If no exporters were added, create a trivial exporter by implementing the trait
    if multi.get_all_stats().is_empty() {
        struct SimpleExporter;
        impl sqllog_analysis::exporter::SyncExporter for SimpleExporter {
            fn name(&self) -> &str {
                "SIMPLE"
            }
            fn export_record(
                &mut self,
                _record: &Sqllog,
            ) -> sqllog_analysis::error::Result<()> {
                Ok(())
            }
        }
        multi.add_exporter(SimpleExporter);
    }

    let rec = Sqllog {
        occurrence_time: "t".into(),
        ep: "e".into(),
        ..Default::default()
    };
    multi.export_record(&rec)?;

    multi.finalize_all()?;

    let all = multi.get_all_stats();
    assert!(!all.is_empty());
    // print report to exercise print_stats_report
    multi.print_stats_report();

    Ok(())
}

#[test]
fn test_export_stats_display_and_merge() {
    use std::time::Duration;

    let mut a = sqllog_analysis::exporter::ExportStats::new();
    a.exported_records = 5;
    a.failed_records = 1;
    // ensure duration > 0
    if let Some(start) = a.start_time {
        a.end_time = Some(start + Duration::from_secs(1));
    }

    let s = format!("{}", a);
    assert!(s.contains("成功"));
    assert!(s.contains("耗时") || s.contains("成功率"));

    let mut b = sqllog_analysis::exporter::ExportStats::new();
    b.exported_records = 2;
    b.failed_records = 3;
    if let Some(start) = b.start_time {
        b.end_time = Some(start + Duration::from_secs(2));
    }

    // merge b into a and ensure totals updated
    a.merge(&b);
    assert_eq!(a.exported_records, 7);
    assert_eq!(a.failed_records, 4);
}
