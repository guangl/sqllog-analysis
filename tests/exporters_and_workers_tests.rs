#![cfg(any(feature = "exporter-csv", feature = "exporter-json"))]

use sqllog_analysis::exporter::SyncExporter;
use sqllog_analysis::exporter::sync_impl::SyncCsvExporter;
#[cfg(feature = "exporter-json")]
use sqllog_analysis::exporter::sync_impl::SyncJsonExporter;

use sqllog_analysis::error::Result;
use sqllog_analysis::sqllog::types::Sqllog;

use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::mpsc;

#[test]
fn test_csv_exporter_writes_header_and_records() -> Result<()> {
    let tmp = std::env::temp_dir().join("test_exporter.csv");
    let _ = fs::remove_file(&tmp);

    let mut exporter = SyncCsvExporter::new(&tmp)?;

    let record = Sqllog {
        occurrence_time: "2025-01-01T00:00:00Z".to_string(),
        ep: "ep1".to_string(),
        session: Some("s1".to_string()),
        thread: Some("t1".to_string()),
        user: Some("u1".to_string()),
        trx_id: Some("trx".to_string()),
        statement: Some("select 1".to_string()),
        appname: Some("app".to_string()),
        ip: Some("127.0.0.1".to_string()),
        sql_type: Some("QUERY".to_string()),
        description: "desc".to_string(),
        execute_time: Some(10),
        rowcount: Some(1),
        execute_id: Some(99),
    };

    exporter.export_record(&record)?;
    exporter.finalize()?;

    let mut s = String::new();
    fs::File::open(&tmp)?.read_to_string(&mut s)?;
    assert!(s.contains("occurrence_time,ep,session"));
    assert!(s.contains("select 1"));

    // cleanup
    let _ = fs::remove_file(&tmp);
    Ok(())
}

#[cfg(feature = "exporter-json")]
#[test]
fn test_json_exporter_writes_array_and_records() -> Result<()> {
    let tmp = std::env::temp_dir().join("test_exporter.json");
    let _ = fs::remove_file(&tmp);

    let mut exporter = SyncJsonExporter::new(&tmp)?;

    let record = Sqllog {
        occurrence_time: "2025-01-01T00:00:00Z".to_string(),
        ep: "ep1".to_string(),
        session: None,
        thread: None,
        user: None,
        trx_id: None,
        statement: None,
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
    fs::File::open(&tmp)?.read_to_string(&mut s)?;
    assert!(s.trim_start().starts_with("["));
    assert!(s.contains("occurrence_time"));

    let _ = fs::remove_file(&tmp);
    Ok(())
}

// Test export_worker behavior: it should send stats on success
#[test]
fn test_export_worker_sends_stats_on_success() -> Result<()> {
    // Use CSV exporter for the worker
    let tmp = std::env::temp_dir().join("worker_test.csv");
    let _ = fs::remove_file(&tmp);
    let exporter = SyncCsvExporter::new(&tmp)?;

    let (task_tx, task_rx) = mpsc::channel();
    let (res_tx, res_rx) = mpsc::channel();

    let exporter_thread = std::thread::spawn(move || {
        let tasks = task_rx;
        let _ =
            sqllog_analysis::sqllog::concurrent::export_workers::export_worker(
                1, exporter, tasks, res_tx,
            );
    });

    // send one export task
    let task = sqllog_analysis::sqllog::concurrent::types::ExportTask {
        records: vec![Sqllog {
            occurrence_time: "t".to_string(),
            ep: "e".to_string(),
            session: None,
            thread: None,
            user: None,
            trx_id: None,
            statement: None,
            appname: None,
            ip: None,
            sql_type: None,
            description: "d".to_string(),
            execute_time: None,
            rowcount: None,
            execute_id: None,
        }],
        task_id: 42,
        source_file: PathBuf::from("/tmp/x"),
    };

    task_tx.send(task).unwrap();
    // close the sender so worker exits after processing
    drop(task_tx);

    // receive stats
    let stats = res_rx.recv().unwrap();
    assert_eq!(stats.exported_records, 1);

    exporter_thread.join().unwrap();

    let _ = fs::remove_file(&tmp);
    Ok(())
}
