#![cfg(any(feature = "exporter-csv", feature = "exporter-json"))]

use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use sqllog_analysis::exporter::{ExportStats, SyncExporter, SyncMultiExporter};
use sqllog_analysis::Sqllog;

/// A simple dummy exporter used for tests. Can be configured to fail on record or finalize.
struct DummyExporter {
    name: String,
    fail_record: bool,
    fail_finalize: bool,
}

impl DummyExporter {
    fn new(name: &str, fail_record: bool, fail_finalize: bool) -> Self {
        Self {
            name: name.to_string(),
            fail_record,
            fail_finalize,
        }
    }
}

impl SyncExporter for DummyExporter {
    fn name(&self) -> &str {
        &self.name
    }

    fn export_record(&mut self, _record: &Sqllog) -> sqllog_analysis::Result<()> {
        if self.fail_record {
            Err(sqllog_analysis::SqllogError::other("dummy fail"))
        } else {
            Ok(())
        }
    }

    fn finalize(&mut self) -> sqllog_analysis::Result<()> {
        if self.fail_finalize {
            Err(sqllog_analysis::SqllogError::other("finalize fail"))
        } else {
            Ok(())
        }
    }

    fn get_stats(&self) -> ExportStats {
        ExportStats::default()
    }
}

#[test]
fn test_sync_multi_exporter_success_and_failure() {
    let mut multi = SyncMultiExporter::new();

    // exporter 0: succeeds
    multi.add_exporter(DummyExporter::new("ok", false, false));

    // exporter 1: fails on record and finalize
    multi.add_exporter(DummyExporter::new("bad", true, true));

    let mut r = Sqllog::new();
    r.occurrence_time = "t".into();
    r.ep = "ep".into();
    r.description = "desc".into();

    // single record: ok -> exporter 0 increments exported, exporter 1 increments failed
    multi.export_record(&r).unwrap();
    let stats = multi.get_all_stats();
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].1.exported_records, 1);
    assert!(stats[0].1.failed_records == 0);
    assert_eq!(stats[1].1.failed_records, 1);

    // batch of two - should apply same behavior for each record
    multi.export_batch(&[r.clone(), r.clone()]).unwrap();
    let stats = multi.get_all_stats();
    // exporter 0: previously 1 + 2
    assert_eq!(stats[0].1.exported_records, 3);
    // exporter 1: previously failed 1 + 2
    assert_eq!(stats[1].1.failed_records, 3);

    // finalize: exporter 0 finish() should set end_time, exporter 1 finalize will Err and increment failed_records
    multi.finalize_all().unwrap();
    let stats = multi.get_all_stats();
    // exporter 0 should have end_time set via finish()
    assert!(stats[0].1.end_time.is_some());
    // exporter 1 should have at least previous failed count (and possibly increased due to finalize failure)
    assert!(stats[1].1.failed_records >= 3);
}

#[test]
fn test_export_worker_sends_stats() {
    use sqllog_analysis::sqllog::concurrent::types::ExportTask;
    use sqllog_analysis::sqllog::concurrent::export_workers::export_worker;

    // exporter that succeeds on batch
    struct BatchOkExporter;
    impl SyncExporter for BatchOkExporter {
        fn name(&self) -> &str { "batch_ok" }
        fn export_record(&mut self, _record: &Sqllog) -> sqllog_analysis::Result<()> { Ok(()) }
        // use default export_batch which calls export_record
        fn finalize(&mut self) -> sqllog_analysis::Result<()> { Ok(()) }
    }

    let (task_tx, task_rx) = mpsc::channel::<ExportTask>();
    let (result_tx, result_rx) = mpsc::channel::<ExportStats>();

    // spawn worker thread
    let handle = thread::spawn(move || {
        let exporter = BatchOkExporter;
        export_worker(0usize, exporter, task_rx, result_tx).unwrap();
    });

    // prepare a single-task with one record
    let mut r = Sqllog::new();
    r.occurrence_time = "t".into();
    r.ep = "ep".into();
    r.description = "d".into();

    let task = ExportTask {
        records: vec![r],
        task_id: 1,
        source_file: PathBuf::from("test"),
    };

    task_tx.send(task).unwrap();
    // close channel to let worker exit after processing
    drop(task_tx);

    // receive stats sent by worker
    let stats = result_rx.recv().expect("should receive stats");
    assert_eq!(stats.exported_records, 1);

    // worker should exit and thread join
    handle.join().expect("worker thread joined");
}
