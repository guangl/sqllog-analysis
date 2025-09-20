#![cfg(feature = "test-helpers")]

use sqllog_analysis::duckdb_writer;
use sqllog_analysis::sqllog::Sqllog;
use tempfile::tempdir;
use sqllog_analysis::duckdb_writer::set_inject_bad_index;

#[test]
fn test_index_creation_failure_reports_error() {
    // instruct writer to inject a bad index statement via test helper
    set_inject_bad_index(true);

    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_sqllogs_bad.duckdb");

    let records = vec![Sqllog {
        occurrence_time: "2025-09-20 12:00:00.000".to_string(),
        ep: 1,
        session: Some("sess1".to_string()),
        thread: Some("thr1".to_string()),
        user: Some("u1".to_string()),
        trx_id: Some("trx1".to_string()),
        statement: Some("select 1".to_string()),
        appname: Some("app".to_string()),
        ip: Some("127.0.0.1".to_string()),
        sql_type: Some("SEL".to_string()),
        description: "desc".to_string(),
        execute_time: Some(10),
        rowcount: Some(1),
        execute_id: Some(100),
    }];

    let reports =
        duckdb_writer::write_sqllogs_to_duckdb_with_chunk_and_report(&db_path, &records, 1, true)
            .expect("write should succeed");

    // we expect at least one report entry to contain an error due to injection
    let any_error = reports.iter().any(|r| r.error.is_some());
    assert!(
        any_error,
        "expected at least one index creation to fail and be reported"
    );
}
