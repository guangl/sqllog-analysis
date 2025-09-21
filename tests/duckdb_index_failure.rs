use sqllog_analysis::duckdb_writer;
use sqllog_analysis::sqllog::Sqllog;
use tempfile::tempdir;

#[test]
fn test_index_creation_failure_reports_error() {
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

    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records)
        .expect("write should succeed");
}
