use sqllog_analysis::duckdb_writer;
use sqllog_analysis::sqllog::Sqllog;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_export_csv_json_xlsx() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_export.duckdb");

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

    // write to DB
    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records).expect("write");

    // export csv
    let csv_path = dir.path().join("out.csv");
    duckdb_writer::export_sqllogs_to_file(&db_path, &csv_path, "csv")
        .expect("export csv");
    assert!(csv_path.exists());
    let meta = fs::metadata(&csv_path).expect("meta");
    assert!(meta.len() > 0);

    // export json
    let json_path = dir.path().join("out.json");
    duckdb_writer::export_sqllogs_to_file(&db_path, &json_path, "json")
        .expect("export json");
    assert!(json_path.exists());
    let meta = fs::metadata(&json_path).expect("meta");
    assert!(meta.len() > 0);

    // Note: XLSX export depends on DuckDB build; not asserted here.
}
