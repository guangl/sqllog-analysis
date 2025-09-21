use sqllog_analysis::duckdb_writer;
use sqllog_analysis::sqllog::Sqllog;
// use std::fs; // Removed unused import
use tempfile::tempdir;

#[test]
fn test_write_empty_records_creates_db() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.duckdb");
    let records: Vec<Sqllog> = vec![];
    let res = duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, false);
    assert!(res.is_ok());
    assert!(db_path.exists());
    // cleanup handled by tempdir
}
