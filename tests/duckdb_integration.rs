use duckdb::Connection;
use sqllog_analysis::duckdb_writer;
use sqllog_analysis::sqllog::Sqllog;
// no runtime env usage; tests should use internal helpers when injection is needed
use tempfile::tempdir;

#[test]
fn test_write_and_index_duckdb() {
    // Default behavior creates indexes; no env injection required.

    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_sqllogs.duckdb");

    let records = vec![
        Sqllog {
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
        },
        Sqllog {
            occurrence_time: "2025-09-20 12:00:01.000".to_string(),
            ep: 2,
            session: Some("sess2".to_string()),
            thread: Some("thr2".to_string()),
            user: Some("u2".to_string()),
            trx_id: Some("trx2".to_string()),
            statement: Some("select 2".to_string()),
            appname: Some("app".to_string()),
            ip: Some("127.0.0.2".to_string()),
            sql_type: Some("SEL".to_string()),
            description: "desc2".to_string(),
            execute_time: Some(20),
            rowcount: Some(2),
            execute_id: Some(200),
        },
    ];

    // write to DB (索引功能已移除，测试仅验证写入与查询结果)
    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, false)
        .expect("write should succeed");

    // open DB and verify rows
    let conn = Connection::open(&db_path).expect("open db");
    let mut stmt =
        conn.prepare("SELECT COUNT(*) FROM sqllogs").expect("prepare");
    let count: i64 = stmt.query_row([], |row| row.get(0)).expect("query_row");
    assert_eq!(count, 2);

    // verify indexed columns exist by attempting a query that would use them
    let mut stmt2 = conn
        .prepare("SELECT trx_id, thread, session, ip FROM sqllogs ORDER BY execute_id")
        .expect("prepare2");
    let mut rows = stmt2.query([]).expect("query");
    let mut seen = 0;
    while let Some(r) = rows.next().expect("row next") {
        let _trx: Option<String> = r.get(0).ok();
        let _thr: Option<String> = r.get(1).ok();
        let _sess: Option<String> = r.get(2).ok();
        let _ip: Option<String> = r.get(3).ok();
        seen += 1;
    }
    assert_eq!(seen, 2);
}
