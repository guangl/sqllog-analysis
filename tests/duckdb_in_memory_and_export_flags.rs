use duckdb::Connection;
use sqllog_analysis::config::{ExportOptions, WriteFlags};
use sqllog_analysis::duckdb_writer;
use sqllog_analysis::sqllog::Sqllog;
use tempfile::tempdir;

#[test]
fn test_write_via_in_memory_export() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_in_memory.duckdb");

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

    // Use the in-memory + ATTACH path
    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, true)
        .expect("in-memory write should succeed");

    // With the new in-memory-only behavior we do not create a table on disk.
    // Ensure the on-disk DB does NOT contain the sqllogs table (previously
    // this test expected CTAS behavior). The expected behavior is that data
    // was written only to the in-memory connection and not exported to disk.
    let conn = Connection::open(&db_path).expect("open db");
    let res = conn.prepare("SELECT COUNT(*) FROM sqllogs");
    assert!(
        res.is_err(),
        "sqllogs table should not exist on disk when using in-memory export"
    );
}

#[test]
fn test_export_per_thread_out() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_export_per_thread.duckdb");

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

    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, false)
        .expect("write to disk should succeed");

    let opts = ExportOptions {
        per_thread_out: true,
        write_flags: WriteFlags {
            overwrite_or_ignore: false,
            overwrite: false,
            append: false,
        },
        file_size_bytes: None,
    };
    let out_path = dir.path().join("out_per_thread.csv");
    duckdb_writer::export_sqllogs_to_file_with_flags(
        &db_path, &out_path, "csv", &opts,
    )
    .expect("export per_thread_out");
    assert!(out_path.exists());
}

#[test]
fn test_export_overwrite_or_ignore() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_export_overwrite_or_ignore.duckdb");

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

    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, false)
        .expect("write to disk");
    let opts = ExportOptions {
        per_thread_out: false,
        write_flags: WriteFlags {
            overwrite_or_ignore: true,
            overwrite: false,
            append: false,
        },
        file_size_bytes: None,
    };
    let out_path = dir.path().join("out_overwrite_or_ignore.csv");
    duckdb_writer::export_sqllogs_to_file_with_flags(
        &db_path, &out_path, "csv", &opts,
    )
    .expect("export overwrite_or_ignore");
    assert!(out_path.exists());
}

#[test]
fn test_export_overwrite() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_export_overwrite.duckdb");

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

    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, false)
        .expect("write to disk");
    let opts = ExportOptions {
        per_thread_out: false,
        write_flags: WriteFlags {
            overwrite_or_ignore: false,
            overwrite: true,
            append: false,
        },
        file_size_bytes: None,
    };
    let out_path = dir.path().join("out_overwrite.csv");
    duckdb_writer::export_sqllogs_to_file_with_flags(
        &db_path, &out_path, "csv", &opts,
    )
    .expect("export overwrite");
    assert!(out_path.exists());
}

#[test]
fn test_export_append_and_file_size() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_export_append.duckdb");

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

    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, false)
        .expect("write to disk");
    let opts = ExportOptions {
        per_thread_out: false,
        write_flags: WriteFlags {
            overwrite_or_ignore: false,
            overwrite: false,
            append: true,
        },
        file_size_bytes: Some(1024),
    };
    let out_path = dir.path().join("out_append.csv");
    duckdb_writer::export_sqllogs_to_file_with_flags(
        &db_path, &out_path, "csv", &opts,
    )
    .expect("export append and file_size");
    assert!(out_path.exists());
}

// Additional tests to increase branch coverage
#[test]
fn test_write_empty_records_direct_and_in_memory() {
    let dir = tempdir().expect("tempdir");
    let db_path_direct = dir.path().join("test_empty_direct.duckdb");
    let db_path_mem = dir.path().join("test_empty_mem.duckdb");

    let empty: Vec<Sqllog> = vec![];

    // direct write should succeed with zero rows
    duckdb_writer::write_sqllogs_to_duckdb(&db_path_direct, &empty, false)
        .expect("direct empty write");
    let conn = Connection::open(&db_path_direct).expect("open");
    let mut stmt =
        conn.prepare("SELECT COUNT(*) FROM sqllogs").expect("prepare");
    let count: i64 = stmt.query_row([], |row| row.get(0)).expect("query_row");
    assert_eq!(count, 0);

    // in-memory route should NOT create a table on disk anymore. Verify disk
    // DB has no sqllogs table; the direct write above covers the disk case.
    duckdb_writer::write_sqllogs_to_duckdb(&db_path_mem, &empty, true)
        .expect("in-memory empty write");
    let conn2 = Connection::open(&db_path_mem).expect("open");
    let res2 = conn2.prepare("SELECT COUNT(*) FROM sqllogs");
    assert!(
        res2.is_err(),
        "sqllogs table should not exist on disk for in-memory writes"
    );
}

#[test]
fn test_write_multiple_records_direct() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_multi_direct.duckdb");

    let records = vec![
        Sqllog {
            occurrence_time: "2025-09-20 12:00:00.000".to_string(),
            ep: 1,
            session: None,
            thread: None,
            user: None,
            trx_id: None,
            statement: Some("select 1".to_string()),
            appname: None,
            ip: None,
            sql_type: None,
            description: "d1".to_string(),
            execute_time: Some(5),
            rowcount: Some(1),
            execute_id: Some(1),
        },
        Sqllog {
            occurrence_time: "2025-09-20 12:01:00.000".to_string(),
            ep: 2,
            session: None,
            thread: None,
            user: None,
            trx_id: None,
            statement: Some("select 2".to_string()),
            appname: None,
            ip: None,
            sql_type: None,
            description: "d2".to_string(),
            execute_time: Some(6),
            rowcount: Some(2),
            execute_id: Some(2),
        },
    ];

    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, false)
        .expect("write multi direct");
    let conn = Connection::open(&db_path).expect("open");
    let mut stmt =
        conn.prepare("SELECT COUNT(*) FROM sqllogs").expect("prepare");
    let count: i64 = stmt.query_row([], |row| row.get(0)).expect("query_row");
    assert_eq!(count, 2);
}

#[test]
fn test_export_conflicting_write_flags_errors() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_conflict_flags.duckdb");

    let records = vec![Sqllog {
        occurrence_time: "2025-09-20 12:00:00.000".to_string(),
        ep: 1,
        session: None,
        thread: None,
        user: None,
        trx_id: None,
        statement: Some("select 1".to_string()),
        appname: None,
        ip: None,
        sql_type: None,
        description: "d".to_string(),
        execute_time: None,
        rowcount: None,
        execute_id: None,
    }];

    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, false)
        .expect("write for conflict test");

    // Set multiple mutually-exclusive flags; library should return Err
    let opts = ExportOptions {
        per_thread_out: false,
        write_flags: WriteFlags {
            overwrite_or_ignore: true,
            overwrite: true,
            append: true,
        },
        file_size_bytes: None,
    };
    let out_path = dir.path().join("out_conflict.csv");
    let res = duckdb_writer::export_sqllogs_to_file_with_flags(
        &db_path, &out_path, "csv", &opts,
    );
    assert!(
        res.is_err(),
        "expected error when multiple mutually-exclusive write flags are set"
    );
}

#[test]
fn test_export_default_helper_and_unsupported_format() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_default_export.duckdb");

    let records = vec![Sqllog {
        occurrence_time: "2025-09-20 12:00:00.000".to_string(),
        ep: 1,
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
    }];

    // write and use default export helper (which uses CSV by default)
    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, false)
        .expect("write for default export");
    let out = dir.path().join("default_out.csv");
    duckdb_writer::export_sqllogs_to_file(&db_path, &out, "csv")
        .expect("default export");
    assert!(out.exists());

    // unsupported format should return Err
    let bad = duckdb_writer::export_sqllogs_to_file(
        &db_path,
        &dir.path().join("bad.out"),
        "bin",
    );
    assert!(bad.is_err());
}

#[test]
fn test_export_path_with_single_quote_and_json_format() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_quote.duckdb");

    let records = vec![Sqllog {
        occurrence_time: "2025-09-20 12:00:00.000".to_string(),
        ep: 1,
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
    }];

    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, false)
        .expect("write for quote test");

    // include a single quote in filename to exercise escaping logic
    let out = dir.path().join("out'quote.json");
    duckdb_writer::export_sqllogs_to_file_with_flags(
        &db_path,
        &out,
        "json",
        &ExportOptions {
            per_thread_out: false,
            write_flags: WriteFlags {
                overwrite_or_ignore: false,
                overwrite: false,
                append: false,
            },
            file_size_bytes: None,
        },
    )
    .expect("export json with quote");
    assert!(out.exists());
}
// include a single quote in filename to exercise escaping logic

#[test]
fn test_export_very_large_file_size() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_size_large.duckdb");

    let records = vec![Sqllog {
        occurrence_time: "2025-09-20 12:00:00.000".to_string(),
        ep: 1,
        session: None,
        thread: None,
        user: None,
        trx_id: None,
        statement: Some("select 1".to_string()),
        appname: None,
        ip: None,
        sql_type: None,
        description: "d".to_string(),
        execute_time: None,
        rowcount: None,
        execute_id: None,
    }];
    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &records, false)
        .expect("write");

    // Extremely large file_size_bytes should still succeed and not panic
    let opts = ExportOptions {
        per_thread_out: false,
        write_flags: WriteFlags {
            overwrite_or_ignore: false,
            overwrite: false,
            append: false,
        },
        file_size_bytes: Some(10_000_000_000u64),
    };
    let out = dir.path().join("out_size_large.csv");
    duckdb_writer::export_sqllogs_to_file_with_flags(
        &db_path, &out, "csv", &opts,
    )
    .expect("export large size");
    assert!(out.exists());
}

#[test]
fn test_append_export_appends_file() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("test_append.duckdb");

    // first write and export one record
    let rec1 = vec![Sqllog {
        occurrence_time: "2025-09-20 12:00:00.000".to_string(),
        ep: 1,
        session: None,
        thread: None,
        user: None,
        trx_id: None,
        statement: Some("select 1".to_string()),
        appname: None,
        ip: None,
        sql_type: None,
        description: "r1".to_string(),
        execute_time: None,
        rowcount: None,
        execute_id: None,
    }];
    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &rec1, false)
        .expect("write1");
    let out = dir.path().join("out_append_test.csv");
    duckdb_writer::export_sqllogs_to_file_with_flags(
        &db_path,
        &out,
        "csv",
        &ExportOptions {
            per_thread_out: false,
            write_flags: WriteFlags {
                overwrite_or_ignore: false,
                overwrite: false,
                append: false,
            },
            file_size_bytes: None,
        },
    )
    .expect("export1");

    // then write a second record and append
    let rec2 = vec![Sqllog {
        occurrence_time: "2025-09-20 12:02:00.000".to_string(),
        ep: 2,
        session: None,
        thread: None,
        user: None,
        trx_id: None,
        statement: Some("select 2".to_string()),
        appname: None,
        ip: None,
        sql_type: None,
        description: "r2".to_string(),
        execute_time: None,
        rowcount: None,
        execute_id: None,
    }];
    duckdb_writer::write_sqllogs_to_duckdb(&db_path, &rec2, false)
        .expect("write2");
    duckdb_writer::export_sqllogs_to_file_with_flags(
        &db_path,
        &out,
        "csv",
        &ExportOptions {
            per_thread_out: false,
            write_flags: WriteFlags {
                overwrite_or_ignore: false,
                overwrite: false,
                append: true,
            },
            file_size_bytes: None,
        },
    )
    .expect("export_append");

    // file should exist and be non-empty
    assert!(out.exists());
    let metadata = std::fs::metadata(&out).expect("metadata");
    assert!(metadata.len() > 0);
}
