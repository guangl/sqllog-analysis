//! DuckDB export feature integration tests

mod common;

#[cfg(feature = "exporter-duckdb")]
mod duckdb_tests {
    use duckdb::Connection;
    use sqllog_analysis::{
        exporter::SyncExporter,
        exporter::sync_impl::duckdb::SyncDuckdbExporter,
        sqllog::{SyncSqllogParser, types::Sqllog},
    };
    use tempfile::TempDir;

    use super::common;

    #[test]
    fn test_duckdb_basic_export() {
        println!("Testing basic DuckDB export functionality...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let test_file = common::create_test_sqllog(
            &temp_dir,
            "basic_test.log",
            common::SAMPLE_SQLLOG_CONTENT,
        );

        // Parse data
        let mut all_records = Vec::new();
        let mut all_errors = Vec::new();

        let parse_result = SyncSqllogParser::parse_with_hooks(
            &test_file,
            1000,
            |records, errors| {
                all_records.extend_from_slice(records);
                all_errors.extend_from_slice(errors);
            },
        );
        assert!(parse_result.is_ok(), "Parse should succeed");

        // Export to DuckDB
        let db_path = temp_dir.path().join("basic_export.duckdb");
        let mut duckdb_exporter = SyncDuckdbExporter::new(&db_path)
            .expect("Should be able to create DuckDB exporter");

        let export_result = duckdb_exporter.export_batch(&all_records);
        assert!(export_result.is_ok(), "DuckDB export should succeed");

        let finalize_result = duckdb_exporter.finalize();
        assert!(finalize_result.is_ok(), "DuckDB finalization should succeed");

        // Drop the exporter to release the connection
        drop(duckdb_exporter);

        // Verify database content
        assert!(db_path.exists(), "DuckDB database file should exist");

        let conn = Connection::open(&db_path)
            .expect("Should be able to open database");
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM sqllogs").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();

        assert!(count > 0, "Should have at least one record in database");

        // Verify table structure
        let mut stmt = conn
            .prepare(
                "SELECT occurrence_time, ep, description FROM sqllogs LIMIT 1",
            )
            .unwrap();
        let mut rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap();

        if let Some(row) = rows.next() {
            let (time, ep, desc) = row.unwrap();
            assert!(!time.is_empty(), "occurrence_time should not be empty");
            assert!(!ep.is_empty(), "ep should not be empty");
            assert!(!desc.is_empty(), "description should not be empty");
        }

        println!("DuckDB basic export test passed - {} records", count);
    }

    #[test]
    fn test_duckdb_batch_export() {
        println!("Testing DuckDB batch export...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("batch_export.duckdb");
        let mut duckdb_exporter = SyncDuckdbExporter::new(&db_path)
            .expect("Should be able to create DuckDB exporter");

        // Create multiple test records
        let test_records: Vec<Sqllog> = (1..=5)
            .map(|i| Sqllog {
                occurrence_time: format!("2023-09-16 20:02:{:02}", 50 + i),
                ep: format!("{:03}", i),
                thread: Some(format!("{}", i * 10)),
                session: Some(format!("{}", i * 100)),
                user: Some("TEST_USER".to_string()),
                description: format!("Test query number {}", i),
                ..Sqllog::default()
            })
            .collect();

        // Export batch
        let export_result = duckdb_exporter.export_batch(&test_records);
        assert!(export_result.is_ok(), "Batch export should succeed");

        let finalize_result = duckdb_exporter.finalize();
        assert!(finalize_result.is_ok(), "Finalization should succeed");

        // Drop the exporter to release the connection
        drop(duckdb_exporter);

        // Verify content
        let conn = Connection::open(&db_path)
            .expect("Should be able to open database");
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM sqllogs").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(count, 5, "Should have 5 records");

        // Verify record sequence
        let mut stmt =
            conn.prepare("SELECT ep FROM sqllogs ORDER BY ep").unwrap();
        let eps: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        for (i, ep) in eps.iter().enumerate() {
            let expected_ep = format!("{:03}", i + 1);
            assert_eq!(
                ep,
                &expected_ep,
                "EP field should match for record {}",
                i + 1
            );
        }

        println!("DuckDB batch export test passed - 5 records");
    }

    #[test]
    fn test_duckdb_empty_export() {
        println!("Testing DuckDB empty export...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("empty_export.duckdb");
        let mut duckdb_exporter = SyncDuckdbExporter::new(&db_path)
            .expect("Should be able to create DuckDB exporter");

        // Export empty batch
        let empty_records: Vec<Sqllog> = Vec::new();
        let export_result = duckdb_exporter.export_batch(&empty_records);
        assert!(export_result.is_ok(), "Empty export should succeed");

        let finalize_result = duckdb_exporter.finalize();
        assert!(finalize_result.is_ok(), "Finalization should succeed");

        // Drop the exporter to release the connection
        drop(duckdb_exporter);

        // Verify empty database
        let conn = Connection::open(&db_path)
            .expect("Should be able to open database");
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM sqllogs").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(count, 0, "Should have 0 records");

        println!("DuckDB empty export test passed");
    }

    #[test]
    fn test_duckdb_export_stats() {
        println!("Testing DuckDB export statistics...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("stats_export.duckdb");
        let mut duckdb_exporter = SyncDuckdbExporter::new(&db_path)
            .expect("Should be able to create DuckDB exporter");

        // Add some records
        let test_records: Vec<Sqllog> = (1..=3)
            .map(|i| Sqllog {
                occurrence_time: format!("2023-09-16 20:02:{:02}", 50 + i),
                ep: "001".to_string(),
                description: format!("Test statement {}", i),
                ..Sqllog::default()
            })
            .collect();

        duckdb_exporter
            .export_batch(&test_records)
            .expect("Export should succeed");

        // Check stats before finalization
        let stats = duckdb_exporter.get_stats();
        assert_eq!(stats.exported_records, 3, "Should have 3 records in stats");

        duckdb_exporter.finalize().expect("Finalization should succeed");

        println!("DuckDB export stats test passed - 3 records");
    }

    #[test]
    fn test_duckdb_data_integrity() {
        println!("Testing DuckDB data integrity...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("integrity_test.duckdb");
        let mut duckdb_exporter = SyncDuckdbExporter::new(&db_path)
            .expect("Should be able to create DuckDB exporter");

        // Create record with all fields populated
        let complete_record = Sqllog {
            occurrence_time: "2023-09-16 20:02:53".to_string(),
            ep: "001".to_string(),
            session: Some("SESSION_123".to_string()),
            thread: Some("THREAD_456".to_string()),
            user: Some("SYSDBA".to_string()),
            trx_id: Some("TRX_789".to_string()),
            statement: Some("STMT_101".to_string()),
            appname: Some("TEST_APP".to_string()),
            ip: Some("192.168.1.100".to_string()),
            sql_type: Some("SEL".to_string()),
            description: "SELECT * FROM users WHERE id = 1".to_string(),
            execute_time: Some(150),
            rowcount: Some(1),
            execute_id: Some(98765),
        };

        duckdb_exporter
            .export_batch(&[complete_record])
            .expect("Export should succeed");
        duckdb_exporter.finalize().expect("Finalization should succeed");

        // Drop the exporter to release the connection
        drop(duckdb_exporter);

        // Verify all fields are preserved
        let conn = Connection::open(&db_path)
            .expect("Should be able to open database");
        let mut stmt = conn.prepare(
            "SELECT occurrence_time, ep, session, thread, user, sql_type, execute_time, rowcount
             FROM sqllogs LIMIT 1"
        ).unwrap();

        let mut rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                ))
            })
            .unwrap();

        if let Some(row) = rows.next() {
            let (
                time,
                ep,
                session,
                thread,
                user,
                sql_type,
                exec_time,
                rowcount,
            ) = row.unwrap();

            // Verify key fields
            assert_eq!(time, "2023-09-16 20:02:53");
            assert_eq!(ep, "001");
            assert_eq!(session.unwrap(), "SESSION_123");
            assert_eq!(thread.unwrap(), "THREAD_456");
            assert_eq!(user.unwrap(), "SYSDBA");
            assert_eq!(sql_type.unwrap(), "SEL");
            assert_eq!(exec_time.unwrap(), 150);
            assert_eq!(rowcount.unwrap(), 1);

            println!("DuckDB data integrity test passed");
        } else {
            panic!("Should have at least one record");
        }
    }

    #[test]
    fn test_duckdb_export_error_handling() {
        println!("Testing DuckDB export error handling...");

        // Test invalid path (directory that doesn't exist)
        let invalid_path = "/invalid/path/that/does/not/exist/export.duckdb";

        let result =
            SyncDuckdbExporter::new(std::path::Path::new(invalid_path));
        assert!(result.is_err(), "Should fail with invalid path");

        println!("DuckDB export error handling test passed");
    }
}

#[cfg(not(feature = "exporter-duckdb"))]
mod no_duckdb_tests {
    #[test]
    fn test_no_duckdb_feature() {
        println!(
            "No DuckDB export feature test passed - exporter-duckdb feature not enabled"
        );
    }
}
