//! JSON export feature integration tests

mod common;

#[cfg(feature = "exporter-json")]
mod json_tests {
    use sqllog_analysis::{
        exporter::SyncExporter,
        exporter::sync_impl::json::SyncJsonExporter,
        sqllog::{SyncSqllogParser, types::Sqllog},
    };
    use std::fs;
    use tempfile::TempDir;

    use super::common;

    #[test]
    fn test_json_basic_export() {
        println!("Testing basic JSON export functionality...");

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

        // Export to JSON
        let json_path = temp_dir.path().join("basic_export.json");
        let mut json_exporter = SyncJsonExporter::new(&json_path)
            .expect("Should be able to create JSON exporter");

        let export_result = json_exporter.export_batch(&all_records);
        assert!(export_result.is_ok(), "JSON export should succeed");

        let finalize_result = json_exporter.finalize();
        assert!(finalize_result.is_ok(), "JSON finalization should succeed");

        // Verify file exists and parse JSON content
        assert!(json_path.exists(), "JSON file should exist");
        let content = fs::read_to_string(&json_path)
            .expect("Should be able to read JSON file");
        let parsed: serde_json::Value =
            serde_json::from_str(&content).expect("Should be valid JSON");

        if let serde_json::Value::Array(arr) = parsed {
            assert!(!arr.is_empty(), "Should have at least one record");
            let first_record = &arr[0];
            assert!(first_record.get("occurrence_time").is_some());
            assert!(first_record.get("ep").is_some());
            assert!(first_record.get("description").is_some());
            println!("JSON basic export test passed - {} records", arr.len());
        } else {
            panic!("JSON should be an array");
        }
    }

    #[test]
    fn test_json_batch_export() {
        println!("Testing JSON batch export...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let json_path = temp_dir.path().join("batch_export.json");
        let mut json_exporter = SyncJsonExporter::new(&json_path)
            .expect("Should be able to create JSON exporter");

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
        let export_result = json_exporter.export_batch(&test_records);
        assert!(export_result.is_ok(), "Batch export should succeed");

        let finalize_result = json_exporter.finalize();
        assert!(finalize_result.is_ok(), "Finalization should succeed");

        // Verify content
        let content = fs::read_to_string(&json_path)
            .expect("Should be able to read JSON");
        let parsed: serde_json::Value =
            serde_json::from_str(&content).expect("Should be valid JSON");

        if let serde_json::Value::Array(arr) = parsed {
            assert_eq!(arr.len(), 5, "Should have 5 records");

            // Verify record sequence
            for (i, record) in arr.iter().enumerate() {
                let expected_ep = format!("{:03}", i + 1);
                assert_eq!(
                    record["ep"].as_str().unwrap(),
                    expected_ep,
                    "EP field should match for record {}",
                    i + 1
                );
            }

            println!("JSON batch export test passed - 5 records");
        } else {
            panic!("JSON should be an array");
        }
    }

    #[test]
    fn test_json_empty_export() {
        println!("Testing JSON empty export...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let json_path = temp_dir.path().join("empty_export.json");
        let mut json_exporter = SyncJsonExporter::new(&json_path)
            .expect("Should be able to create JSON exporter");

        // Export empty batch
        let empty_records: Vec<Sqllog> = Vec::new();
        let export_result = json_exporter.export_batch(&empty_records);
        assert!(export_result.is_ok(), "Empty export should succeed");

        let finalize_result = json_exporter.finalize();
        assert!(finalize_result.is_ok(), "Finalization should succeed");

        // Verify empty array
        let content = fs::read_to_string(&json_path)
            .expect("Should be able to read JSON");
        let parsed: serde_json::Value =
            serde_json::from_str(&content).expect("Should be valid JSON");

        if let serde_json::Value::Array(arr) = parsed {
            assert_eq!(arr.len(), 0, "Should have 0 records");
            println!("JSON empty export test passed");
        } else {
            panic!("JSON should be an array");
        }
    }

    #[test]
    fn test_json_export_stats() {
        println!("Testing JSON export statistics...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let json_path = temp_dir.path().join("stats_export.json");
        let mut json_exporter = SyncJsonExporter::new(&json_path)
            .expect("Should be able to create JSON exporter");

        // Add some records
        let test_records: Vec<Sqllog> = (1..=3)
            .map(|i| Sqllog {
                occurrence_time: format!("2023-09-16 20:02:{:02}", 50 + i),
                ep: "001".to_string(),
                description: format!("Test statement {}", i),
                ..Sqllog::default()
            })
            .collect();

        json_exporter
            .export_batch(&test_records)
            .expect("Export should succeed");

        // Check stats before finalization
        let stats = json_exporter.get_stats();
        assert_eq!(stats.exported_records, 3, "Should have 3 records in stats");

        json_exporter.finalize().expect("Finalization should succeed");

        println!("JSON export stats test passed - 3 records");
    }

    #[test]
    fn test_json_data_integrity() {
        println!("Testing JSON data integrity...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let json_path = temp_dir.path().join("integrity_test.json");
        let mut json_exporter = SyncJsonExporter::new(&json_path)
            .expect("Should be able to create JSON exporter");

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

        json_exporter
            .export_batch(&[complete_record])
            .expect("Export should succeed");
        json_exporter.finalize().expect("Finalization should succeed");

        // Verify all fields are preserved
        let content = fs::read_to_string(&json_path)
            .expect("Should be able to read JSON");
        let parsed: serde_json::Value =
            serde_json::from_str(&content).expect("Should be valid JSON");

        if let serde_json::Value::Array(arr) = parsed {
            assert_eq!(arr.len(), 1, "Should have 1 record");

            let record = &arr[0];

            // Verify key fields
            assert_eq!(
                record["occurrence_time"].as_str().unwrap(),
                "2023-09-16 20:02:53"
            );
            assert_eq!(record["ep"].as_str().unwrap(), "001");
            assert_eq!(record["session"].as_str().unwrap(), "SESSION_123");
            assert_eq!(record["thread"].as_str().unwrap(), "THREAD_456");
            assert_eq!(record["user"].as_str().unwrap(), "SYSDBA");
            assert_eq!(record["sql_type"].as_str().unwrap(), "SEL");
            assert_eq!(record["execute_time"].as_i64().unwrap(), 150);
            assert_eq!(record["rowcount"].as_i64().unwrap(), 1);

            println!("JSON data integrity test passed");
        } else {
            panic!("JSON should be an array");
        }
    }

    #[test]
    fn test_json_export_error_handling() {
        println!("Testing JSON export error handling...");

        // Test invalid path
        let invalid_path = "/invalid/path/that/does/not/exist/export.json";

        let result = SyncJsonExporter::new(invalid_path);
        assert!(result.is_err(), "Should fail with invalid path");

        println!("JSON export error handling test passed");
    }
}

#[cfg(not(feature = "exporter-json"))]
mod no_json_tests {
    #[test]
    fn test_no_json_feature() {
        println!(
            "No JSON export feature test passed - exporter-json feature not enabled"
        );
    }
}
