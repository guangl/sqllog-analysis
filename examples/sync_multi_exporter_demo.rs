//! Sync Multi-Exporter Demo
//!
//! Demonstrates synchronous export to multiple formats (CSV, JSON, SQLite)
//! with sync processing and statistics tracking.

fn run_demo() -> Result<(), Box<dyn std::error::Error>> {
    use sqllog_analysis::prelude::*;

    use std::time::Instant;

    println!("=== Sync Multi-Exporter Demo ===");

    // Create output directory
    let output_dir = std::path::Path::new("output_sync");
    std::fs::create_dir_all(output_dir)?;

    // Setup sync multi-exporter
    let mut multi_exporter = SyncMultiExporter::new();

    println!("Setting up sync exporters:");

    // Add CSV exporter
    #[cfg(feature = "exporter-csv")]
    {
        let csv_path = output_dir.join("output_sync.csv");
        let csv_exporter = SyncCsvExporter::new(csv_path)?;
        multi_exporter.add_exporter(csv_exporter);
        println!("  - Sync CSV exporter");
    }

    // Add JSON exporter
    #[cfg(feature = "exporter-json")]
    {
        let json_path = output_dir.join("output_sync.json");
        let json_exporter = SyncJsonExporter::new(json_path)?;
        multi_exporter.add_exporter(json_exporter);
        println!("  - Sync JSON exporter");
    }

    // Add SQLite exporter
    #[cfg(feature = "exporter-sqlite")]
    {
        let sqlite_path = output_dir.join("output_sync.db");
        let sqlite_exporter = SyncSqliteExporter::new(&sqlite_path)?;
        multi_exporter.add_exporter(sqlite_exporter);
        println!("  - Sync SQLite exporter");
    }

    println!("Starting sync parse and export...");

    // Create test data
    let test_file_path = create_test_data(&output_dir)?;

    // Parse with sync processing and export
    let start_time = Instant::now();

    let mut total_records = 0usize;
    let mut total_errors = 0usize;

    SyncSqllogParser::parse_with_hooks(
        &test_file_path,
        100,
        |records, errors| {
            // Export records
            if let Err(e) = multi_exporter.export_batch(&records) {
                eprintln!("Export error: {}", e);
            }

            // Handle errors
            if !errors.is_empty() {
                println!("Parse warnings: {} errors", errors.len());
                for (i, error) in errors.iter().enumerate().take(3) {
                    println!("    {}: {:?}", i + 1, error);
                }
                if errors.len() > 3 {
                    println!("    ... and {} more errors", errors.len() - 3);
                }
            }

            // Update counters
            total_records += records.len();
            total_errors += errors.len();
        },
    )?;

    // Finalize all exporters
    multi_exporter.finalize_all()?;
    let final_stats = multi_exporter.get_all_stats();

    let elapsed = start_time.elapsed();

    // Display results
    println!("Sync export completed:");
    println!("  Total records: {}", total_records);
    println!("  Total errors: {}", total_errors);
    println!("  Total time: {:.2}s", elapsed.as_secs_f64());
    if total_records > 0 {
        println!(
            "  Average speed: {:.2} records/sec",
            total_records as f64 / elapsed.as_secs_f64()
        );
    }

    // Show per-exporter statistics
    println!("Per-exporter statistics:");
    for (name, stats) in final_stats {
        println!("  {}: {} records exported", name, stats.exported_records);
    }

    println!("Generated files:");
    for entry in std::fs::read_dir(output_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let path = entry.path();
            let metadata = std::fs::metadata(&path)?;
            println!("  {} ({} bytes)", path.display(), metadata.len());
        }
    }

    Ok(())
}

fn create_test_data(
    output_dir: &std::path::Path,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let test_file_path = output_dir.join("test_data_sync.log");

    // Create sample log data
    let sample_data = r#"2024-09-23 10:00:00.123 (EP[1] sess:0x12345 thrd:101 user:admin trxid:1001 stmt:0x11111) [SEL]: SELECT * FROM users WHERE active = 1;
2024-09-23 10:00:00.156 (EP[1] sess:0x12346 thrd:102 user:user1 trxid:1002 stmt:0x11112) [UPD]: UPDATE products SET price = 29.99 WHERE product_id = 123;
2024-09-23 10:00:00.250 (EP[2] sess:0x12347 thrd:103 user:user1 trxid:1003 stmt:0x11113) [INS]: INSERT INTO logs (user_id, action, timestamp) VALUES (1, 'login', CURRENT_TIMESTAMP);
2024-09-23 10:00:00.289 (EP[1] sess:0x12348 thrd:104 user:user1 trxid:1004 stmt:0x11114) [DEL]: DELETE FROM temp_cache WHERE created_at < '2024-09-22';
2024-09-23 10:00:00.380 (EP[2] sess:0x12348 thrd:104 user:user1 trxid:1004 stmt:0x11114) [SEL]: SELECT COUNT(*) FROM products WHERE category = 'electronics';
2024-09-23 10:00:00.456 (EP[1] sess:0x12349 thrd:105 user:admin trxid:1005 stmt:0x11115) [UPD]: UPDATE user_sessions SET last_active = CURRENT_TIMESTAMP WHERE session_id = 'abc123';
2024-09-23 10:00:00.620 (EP[1] sess:0x1234a thrd:106 user:admin trxid:1006 stmt:0x11116) [SEL]: SELECT u.username, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE p.status = 'published';
2024-09-23 10:00:00.789 (EP[2] sess:0x1234b thrd:107 user:user2 trxid:1007 stmt:0x11117) [INS]: INSERT INTO orders (user_id, product_id, quantity, total_price) VALUES (2, 456, 2, 59.98);
2024-09-23 10:00:00.890 (EP[1] sess:0x1234c thrd:108 user:user2 trxid:1008 stmt:0x11118) [SEL]: SELECT o.*, p.name FROM orders o JOIN products p ON o.product_id = p.id WHERE o.user_id = 2;
2024-09-23 10:00:01.010 (EP[1] sess:0x1234d thrd:109 user:admin trxid:1009 stmt:0x11119) [INS]: INSERT INTO audit_log (user_id, action, details, timestamp) VALUES (1, 'data_export', 'Exported user data', CURRENT_TIMESTAMP);"#;

    std::fs::write(&test_file_path, sample_data)?;
    Ok(test_file_path)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_demo()
}
