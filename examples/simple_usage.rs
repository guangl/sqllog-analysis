//! Simple Usage Examples
//!
//! Demonstrates basic usage of both sync and async exporters

// Synchronous example
fn sync_example() -> Result<(), Box<dyn std::error::Error>> {
    use sqllog_analysis::prelude::*;

    println!("=== Synchronous Export Example ===");

    // Create a sync CSV exporter
    #[cfg(feature = "exporter-csv")]
    {
        let mut csv_exporter = SyncCsvExporter::new("sync_output.csv")?;

        // Create sample data
        let record = Sqllog {
            occurrence_time: "2024-09-23 10:00:00.123".to_string(),
            ep: "1".to_string(),
            session: Some("0x12345".to_string()),
            user: Some("admin".to_string()),
            sql_type: Some("SEL".to_string()),
            description: "SELECT * FROM users".to_string(),
            ..Default::default()
        };

        // Export single record
        csv_exporter.export_record(&record)?;

        // Finalize export
        csv_exporter.finalize()?;

        println!("Sync CSV export completed!");
    }

    Ok(())
}

// Asynchronous example
#[cfg(feature = "async")]
async fn async_example() -> Result<(), Box<dyn std::error::Error>> {
    use sqllog_analysis::prelude::*;

    println!("=== Asynchronous Export Example ===");

    // Create an async JSON exporter
    #[cfg(feature = "exporter-json")]
    {
        let mut json_exporter = JsonExporter::new("async_output.json").await?;

        // Create sample data
        let record = Sqllog {
            occurrence_time: "2024-09-23 10:00:00.123".to_string(),
            ep: "1".to_string(),
            session: Some("0x12345".to_string()),
            user: Some("admin".to_string()),
            sql_type: Some("SEL".to_string()),
            description: "SELECT * FROM users".to_string(),
            ..Default::default()
        };

        // Export single record
        json_exporter.export_record(&record).await?;

        // Finalize export
        json_exporter.finalize().await?;

        println!("Async JSON export completed!");
    }

    Ok(())
}

#[cfg(feature = "async")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Run sync example
    sync_example()?;

    // Run async example
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async_example())?;

    Ok(())
}

#[cfg(not(feature = "async"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    sync_example()
}