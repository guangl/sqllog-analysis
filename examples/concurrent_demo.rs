//! 并发解析和导出示例
//!
//! 演示如何使用多线程并发解析 SQL 日志并使用多个导出器进行导出

use sqllog_analysis::config::SqllogConfig;
use sqllog_analysis::prelude::*;
use std::path::PathBuf;
use std::time::Instant;

#[cfg(all(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-duckdb"
))]
fn main() -> Result<()> {
    // 初始化日志系统
    #[cfg(feature = "logging")]
    {
        tracing_subscriber::fmt().with_env_filter("debug").init();
    }

    println!("=== 并发解析和导出示例 ===\n");

    // 配置并发解析器
    let config = SqllogConfig {
        thread_count: Some(4),   // 使用4个解析线程
        batch_size: 1000,        // 每批1000条记录
        queue_buffer_size: 5000, // 队列缓冲区大小
    };

    let parser = ConcurrentParser::new(config);

    // 准备要解析的文件列表
    let file_paths = vec![
        PathBuf::from("sqllog/dmsql_OA01_20250916_200253.log"),
        PathBuf::from("logs/app.log.2025-09-23"),
        PathBuf::from("logs/sqllog.2025-09-23"),
    ]
    .into_iter()
    .filter(|p| p.exists()) // 只处理存在的文件
    .collect::<Vec<_>>();

    if file_paths.is_empty() {
        println!("没有找到可解析的日志文件！");
        println!("请确保以下文件存在：");
        println!("  - sqllog/dmsql_OA01_20250916_200253.log");
        println!("  - logs/app.log.2025-09-23");
        println!("  - logs/sqllog.2025-09-23");
        return Ok(());
    }

    println!("找到 {} 个日志文件准备解析:", file_paths.len());
    for (i, path) in file_paths.iter().enumerate() {
        println!("  {}. {}", i + 1, path.display());
    }
    println!();

    // 创建多个导出器
    let mut exporters: Vec<Box<dyn SyncExporter>> = Vec::new();

    // CSV 导出器
    let csv_exporter = SyncCsvExporter::new("output/concurrent_output.csv")?;
    exporters.push(Box::new(csv_exporter));

    // JSON 导出器
    let json_exporter = SyncJsonExporter::new("output/concurrent_output.json")?;
    exporters.push(Box::new(json_exporter));

    // DuckDB 导出器
    let duckdb_exporter =
        SyncDuckdbExporter::new("output/concurrent_output.db")?;
    exporters.push(Box::new(duckdb_exporter));

    println!("创建了 {} 个导出器:", exporters.len());
    for exporter in &exporters {
        println!("  - {}", exporter.name());
    }
    println!();

    // 开始并发解析和导出
    let start_time = Instant::now();
    println!("开始并发解析和导出...");

    let (errors, stats) =
        parser.parse_and_export_concurrent(&file_paths, exporters)?;

    let elapsed = start_time.elapsed();
    println!("\n=== 解析和导出完成 ===");
    println!("总耗时: {:?}", elapsed);
    println!("解析错误数: {}", errors.len());

    // 打印错误详情（如果有）
    if !errors.is_empty() {
        println!("\n解析错误详情:");
        for (i, error) in errors.iter().enumerate().take(10) {
            println!("  {}. 第{}行: {}", i + 1, error.line, error.error);
            if !error.content.is_empty() {
                println!(
                    "     内容: {}",
                    error.content.chars().take(100).collect::<String>()
                );
            }
        }
        if errors.len() > 10 {
            println!("  ... 还有 {} 个错误", errors.len() - 10);
        }
    }

    // 打印导出统计信息
    println!("\n=== 导出统计信息 ===");
    let mut total_exported = 0;
    let mut total_failed = 0;

    for (name, stat) in stats {
        println!("\n导出器: {}", name);
        println!("  成功导出: {} 条记录", stat.exported_records);
        println!("  失败记录: {} 条记录", stat.failed_records);
        println!("  开始时间: {:?}", stat.start_time);
        if let Some(end_time) = stat.end_time {
            println!("  结束时间: {:?}", end_time);
            println!(
                "  导出耗时: {:?}",
                end_time.duration_since(stat.start_time).unwrap_or_default()
            );
        }

        total_exported += stat.exported_records;
        total_failed += stat.failed_records;
    }

    println!("\n=== 总体统计 ===");
    println!("总导出记录: {} 条", total_exported);
    println!("总失败记录: {} 条", total_failed);
    println!("解析错误: {} 个", errors.len());
    println!("总处理时间: {:?}", elapsed);

    if total_exported > 0 {
        let records_per_second = total_exported as f64 / elapsed.as_secs_f64();
        println!("处理速度: {:.2} 记录/秒", records_per_second);
    }

    println!("\n输出文件:");
    println!("  - output/concurrent_output.csv");
    println!("  - output/concurrent_output.json");
    println!("  - output/concurrent_output.db");

    Ok(())
}

#[cfg(not(all(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-duckdb"
)))]
fn main() {
    println!("此示例需要启用以下特性:");
    println!("  --features=\"exporter-csv,exporter-json,exporter-duckdb\"");
    println!("\n请使用以下命令运行:");
    println!(
        "  cargo run --example concurrent_demo --features=\"exporter-csv,exporter-json,exporter-duckdb\""
    );
}

/// 简化版本：只解析不导出
fn demo_parse_only() -> Result<()> {
    println!("\n=== 纯解析示例（不导出）===");

    let config = SqllogConfig::default();
    let parser = ConcurrentParser::new(config);

    let file_paths = vec![PathBuf::from("example_output/test_data.log")]
        .into_iter()
        .filter(|p| p.exists())
        .collect::<Vec<_>>();

    if file_paths.is_empty() {
        println!("没有找到测试文件 example_output/test_data.log");
        return Ok(());
    }

    let start_time = Instant::now();
    let (records, errors) = parser.parse_files_concurrent(&file_paths)?;
    let elapsed = start_time.elapsed();

    println!("解析完成:");
    println!("  文件数: {}", file_paths.len());
    println!("  记录数: {}", records.len());
    println!("  错误数: {}", errors.len());
    println!("  耗时: {:?}", elapsed);

    if !records.is_empty() {
        println!("\n前5条记录:");
        for (i, record) in records.iter().take(5).enumerate() {
            println!(
                "  {}. {} - {} - {}",
                i + 1,
                record.occurrence_time,
                record.sql_type.as_deref().unwrap_or("未知"),
                record.description.chars().take(50).collect::<String>()
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_demo_parse_only() {
        let _ = demo_parse_only();
    }
}
