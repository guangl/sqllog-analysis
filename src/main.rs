//! SQL 日志分析工具
//!
//! 支持多线程并发解析和多格式导出

use sqllog_analysis::config::SqllogConfig;
use sqllog_analysis::prelude::*;
use std::env;
use std::path::PathBuf;

fn main() -> Result<()> {
    // 初始化日志系统
    #[cfg(feature = "logging")]
    {
        tracing_subscriber::fmt()
            .with_env_filter(
                env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
            )
            .init();
    }

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage(&args[0]);
        return Ok(());
    }

    match args[1].as_str() {
        "parse" => {
            let files = get_file_args(&args[2..]);
            if files.is_empty() {
                eprintln!("错误: 至少需要指定一个日志文件");
                return Ok(());
            }
            run_parse_only(&files)
        }
        "export" => {
            let files = get_file_args(&args[2..]);
            if files.is_empty() {
                eprintln!("错误: 至少需要指定一个日志文件");
                return Ok(());
            }
            run_concurrent_export(&files)
        }
        "demo" => run_demo(),
        "help" | "--help" | "-h" => {
            print_usage(&args[0]);
            Ok(())
        }
        _ => {
            eprintln!("未知命令: {}", args[1]);
            print_usage(&args[0]);
            Ok(())
        }
    }
}

fn print_usage(program_name: &str) {
    println!("SQL 日志分析工具 v{}", sqllog_analysis::VERSION);
    println!();
    println!("用法:");
    println!("  {} <命令> [选项]", program_name);
    println!();
    println!("命令:");
    println!("  parse <文件...>     - 解析日志文件（不导出）");
    println!("  export <文件...>    - 解析并导出日志文件");
    println!("  demo                - 运行演示程序");
    println!("  help                - 显示此帮助信息");
    println!();
    println!("示例:");
    println!("  {} parse logs/app.log", program_name);
    println!("  {} export sqllog/*.log", program_name);
    println!("  {} demo", program_name);
    println!();
    println!("特性支持:");
    #[cfg(feature = "exporter-csv")]
    println!("  ✓ CSV 导出");
    #[cfg(not(feature = "exporter-csv"))]
    println!("  ✗ CSV 导出 (需要 --features=\"exporter-csv\")");

    #[cfg(feature = "exporter-json")]
    println!("  ✓ JSON 导出");
    #[cfg(not(feature = "exporter-json"))]
    println!("  ✗ JSON 导出 (需要 --features=\"exporter-json\")");

    #[cfg(feature = "exporter-duckdb")]
    println!("  ✓ DuckDB 导出");
    #[cfg(not(feature = "exporter-duckdb"))]
    println!("  ✗ DuckDB 导出 (需要 --features=\"exporter-duckdb\")");

    #[cfg(feature = "exporter-sqlite")]
    println!("  ✓ SQLite 导出");
    #[cfg(not(feature = "exporter-sqlite"))]
    println!("  ✗ SQLite 导出 (需要 --features=\"exporter-sqlite\")");
}

fn get_file_args(args: &[String]) -> Vec<PathBuf> {
    args.iter().map(|s| PathBuf::from(s)).filter(|p| p.exists()).collect()
}

/// 只解析，不导出
fn run_parse_only(files: &[PathBuf]) -> Result<()> {
    println!("=== 解析模式 ===");
    println!("文件数量: {}", files.len());

    let config = SqllogConfig {
        thread_count: Some(4), // 最多使用4个线程
        batch_size: 1000,
        queue_buffer_size: 5000,
    };

    let parser = ConcurrentParser::new(config);

    let start = std::time::Instant::now();
    let (records, errors) = parser.parse_files_concurrent(files)?;
    let elapsed = start.elapsed();

    println!("\n=== 解析结果 ===");
    println!("成功解析记录: {} 条", records.len());
    println!("解析错误: {} 个", errors.len());
    println!("处理时间: {:?}", elapsed);

    if !errors.is_empty() {
        println!("\n前几个错误:");
        for (i, error) in errors.iter().take(5).enumerate() {
            println!("  {}. 第{}行: {}", i + 1, error.line, error.error);
        }
    }

    if !records.is_empty() {
        println!("\n前几条记录:");
        for (i, record) in records.iter().take(3).enumerate() {
            println!(
                "  {}. {} [{}] {}",
                i + 1,
                record.occurrence_time,
                record.sql_type.as_deref().unwrap_or("未知"),
                record.description.chars().take(80).collect::<String>()
            );
        }
    }

    Ok(())
}

/// 并发解析和导出
#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
fn run_concurrent_export(files: &[PathBuf]) -> Result<()> {
    println!("=== 并发导出模式 ===");
    println!("文件数量: {}", files.len());

    let config = SqllogConfig {
        thread_count: Some(4), // 最多使用4个线程
        batch_size: 1000,
        queue_buffer_size: 5000,
    };

    let parser = ConcurrentParser::new(config);

    // 创建导出器
    let mut exporters: Vec<Box<dyn SyncExporter + Send>> = Vec::new();

    #[cfg(feature = "exporter-csv")]
    {
        println!("添加 CSV 导出器");
        exporters
            .push(Box::new(SyncCsvExporter::new("output/export_result.csv")?));
    }

    #[cfg(feature = "exporter-json")]
    {
        println!("添加 JSON 导出器");
        exporters.push(Box::new(SyncJsonExporter::new(
            "output/export_result.json",
        )?));
    }

    #[cfg(feature = "exporter-duckdb")]
    {
        println!("添加 DuckDB 导出器");
        exporters.push(Box::new(SyncDuckdbExporter::new(
            "output/export_result.db",
        )?));
    }

    #[cfg(feature = "exporter-sqlite")]
    {
        println!("添加 SQLite 导出器");
        exporters.push(Box::new(SyncSqliteExporter::new(
            "output/export_result.sqlite",
        )?));
    }

    if exporters.is_empty() {
        println!("警告: 没有可用的导出器");
        println!("请使用 --features 启用导出器，例如:");
        println!("  cargo run --features=\"exporter-csv,exporter-json\"");
        return Ok(());
    }

    println!("导出器数量: {}", exporters.len());

    let start = std::time::Instant::now();
    let (errors, stats) =
        parser.parse_and_export_concurrent(files, exporters)?;
    let elapsed = start.elapsed();

    println!("\n=== 导出结果 ===");
    println!("处理时间: {:?}", elapsed);
    println!("解析错误: {} 个", errors.len());

    let mut total_exported = 0;
    for (name, stat) in stats {
        println!("\n{}: ", name);
        println!("  导出记录: {} 条", stat.exported_records);
        println!("  失败记录: {} 条", stat.failed_records);
        total_exported += stat.exported_records;
    }

    println!("\n总导出记录: {} 条", total_exported);
    if total_exported > 0 {
        println!(
            "导出速度: {:.2} 记录/秒",
            total_exported as f64 / elapsed.as_secs_f64()
        );
    }

    Ok(())
}

#[cfg(not(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
)))]
fn run_concurrent_export(_files: &[PathBuf]) -> Result<()> {
    println!("导出功能需要启用导出器特性");
    println!("请使用 --features 参数，例如:");
    println!("  cargo run --features=\"exporter-csv,exporter-json\"");
    Ok(())
}

/// 运行演示程序
fn run_demo() -> Result<()> {
    println!("=== 演示模式 ===");

    // 查找示例文件
    let demo_files = vec![
        PathBuf::from("example_output/test_data.log"),
        PathBuf::from("logs/example.log.2025-09-23"),
        PathBuf::from("sqllog/dmsql_OA01_20250916_200253.log"),
    ]
    .into_iter()
    .filter(|p| p.exists())
    .collect::<Vec<_>>();

    if demo_files.is_empty() {
        println!("没有找到演示文件，创建测试数据...");
        create_demo_data()?;
        return run_parse_only(&[PathBuf::from("demo_test.log")]);
    }

    println!("找到演示文件: {} 个", demo_files.len());
    for file in &demo_files {
        println!("  - {}", file.display());
    }

    run_parse_only(&demo_files)
}

/// 创建演示数据
fn create_demo_data() -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let demo_content = r#"2024-01-01 12:00:00.000 (EP[1] sess:session1 thrd:thread1 user:admin trxid:tx001 stmt:SELECT * FROM users) [SEL]: 查询用户表;
EXECTIME: 100(ms) ROWCOUNT: 50 EXEC_ID: 1001.
2024-01-01 12:01:00.000 (EP[2] sess:session2 thrd:thread2 user:user1 trxid:tx002 stmt:INSERT INTO logs VALUES (1, 'test')) [INS]: 插入日志记录;
EXECTIME: 50(ms) ROWCOUNT: 1 EXEC_ID: 1002.
2024-01-01 12:02:00.000 (EP[1] sess:session1 thrd:thread1 user:admin trxid:tx003 stmt:UPDATE users SET status = 'active') [UPD]: 更新用户状态;
EXECTIME: 75(ms) ROWCOUNT: 20 EXEC_ID: 1003.
2024-01-01 12:03:00.000 (EP[3] sess:session3 thrd:thread3 user:admin trxid:tx004 stmt:DELETE FROM temp_table) [DEL]: 删除临时表数据;
EXECTIME: 25(ms) ROWCOUNT: 5 EXEC_ID: 1004.
"#;

    let mut file = File::create("demo_test.log")?;
    file.write_all(demo_content.as_bytes())?;

    println!("创建了演示文件: demo_test.log");

    Ok(())
}
