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
            let options = parse_export_args(&args[2..]);
            if options.files.is_empty() {
                eprintln!("错误: 至少需要指定一个日志文件");
                return Ok(());
            }
            run_concurrent_export(&options)
        }
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

#[derive(Debug)]
struct ExportOptions {
    files: Vec<PathBuf>,
    output: Option<String>,
    format: Option<String>,
    batch_size: Option<usize>,
}

fn parse_export_args(args: &[String]) -> ExportOptions {
    let mut options = ExportOptions {
        files: Vec::new(),
        output: None,
        format: None,
        batch_size: None,
    };

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        match arg.as_str() {
            "--output" => {
                i += 1;
                if i < args.len() {
                    options.output = Some(args[i].clone());
                }
            }
            "--format" => {
                i += 1;
                if i < args.len() {
                    options.format = Some(args[i].clone());
                }
            }
            "--batch-size" => {
                i += 1;
                if i < args.len() {
                    if let Ok(size) = args[i].parse::<usize>() {
                        options.batch_size = Some(size);
                    }
                }
            }
            "--limit" => {
                i += 1;
                if i < args.len() {
                    // 忽略limit参数，不再支持
                    println!("警告: --limit 参数已被移除");
                }
            }
            _ => {
                if !arg.starts_with("--") {
                    let path = PathBuf::from(arg);
                    if path.exists() {
                        options.files.push(path);
                    }
                }
            }
        }
        i += 1;
    }

    options
}

fn get_file_args(args: &[String]) -> Vec<PathBuf> {
    args.iter().map(|s| PathBuf::from(s)).filter(|p| p.exists()).collect()
}

/// 只解析，不导出
fn run_parse_only(files: &[PathBuf]) -> Result<()> {
    println!("=== 解析模式 ===");
    println!("文件数量: {}", files.len());

    // 使用新的默认配置：batch_size = 0, thread_count = Some(0)
    let config = SqllogConfig::default();

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
fn run_concurrent_export(options: &ExportOptions) -> Result<()> {
    println!("=== 并发导出模式 ===");
    println!("文件数量: {}", options.files.len());

    // 使用新的默认配置：batch_size = 0, thread_count = Some(0)
    let mut config = SqllogConfig::default();

    // 应用批量大小配置
    if let Some(batch_size) = options.batch_size {
        config.batch_size = batch_size;
    }

    let parser = ConcurrentParser::new(config);

    // 创建导出器
    let mut exporters: Vec<Box<dyn SyncExporter + Send>> = Vec::new();

    // 根据format选项或默认创建导出器
    let format = options.format.as_deref().unwrap_or("auto");
    let output_base =
        options.output.as_deref().unwrap_or("output/export_result");

    // 确保输出目录存在
    if let Some(parent) = PathBuf::from(output_base).parent() {
        std::fs::create_dir_all(parent)?;
    }

    match format {
        #[cfg(feature = "exporter-csv")]
        "csv" => {
            println!("添加 CSV 导出器");
            let output_path = format!("{}.csv", output_base);
            exporters.push(Box::new(SyncCsvExporter::new(&output_path)?));
        }
        #[cfg(feature = "exporter-json")]
        "json" => {
            println!("添加 JSON 导出器");
            let output_path = format!("{}.json", output_base);
            exporters.push(Box::new(SyncJsonExporter::new(&output_path)?));
        }
        #[cfg(feature = "exporter-sqlite")]
        "sqlite" => {
            println!("添加 SQLite 导出器");
            let output_path = format!("{}.sqlite", output_base);
            exporters.push(Box::new(SyncSqliteExporter::new(&PathBuf::from(
                output_path,
            ))?));
        }
        #[cfg(feature = "exporter-duckdb")]
        "duckdb" => {
            println!("添加 DuckDB 导出器");
            let output_path = format!("{}.duckdb", output_base);
            exporters.push(Box::new(SyncDuckdbExporter::new(&PathBuf::from(
                output_path,
            ))?));
        }
        "auto" | _ => {
            // 自动模式：添加所有可用的导出器
            #[cfg(feature = "exporter-csv")]
            {
                println!("添加 CSV 导出器");
                let output_path = format!("{}.csv", output_base);
                exporters.push(Box::new(SyncCsvExporter::new(&output_path)?));
            }

            #[cfg(feature = "exporter-json")]
            {
                println!("添加 JSON 导出器");
                let output_path = format!("{}.json", output_base);
                exporters.push(Box::new(SyncJsonExporter::new(&output_path)?));
            }

            #[cfg(feature = "exporter-duckdb")]
            {
                println!("添加 DuckDB 导出器");
                let output_path = format!("{}.duckdb", output_base);
                exporters.push(Box::new(SyncDuckdbExporter::new(
                    &PathBuf::from(output_path),
                )?));
            }

            #[cfg(feature = "exporter-sqlite")]
            {
                println!("添加 SQLite 导出器");
                let output_path = format!("{}.sqlite", output_base);
                exporters.push(Box::new(SyncSqliteExporter::new(
                    &PathBuf::from(output_path),
                )?));
            }
        }
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
        parser.parse_and_export_concurrent(&options.files, exporters)?;
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
fn run_concurrent_export(_options: &ExportOptions) -> Result<()> {
    println!("导出功能需要启用导出器特性");
    println!("请使用 --features 参数，例如:");
    println!("  cargo run --features=\"exporter-csv,exporter-json\"");
    Ok(())
}
