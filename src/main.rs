//! SQL 日志分析工具 - 使用 clap 进行命令行解析

use clap::{Parser, Subcommand, ValueEnum};
use sqllog_analysis::config::SqllogConfig;
use sqllog_analysis::prelude::*;
use std::path::PathBuf;

#[cfg(feature = "exporter-csv")]
use sqllog_analysis::exporter::sync_impl::SyncCsvExporter;

#[cfg(feature = "exporter-json")]
use sqllog_analysis::exporter::sync_impl::SyncJsonExporter;

#[cfg(feature = "exporter-sqlite")]
use sqllog_analysis::exporter::sync_impl::SyncSqliteExporter;

#[cfg(feature = "exporter-duckdb")]
use sqllog_analysis::exporter::sync_impl::SyncDuckdbExporter;

/// SQL 日志分析工具
#[derive(Parser)]
#[command(name = "sqllog-cli")]
#[command(about = "达梦数据库 SQL 日志分析工具")]
#[command(version = sqllog_analysis::VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// 启用详细日志输出
    #[arg(short, long)]
    verbose: bool,

    /// 批处理大小
    #[arg(short, long, default_value = "1000")]
    batch_size: usize,

    /// 线程数量 (0 表示自动)
    #[arg(short, long, default_value = "0")]
    threads: usize,
    /// 错误输出路径（JSONL），若指定会覆盖环境变量 SQLOG_ERRORS_OUT
    #[arg(short = 'e', long, value_name = "ERRORS_OUT")]
    errors_out: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// 解析日志文件（仅解析，不导出）
    Parse {
        /// 日志文件路径
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
    /// 解析并导出日志文件
    Export {
        /// 日志文件路径
        #[arg(required = true)]
        files: Vec<PathBuf>,

        /// 输出文件基础路径（不含扩展名）
        #[arg(short, long, default_value = "output/export_result")]
        output: String,

        /// 导出格式
        #[arg(short, long, value_enum)]
        format: Option<ExportFormat>,
    },
}

#[derive(Clone, Debug, ValueEnum)]
enum ExportFormat {
    /// CSV 格式
    Csv,
    /// JSON 格式
    Json,
    /// SQLite 数据库
    Sqlite,
    /// DuckDB 数据库
    Duckdb,
    /// 所有可用格式
    Auto,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // 初始化日志系统（不再依赖环境变量）
    #[cfg(feature = "logging")]
    {
        let log_level = if cli.verbose { "trace" } else { "debug" };
        tracing_subscriber::fmt()
            .with_max_level(match log_level {
                "trace" => tracing::Level::TRACE,
                "debug" => tracing::Level::DEBUG,
                "info" => tracing::Level::INFO,
                "warn" => tracing::Level::WARN,
                "error" => tracing::Level::ERROR,
                _ => tracing::Level::INFO,
            })
            .init();
        tracing::info!("SQL日志分析工具启动");
    }

    // 创建配置
    let mut config = SqllogConfig::default();
    config.batch_size = cli.batch_size;
    if cli.threads > 0 {
        config.thread_count = Some(cli.threads);
    }

    // 将命令行提供的错误输出路径写入配置（不使用环境变量）
    if let Some(path) = cli.errors_out.as_ref() {
        config.errors_out = Some(path.clone());
    }

    match cli.command {
        Commands::Parse { files } => run_parse_only(&files, config),
        Commands::Export { files, output, format } => {
            run_export(&files, &output, format, config)
        }
    }
}

/// 只解析，不导出
fn run_parse_only(files: &[PathBuf], config: SqllogConfig) -> Result<()> {
    println!("=== 解析模式 ===");
    println!("文件数量: {}", files.len());

    #[cfg(feature = "logging")]
    tracing::info!("开始解析 {} 个文件", files.len());

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
                record.sql_type.as_deref().unwrap_or("UNKNOWN"),
                record.description.chars().take(80).collect::<String>()
            );
        }
    }

    Ok(())
}

/// 并发解析和导出
fn run_export(
    files: &[PathBuf],
    output_base: &str,
    format: Option<ExportFormat>,
    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    config: SqllogConfig,
    #[cfg(not(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    )))]
    _config: SqllogConfig,
) -> Result<()> {
    println!("=== 并发导出模式 ===");
    println!("文件数量: {}", files.len());

    #[cfg(feature = "logging")]
    tracing::info!(
        "开始并发导出: {} 个文件, 格式: {:?}, 输出: {}",
        files.len(),
        format,
        output_base
    );

    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    let parser = ConcurrentParser::new(config);

    // 确保输出目录存在
    if let Some(parent) = PathBuf::from(output_base).parent() {
        std::fs::create_dir_all(parent)?;
    }

    // 根据格式进行流式解析和导出
    let start_time = std::time::Instant::now();
    let results: Vec<(usize, usize)> = match format
        .unwrap_or(ExportFormat::Auto)
    {
        ExportFormat::Csv => {
            #[cfg(feature = "exporter-csv")]
            {
                let output_path = format!("{}.csv", output_base);
                let exporter = SyncCsvExporter::new(&output_path)?;
                parser.parse_and_export_streaming(files, exporter)?
            }
            #[cfg(not(feature = "exporter-csv"))]
            {
                eprintln!(
                    "错误: CSV 导出器未启用。请使用 --features=\"exporter-csv\" 重新编译。"
                );
                Vec::new()
            }
        }
        ExportFormat::Json => {
            #[cfg(feature = "exporter-json")]
            {
                let output_path = format!("{}.json", output_base);
                let exporter = SyncJsonExporter::new(&output_path)?;
                parser.parse_and_export_streaming(files, exporter)?
            }
            #[cfg(not(feature = "exporter-json"))]
            {
                eprintln!(
                    "错误: JSON 导出器未启用。请使用 --features=\"exporter-json\" 重新编译。"
                );
                Vec::new()
            }
        }
        ExportFormat::Sqlite => {
            #[cfg(feature = "exporter-sqlite")]
            {
                let output_path = format!("{}.sqlite", output_base);
                let exporter =
                    SyncSqliteExporter::new(&PathBuf::from(output_path))?;
                parser.parse_and_export_streaming(files, exporter)?
            }
            #[cfg(not(feature = "exporter-sqlite"))]
            {
                eprintln!(
                    "错误: SQLite 导出器未启用。请使用 --features=\"exporter-sqlite\" 重新编译。"
                );
                Vec::new()
            }
        }
        ExportFormat::Duckdb => {
            #[cfg(feature = "exporter-duckdb")]
            {
                let output_path = format!("{}.duckdb", output_base);
                let exporter =
                    SyncDuckdbExporter::new(&PathBuf::from(output_path))?;
                parser.parse_and_export_streaming(files, exporter)?
            }
            #[cfg(not(feature = "exporter-duckdb"))]
            {
                eprintln!(
                    "错误: DuckDB 导出器未启用。请使用 --features=\"exporter-duckdb\" 重新编译。"
                );
                Vec::new()
            }
        }
        ExportFormat::Auto => {
            // Auto 模式选择第一个可用的导出器
            #[cfg(feature = "exporter-csv")]
            {
                let output_path = format!("{}.csv", output_base);
                let exporter = SyncCsvExporter::new(&output_path)?;
                parser.parse_and_export_streaming(files, exporter)?
            }
            #[cfg(all(
                not(feature = "exporter-csv"),
                feature = "exporter-json"
            ))]
            {
                let output_path = format!("{}.json", output_base);
                let exporter = SyncJsonExporter::new(&output_path)?;
                parser.parse_and_export_streaming(files, exporter)?
            }
            #[cfg(all(
                not(feature = "exporter-csv"),
                not(feature = "exporter-json"),
                feature = "exporter-sqlite"
            ))]
            {
                let output_path = format!("{}.sqlite", output_base);
                let exporter =
                    SyncSqliteExporter::new(&PathBuf::from(output_path))?;
                parser.parse_and_export_streaming(files, exporter)?
            }
            #[cfg(all(
                not(feature = "exporter-csv"),
                not(feature = "exporter-json"),
                not(feature = "exporter-sqlite"),
                feature = "exporter-duckdb"
            ))]
            {
                let output_path = format!("{}.duckdb", output_base);
                let exporter =
                    SyncDuckdbExporter::new(&PathBuf::from(output_path))?;
                parser.parse_and_export_streaming(files, exporter)?
            }
            #[cfg(not(any(
                feature = "exporter-csv",
                feature = "exporter-json",
                feature = "exporter-sqlite",
                feature = "exporter-duckdb"
            )))]
            {
                eprintln!(
                    "错误: 没有启用任何导出器。请使用适当的 --features 重新编译。"
                );
                Vec::new()
            }
        }
    };

    let total_duration = start_time.elapsed();
    let total_records: usize = results.iter().map(|(r, _)| r).sum();
    let total_errors: usize = results.iter().map(|(_, e)| e).sum();

    println!("\n=== 处理总结 ===");
    println!("处理文件数: {}", files.len());
    println!("总记录数: {}", total_records);
    println!("总错误数: {}", total_errors);
    println!("总处理时间: {:?}", total_duration);

    println!("导出完成！");
    Ok(())
}

#[cfg(test)]
#[cfg(any(feature = "exporter-csv", feature = "exporter-json"))]
mod tests {
    use super::*;
    use sqllog_analysis::config::SqllogConfig;
    use std::fs;
    use tempfile::TempDir;

    fn sample_log() -> &'static str {
        r#"2025-09-16 20:02:53.562 (EP[0] sess:0x1 thrd:1 user:U trxid:1 stmt:0x1) [SEL]: SELECT 1
2025-09-16 20:02:53.563 (EP[0] sess:0x2 thrd:2 user:U trxid:2 stmt:0x2) [SEL]: SELECT 2"#
    }

    #[cfg(feature = "exporter-csv")]
    #[test]
    fn test_run_export_csv_creates_file() {
        let tmp = TempDir::new().unwrap();
        let log_path = tmp.path().join("test.log");
        fs::write(&log_path, sample_log()).unwrap();

        let output_base = tmp.path().join("out/export_base");
        let output_base_str = output_base.to_str().unwrap();

        let mut config = SqllogConfig::default();
        config.batch_size = 10;

        // Call run_export with CSV format
        let res = run_export(
            &[log_path.clone()],
            output_base_str,
            Some(ExportFormat::Csv),
            config,
        );
        assert!(res.is_ok());

        // csv file should exist
        let csv_path = format!("{}.csv", output_base_str);
        assert!(std::path::Path::new(&csv_path).exists());
    }

    #[cfg(feature = "exporter-json")]
    #[test]
    fn test_run_export_json_creates_file() {
        let tmp = TempDir::new().unwrap();
        let log_path = tmp.path().join("test.log");
        fs::write(&log_path, sample_log()).unwrap();

        let output_base = tmp.path().join("out/export_base2");
        let output_base_str = output_base.to_str().unwrap();

        let mut config = SqllogConfig::default();
        config.batch_size = 10;

        // Call run_export with JSON format
        let res = run_export(
            &[log_path.clone()],
            output_base_str,
            Some(ExportFormat::Json),
            config,
        );
        assert!(res.is_ok());

        // json file should exist
        let json_path = format!("{}.json", output_base_str);
        assert!(std::path::Path::new(&json_path).exists());
    }

    #[test]
    fn test_run_export_auto_prefers_available_exporter() {
        let tmp = TempDir::new().unwrap();
        let log_path = tmp.path().join("test.log");
        fs::write(&log_path, sample_log()).unwrap();

        let output_base = tmp.path().join("out/export_base_auto");
        let output_base_str = output_base.to_str().unwrap();

        let mut config = SqllogConfig::default();
        config.batch_size = 10;

        // Auto should pick the first available exporter (CSV when enabled)
        let res = run_export(
            &[log_path.clone()],
            output_base_str,
            Some(ExportFormat::Auto),
            config,
        );
        assert!(res.is_ok());

        // Expect at least one of csv or json to exist depending on features; prefer csv
        let csv_path = format!("{}.csv", output_base_str);
        let json_path = format!("{}.json", output_base_str);
        assert!(
            std::path::Path::new(&csv_path).exists()
                || std::path::Path::new(&json_path).exists()
        );
    }
}
