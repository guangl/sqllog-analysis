use sqllog_analysis::{config::SqllogConfig, logging::{init_logging, LogConfig}};
use tracing::Level;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    init_logging(LogConfig::new().level(Level::INFO))?;

    println!("=== SQL 日志解析器配置总结 ===\n");

    // 展示默认配置
    let default_config = SqllogConfig::default();
    println!("📋 默认配置：");
    println!("  batch_size: {} (0 = 不分块，解析整个文件)", default_config.batch_size);
    println!("  thread_count: {:?} (Some(0) = 根据文件数量自动检测)", default_config.thread_count);

    // 展示日志级别选项
    println!("\n📊 支持的日志级别：");
    println!("  • ERROR: 只显示错误信息");
    println!("  • WARN:  显示警告和错误");
    println!("  • INFO:  显示基本信息、警告和错误");
    println!("  • DEBUG: 显示详细调试信息");
    println!("  • TRACE: 显示最详细的跟踪信息");

    // 展示配置行为
    println!("\n⚙️ 配置行为说明：");
    println!("  batch_size = 0:");
    println!("    → 不进行分块处理，直接解析整个文件");
    println!("    → 适合小到中等大小的文件");

    println!("\n  thread_count = Some(0):");
    println!("    → 自动检测线程数量");
    println!("    → 线程数 = min(文件数量, CPU核心数)");

    println!("\n  thread_count = None:");
    println!("    → 使用默认线程数（通常等于CPU核心数）");

    // 展示使用示例
    println!("\n💡 使用示例：");
    println!("  RUST_LOG=trace cargo run --example debug_trace_logging");
    println!("  RUST_LOG=debug cargo run --example concurrent_demo");
    println!("  RUST_LOG=info cargo run");

    Ok(())
}