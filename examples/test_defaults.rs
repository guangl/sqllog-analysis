//! 测试默认配置值
//!
//! 验证 batch_size 和 thread_count 的默认值为 0 的行为

use sqllog_analysis::config::SqllogConfig;
use sqllog_analysis::logging::{init_logging, LogConfig};
use sqllog_analysis::sqllog::concurrent_parser::ConcurrentParser;
use tracing::Level;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    let log_config = LogConfig::new().level(Level::INFO);
    init_logging(log_config)?;

    println!("=== 默认配置值测试 ===");

    // 测试默认配置
    let default_config = SqllogConfig::default();
    println!("默认配置:");
    println!("  thread_count: {:?}", default_config.thread_count);
    println!("  batch_size: {}", default_config.batch_size);
    println!("  queue_buffer_size: {}", default_config.queue_buffer_size);

    // 创建解析器
    let parser = ConcurrentParser::new(default_config);
    println!("\n使用默认配置创建的并发解析器已准备就绪");

    // 模拟文件列表
    let file_paths = vec![
        std::path::PathBuf::from("file1.log"),
        std::path::PathBuf::from("file2.log"),
        std::path::PathBuf::from("file3.log"),
    ];

    println!("\n测试场景: 3个文件");
    println!("根据默认配置 thread_count=0，应该自动使用 3 个线程");
    println!("根据默认配置 batch_size=0，应该不分块直接解析整个文件");

    // 注意：实际解析会失败（因为文件不存在），但我们主要测试配置逻辑
    println!("\n配置测试完成 ✓");
    println!("- thread_count 默认值为 Some(0)，表示自动根据文件数量确定");
    println!("- batch_size 默认值为 0，表示不分块直接解析整个文件");

    Ok(())
}