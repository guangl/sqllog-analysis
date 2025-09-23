//! 详细日志调试示例
//!
//! 演示debug和trace级别日志输出，用于调试并发解析过程

use sqllog_analysis::{
    config::SqllogConfig,
    logging::{init_logging, LogConfig},
    sqllog::concurrent_parser::ConcurrentParser,
};
use tracing::Level;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化TRACE级别日志
    let log_config = LogConfig::new().level(Level::TRACE);
    init_logging(log_config)?;

    println!("=== 详细日志调试示例 ===");
    println!("本示例使用TRACE级别日志，展示并发解析的详细过程");

    // 检查sqllog目录
    let sqllog_dir = "sqllog";
    if !std::path::Path::new(sqllog_dir).exists() {
        println!("错误：sqllog目录不存在，请确保在项目根目录运行");
        return Ok(());
    }

    // 查找.log文件
    let mut log_files = Vec::new();
    for entry in std::fs::read_dir(sqllog_dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(ext) = path.extension() {
            if ext == "log" {
                log_files.push(path);
                if log_files.len() >= 2 {
                    break; // 只取前2个文件以避免处理时间过长
                }
            }
        }
    }

    if log_files.is_empty() {
        println!("警告：未找到.log文件");
        return Ok(());
    }

    println!("找到 {} 个日志文件:", log_files.len());
    for (i, file) in log_files.iter().enumerate() {
        println!("  {}. {}", i + 1, file.display());
    }

    // 配置并发解析器
    let config = SqllogConfig {
        thread_count: Some(2), // 使用2个线程
        batch_size: 100,      // 小批次以便观察更多日志输出
        queue_buffer_size: 50,
    };

    let parser = ConcurrentParser::new(config);

    println!("\n--- 开始并发解析（仅解析不导出）---");
    let start_time = std::time::Instant::now();

    match parser.parse_files_concurrent(&log_files) {
        Ok((records, errors)) => {
            let elapsed = start_time.elapsed();

            println!("\n=== 解析完成 ===");
            println!("解析耗时: {:?}", elapsed);
            println!("成功解析: {} 条记录", records.len());
            println!("解析错误: {} 个", errors.len());

            // 显示前几条记录
            if !records.is_empty() {
                println!("\n前3条记录示例:");
                for (i, record) in records.iter().take(3).enumerate() {
                    println!("  {}. {} - {} - {} - EP[{}]",
                        i + 1,
                        record.occurrence_time,
                        record.sql_type.as_deref().unwrap_or("未知"),
                        record.user.as_deref().unwrap_or("未知用户"),
                        record.ep
                    );
                }
            }

            // 显示一些错误示例
            if !errors.is_empty() {
                println!("\n前3个解析错误:");
                for (i, error) in errors.iter().take(3).enumerate() {
                    println!("  {}. 行{}: {}",
                        i + 1,
                        error.line,
                        error.error.chars().take(50).collect::<String>()
                    );
                }
            }
        }
        Err(e) => {
            eprintln!("并发解析失败: {}", e);
        }
    }

    println!("\n--- 测试完成 ---");
    println!("请查看上方控制台输出的详细trace日志");
    println!("日志也会保存到 logs 目录中");

    Ok(())
}