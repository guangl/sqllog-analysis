//! 详细日志测试示例
//!
//! 这个示例演示如何使用更详细的日志记录功能，包括debug和trace级别

use std::path::PathBuf;
use std::io::Write;
use tempfile::NamedTempFile;
use sqllog_analysis::config::SqllogConfig;
use sqllog_analysis::logging::{init_logging, LogConfig};
use sqllog_analysis::sqllog::concurrent_parser::ConcurrentParser;
use tracing::Level;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化详细日志（TRACE级别）
    let log_config = LogConfig::new().level(Level::TRACE);
    init_logging(log_config)?;

    println!("=== 详细日志测试示例 ===");
    println!("启用了TRACE级别日志，您将看到非常详细的调试信息");

    // 创建测试数据
    let test_content = r#"2024-01-01 10:00:00.123 (EP[1] sess:abc thrd:t1 user:testuser trxid:tx1 stmt:s1) [SEL]: SELECT * FROM users WHERE id = 1;
EXECTIME: 15(ms) ROWCOUNT: 1 EXEC_ID: 1001.
2024-01-01 10:00:00.200 (EP[2] sess:def thrd:t2 user:testuser trxid:tx2 stmt:s2) [UPD]: UPDATE users SET name = '张三' WHERE id = 1;
EXECTIME: 25(ms) ROWCOUNT: 1 EXEC_ID: 1002.
2024-01-01 10:00:00.300 (EP[3] sess:ghi thrd:t3 user:testuser trxid:tx3 stmt:s3) [INS]: INSERT INTO users (name, email) VALUES ('李四', 'lisi@test.com');
EXECTIME: 35(ms) ROWCOUNT: 1 EXEC_ID: 1003.
2024-01-01 10:00:00.400 (EP[1] sess:abc thrd:t1 user:testuser trxid:tx4 stmt:s4) [DEL]: DELETE FROM temp_table WHERE created_at < '2024-01-01';
EXECTIME: 120(ms) ROWCOUNT: 50 EXEC_ID: 1004.
"#;

    // 创建多个测试文件
    let mut temp_files = Vec::new();
    for i in 0..3 {
        let mut temp_file = NamedTempFile::new()?;
        temp_file.write_all(test_content.as_bytes())?;
        println!("创建测试文件 {}: {}", i + 1, temp_file.path().display());
        temp_files.push(temp_file);
    }

    let file_paths: Vec<PathBuf> = temp_files
        .iter()
        .map(|f| f.path().to_path_buf())
        .collect();

    // 配置并发解析器
    let config = SqllogConfig {
        thread_count: Some(2), // 使用2个解析线程
        batch_size: 2,         // 小批次以更好地观察日志
        queue_buffer_size: 10,
    };

    let parser = ConcurrentParser::new(config);

    println!("\n--- 开始并发解析测试 ---");
    // 执行并发解析（不导出，仅收集结果）
    match parser.parse_files_concurrent(&file_paths) {
        Ok((records, errors)) => {
            println!("\n=== 解析结果 ===");
            println!("成功解析 {} 条记录", records.len());
            println!("解析错误: {} 个", errors.len());

            // 显示前几条记录的详细信息
            for (i, record) in records.iter().take(3).enumerate() {
                println!("记录 {}: {} - {} - {}ms",
                    i + 1,
                    record.sql_type.as_deref().unwrap_or("未知"),
                    record.description.chars().take(50).collect::<String>(),
                    record.execute_time.unwrap_or(0)
                );
            }
        }
        Err(e) => {
            eprintln!("并发解析失败: {}", e);
        }
    }

    println!("\n--- 测试完成 ---");
    println!("检查控制台输出和 logs 目录中的日志文件以查看详细的调试信息");

    Ok(())
}