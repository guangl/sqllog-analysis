//! 大文件异步流式解析测试示例
//!
//! 演示异步解析器如何处理大量数据的流式解析

#[cfg(feature = "async")]
use sqllog_analysis::prelude::*;

#[cfg(feature = "async")]
use tempfile::NamedTempFile;

#[cfg(feature = "async")]
use std::io::Write;

#[cfg(feature = "async")]
#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志系统
    #[cfg(feature = "logging")]
    sqllog_analysis::logging::init_default_logging().unwrap();

    // 创建临时大文件，包含多条记录
    let mut temp_file = NamedTempFile::new().unwrap();

    println!("生成大量测试数据...");
    let single_record = r#"2024-01-01 12:00:00.000 (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [SEL]: SELECT * FROM users WHERE id = 1;
EXECTIME: 100(ms) ROWCOUNT: 5 EXEC_ID: 123.
"#;

    // 生成1000条记录
    for i in 0..1000 {
        let record = single_record.replace("id = 1", &format!("id = {}", i + 1))
            .replace("EXEC_ID: 123", &format!("EXEC_ID: {}", i + 123));
        temp_file.write_all(record.as_bytes()).unwrap();
    }

    let temp_path = temp_file.path().to_path_buf();

    println!("开始大文件异步流式解析测试...");
    println!("使用小的chunk_size=50来演示流式处理");

    let start_time = std::time::Instant::now();

    // 使用较小的chunk_size来演示流式处理效果
    let (mut record_rx, mut error_rx) = AsyncSqllogParser::parse_with_hooks(
        temp_path,
        50  // 小的chunk_size
    ).await?;

    let mut total_records = 0;
    let mut total_errors = 0;
    let mut batch_count = 0;

    // 使用 tokio::join! 来同时处理记录和错误流
    let record_task = async {
        let mut count = 0;
        let mut batches = 0;
        while let Some(records) = record_rx.recv().await {
            count += records.len();
            batches += 1;
            println!("批次 {}: 接收到 {} 条记录 (累计: {})", batches, records.len(), count);

            // 演示处理每批记录
            if batches <= 3 {  // 只显示前3批的详情
                for (i, record) in records.iter().enumerate().take(3) {
                    println!("  示例记录 {}: 时间={}, SQL类型={:?}",
                        i + 1, record.occurrence_time, record.sql_type);
                }
                if records.len() > 3 {
                    println!("  ... 还有 {} 条记录", records.len() - 3);
                }
            }

            // 模拟处理时间
            if batches % 5 == 0 {
                println!("  -> 处理中... (已处理 {} 批次)", batches);
            }
        }
        println!("记录处理完成: {} 条记录, {} 个批次", count, batches);
        (count, batches)
    };

    let error_task = async {
        let mut count = 0;
        while let Some(errors) = error_rx.recv().await {
            count += errors.len();
            if !errors.is_empty() {
                println!("接收到 {} 个错误", errors.len());
                for error in errors.iter().take(3) {  // 只显示前3个错误
                    println!("  - 第{}行错误: {}", error.line, error.error);
                }
            }
        }
        count
    };

    // 等待两个任务同时完成
    let ((records_count, batches), errors_count) = tokio::join!(record_task, error_task);
    total_records = records_count;
    total_errors = errors_count;
    batch_count = batches;

    let elapsed = start_time.elapsed();

    println!("\n=== 大文件流式解析完成 ===");
    println!("总记录数: {}", total_records);
    println!("总错误数: {}", total_errors);
    println!("批次数量: {}", batch_count);
    println!("平均每批记录数: {:.1}", total_records as f64 / batch_count as f64);
    println!("处理时间: {:?}", elapsed);
    println!("处理速度: {:.0} 记录/秒", total_records as f64 / elapsed.as_secs_f64());

    Ok(())
}

#[cfg(not(feature = "async"))]
fn main() {
    println!("请使用 --features async 运行此示例");
}