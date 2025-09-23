//! 异步流式解析示例
//!
//! 展示如何使用异步流式功能解析 SQL 日志文件

#[cfg(feature = "async")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use sqllog_analysis::prelude::*;
    use std::path::Path;

    // 初始化日志系统
    #[cfg(feature = "logging")]
    {
        use tracing_subscriber::EnvFilter;
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive("sqllog_analysis=debug".parse()?),
            )
            .with_target(false)
            .with_thread_ids(true)
            .with_thread_names(true)
            .init();
    }

    println!("=== SQL日志异步流式解析示例 ===\n");

    let log_file = Path::new("sqllog/dmsql_OA01_20250916_200253.log");

    if !log_file.exists() {
        println!("   ⚠️  日志文件不存在: {}", log_file.display());
        return Ok(());
    }

    // 流式处理演示
    println!("🌊 流式处理大文件");
    println!("   - 使用生产者-消费者模式");
    println!("   - 分批次处理数据，避免内存占用过多");
    println!("   - 实时接收和处理解析结果\n");

    use std::path::PathBuf;

    let log_file_owned: PathBuf = log_file.to_path_buf();
    let (mut record_rx, mut error_rx) = AsyncSqllogParser::parse_with_hooks(
        log_file_owned,
        0, // 每批次处理记录数
    )
    .await?;

    let start = std::time::Instant::now();

    // 并发处理记录和错误流
    let record_task = tokio::spawn(async move {
        let mut total_count = 0;
        let mut batch_num = 0;

        while let Some(records) = record_rx.recv().await {
            batch_num += 1;
            total_count += records.len();

            println!("   📥 接收记录批次 {}: {} 条", batch_num, records.len());

            // 显示前几条记录的详细信息（仅第一批次）
            if batch_num == 1 && !records.is_empty() {
                println!("      💡 首批记录示例:");
                for (i, record) in records.iter().take(3).enumerate() {
                    println!(
                        "         {}. [EP{}] {} - {} {:?}ms",
                        i + 1,
                        record.ep,
                        record.occurrence_time,
                        record.sql_type.as_deref().unwrap_or("未知"),
                        record.execute_time
                    );
                }
            }

            // 模拟处理时间（可选）
            if batch_num % 5 == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }

        println!(
            "   ✅ 记录流处理完成，共接收 {} 条记录，{} 个批次",
            total_count, batch_num
        );
        total_count
    });

    let error_task = tokio::spawn(async move {
        let mut total_count = 0;
        let mut batch_num = 0;

        while let Some(errors) = error_rx.recv().await {
            if !errors.is_empty() {
                batch_num += 1;
                total_count += errors.len();
                println!(
                    "   ⚠️  接收错误批次 {}: {} 个",
                    batch_num,
                    errors.len()
                );

                // 显示错误详情（仅前几个）
                for (i, error) in errors.iter().take(2).enumerate() {
                    println!(
                        "      {}. 行 {}: {}",
                        i + 1,
                        error.line,
                        error.error
                    );
                }
            }
        }

        if total_count > 0 {
            println!(
                "   ❌ 错误流处理完成，共接收 {} 个错误，{} 个批次",
                total_count, batch_num
            );
        } else {
            println!("   ✅ 错误流处理完成，无错误");
        }
        total_count
    });

    // 等待两个任务完成
    let (record_count, error_count) =
        tokio::try_join!(record_task, error_task)?;
    let duration = start.elapsed();

    println!("\n📊 流式处理统计:");
    println!("   - 总记录数: {} 条", record_count);
    println!("   - 总错误数: {} 个", error_count);
    println!("   - 处理耗时: {:?}", duration);

    if record_count > 0 {
        let records_per_sec = record_count as f64 / duration.as_secs_f64();
        println!("   - 处理速度: {:.2} 条/秒", records_per_sec);
    }

    println!("\n💡 流式解析优势:");
    println!("   - 内存占用低：不需要一次性加载整个文件");
    println!("   - 实时处理：边解析边处理，响应及时");
    println!("   - 可中断：可以随时停止接收数据");
    println!("   - 并发友好：生产者和消费者可以独立运行");

    println!("\n=== 异步流式解析示例完成 ===");

    Ok(())
}

#[cfg(not(feature = "async"))]
fn main() {
    println!("此示例需要启用 'async' feature。");
    println!("请使用以下命令运行：");
    println!("cargo run --example async_parsing_demo --features async");
}
