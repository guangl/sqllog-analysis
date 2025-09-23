//! async 功能集成测试

mod common;

#[cfg(feature = "async")]
mod async_tests {
    use sqllog_analysis::sqllog::AsyncSqllogParser;
    use tempfile::TempDir;
    use tokio;

    use super::common;

    #[tokio::test]
    async fn test_async_basic_parsing() {
        println!("🔄 测试异步基本解析功能...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let test_file = common::create_test_sqllog(
            &temp_dir,
            "async_basic.log",
            common::SAMPLE_SQLLOG_CONTENT,
        );

        let result = AsyncSqllogParser::parse_with_hooks(test_file, 1000).await;
        assert!(result.is_ok(), "异步解析创建失败: {:?}", result.err());

        let (mut record_rx, mut error_rx) = result.unwrap();

        let mut all_records = Vec::new();
        let mut all_errors = Vec::new();

        // 接收所有记录
        while let Some(records) = record_rx.recv().await {
            all_records.extend(records);
        }

        // 接收所有错误
        while let Some(errors) = error_rx.recv().await {
            all_errors.extend(errors);
        }

        assert!(!all_records.is_empty(), "应该解析出记录");
        println!(
            "✅ 异步基本解析测试通过: {} 条记录, {} 个错误",
            all_records.len(),
            all_errors.len()
        );
    }

    #[tokio::test]
    async fn test_async_batch_processing() {
        println!("🔄 测试异步批次处理...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let test_file = common::create_test_sqllog(
            &temp_dir,
            "async_batch.log",
            common::COMPLEX_SQLLOG_CONTENT,
        );

        let result = AsyncSqllogParser::parse_with_hooks(test_file, 2).await;
        assert!(result.is_ok(), "异步批处理创建失败: {:?}", result.err());

        let (mut record_rx, mut error_rx) = result.unwrap();

        let mut total_records = 0;
        let mut total_errors = 0;
        let mut batch_count = 0;

        // 接收记录批次
        while let Some(batch_records) = record_rx.recv().await {
            total_records += batch_records.len();
            batch_count += 1;
        }

        // 接收错误批次
        while let Some(batch_errors) = error_rx.recv().await {
            total_errors += batch_errors.len();
        }

        assert!(total_records > 0 || total_errors > 0, "应该有解析结果");
        assert!(batch_count > 0, "应该有批次处理");
        println!(
            "✅ 异步批次处理测试通过: {} 条记录, {} 个错误, {} 个批次",
            total_records, total_errors, batch_count
        );
    }

    #[tokio::test]
    async fn test_async_timeout() {
        println!("🔄 测试异步解析超时...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let test_file = common::create_test_sqllog(
            &temp_dir,
            "async_timeout.log",
            common::SAMPLE_SQLLOG_CONTENT,
        );

        let result = AsyncSqllogParser::parse_with_hooks(test_file, 1000).await;
        assert!(result.is_ok(), "异步解析创建失败: {:?}", result.err());

        let (mut record_rx, mut error_rx) = result.unwrap();

        // 使用超时测试
        let timeout_result =
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let mut total_items = 0;
                while let Some(records) = record_rx.recv().await {
                    total_items += records.len();
                }
                while let Some(errors) = error_rx.recv().await {
                    total_items += errors.len();
                }
                total_items
            })
            .await;

        assert!(timeout_result.is_ok(), "解析应该在超时时间内完成");
        println!("✅ 异步超时测试通过");
    }

    #[tokio::test]
    async fn test_concurrent_tasks() {
        println!("🔄 测试并发异步任务...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        let mut tasks = Vec::new();
        let mut total_record_count = 0;

        for i in 0..3 {
            let test_file = common::create_test_sqllog(
                &temp_dir,
                &format!("concurrent_{}.log", i),
                common::SAMPLE_SQLLOG_CONTENT,
            );

            let task = tokio::spawn(async move {
                let result =
                    AsyncSqllogParser::parse_with_hooks(test_file, 100).await;
                if let Ok((mut record_rx, mut error_rx)) = result {
                    let mut record_count = 0;
                    while let Some(records) = record_rx.recv().await {
                        record_count += records.len();
                    }
                    // 清空错误通道
                    while let Some(_) = error_rx.recv().await {}
                    record_count
                } else {
                    0
                }
            });

            tasks.push(task);
        }

        for task in tasks {
            let count = task.await.expect("任务应该成功完成");
            total_record_count += count;
        }

        assert!(total_record_count > 0, "并发任务应该解析出记录");
        println!("✅ 并发异步任务测试通过: 总共 {} 条记录", total_record_count);
    }

    #[tokio::test]
    async fn test_async_large_chunk() {
        println!("🔄 测试大块异步解析...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // 创建一个较大的测试文件
        let large_content = common::COMPLEX_SQLLOG_CONTENT.repeat(10);
        let test_file = common::create_test_sqllog(
            &temp_dir,
            "large_async.log",
            &large_content,
        );

        let result = AsyncSqllogParser::parse_with_hooks(test_file, 50).await;
        assert!(result.is_ok(), "大文件异步解析创建失败: {:?}", result.err());

        let (mut record_rx, mut error_rx) = result.unwrap();

        let mut total_records = 0;
        let mut total_errors = 0;

        // 使用超时保护，避免无限等待
        let parse_result =
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while let Some(records) = record_rx.recv().await {
                    total_records += records.len();
                }
                while let Some(errors) = error_rx.recv().await {
                    total_errors += errors.len();
                }
                (total_records, total_errors)
            })
            .await;

        assert!(parse_result.is_ok(), "解析应该在超时时间内完成");
        let (records, errors) = parse_result.unwrap();

        assert!(records > 0 || errors > 0, "大文件应该有解析结果");
        println!(
            "✅ 大块异步解析测试通过: {} 条记录, {} 个错误",
            records, errors
        );
    }
}

#[cfg(not(feature = "async"))]
#[test]
fn async_feature_disabled() {
    println!("⚠️  async 功能未启用，跳过相关测试");
}
