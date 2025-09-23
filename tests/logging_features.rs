//! logging 功能集成测试

mod common;

#[cfg(feature = "logging")]
mod logging_tests {
    use sqllog_analysis::{
        config::SqllogConfig,
        sqllog::{ConcurrentParser, SyncSqllogParser},
    };
    use tempfile::TempDir;
    use tracing_subscriber;

    use super::common;

    #[test]
    fn test_logging_initialization() {
        // 测试日志系统初始化
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("测试日志系统初始化");
            tracing::debug!("这是一条调试消息");
            tracing::warn!("这是一条警告消息");
        });

        println!("✅ 日志系统初始化测试通过");
    }

    #[test]
    fn test_sync_parser_with_logging() {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_test_writer()
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let file_path = common::create_test_sqllog(
                &temp_dir,
                "logging_test.log",
                common::SAMPLE_SQLLOG_CONTENT,
            );

            let mut record_count = 0;
            let result = SyncSqllogParser::parse_with_hooks(
                &file_path,
                2,
                |records, _errors| {
                    record_count += records.len();
                    tracing::info!("处理了 {} 条记录", records.len());
                },
            );

            assert!(result.is_ok(), "带日志的解析应该成功");
            assert!(record_count > 0, "应该解析出记录");

            println!("✅ 同步解析器日志功能测试通过: {} 条记录", record_count);
        });
    }

    #[test]
    fn test_concurrent_parser_with_logging() {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let files = common::create_multiple_test_files(&temp_dir, 3);

            let config = SqllogConfig::default();
            let parser = ConcurrentParser::new(config);

            let result = parser.parse_files_concurrent(&files);
            assert!(result.is_ok(), "带日志的并发解析应该成功");

            let (records, errors) = result.unwrap();
            assert!(!records.is_empty(), "应该解析出记录");

            println!(
                "✅ 并发解析器日志功能测试通过: {} 条记录, {} 个错误",
                records.len(),
                errors.len()
            );
        });
    }

    #[test]
    fn test_different_log_levels() {
        let levels = [
            tracing::Level::ERROR,
            tracing::Level::WARN,
            tracing::Level::INFO,
            tracing::Level::DEBUG,
            tracing::Level::TRACE,
        ];

        for level in &levels {
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(*level)
                .with_test_writer()
                .finish();

            tracing::subscriber::with_default(subscriber, || {
                tracing::error!("错误级别日志");
                tracing::warn!("警告级别日志");
                tracing::info!("信息级别日志");
                tracing::debug!("调试级别日志");
                tracing::trace!("跟踪级别日志");
            });
        }

        println!("✅ 不同日志级别测试通过");
    }
}

#[cfg(not(feature = "logging"))]
mod no_logging_tests {
    #[test]
    fn test_no_logging_feature() {
        println!("✅ 无日志功能模式测试通过 - logging 功能未启用");
    }
}
