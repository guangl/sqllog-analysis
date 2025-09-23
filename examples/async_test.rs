//! 异步解析器功能测试示例

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

    // 创建临时测试文件
    let mut temp_file = NamedTempFile::new().unwrap();
    let test_content = r#"2024-01-01 12:00:00.000 (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [SEL]: SELECT * FROM users;
EXECTIME: 100(ms) ROWCOUNT: 5 EXEC_ID: 123.
2024-01-01 12:00:01.000 (EP[2] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [UPD]: UPDATE users SET name = 'test';
EXECTIME: 50(ms) ROWCOUNT: 1 EXEC_ID: 124.
"#;

    temp_file.write_all(test_content.as_bytes()).unwrap();
    let temp_path = temp_file.path().to_path_buf();

    println!("开始异步解析测试...");

    // 异步解析文件
    let (mut record_rx, mut error_rx) =
        AsyncSqllogParser::parse_with_hooks(temp_path, 1000).await?;

    let mut total_records = 0;
    let mut total_errors = 0;

    // 使用 join! 来同时处理记录和错误流
    let record_task = async {
        let mut count = 0;
        while let Some(records) = record_rx.recv().await {
            count += records.len();
            println!("接收到 {} 条记录", records.len());
            for record in records {
                println!(
                    "  - 时间: {}, SQL类型: {:?}, 执行时间: {:?}ms",
                    record.occurrence_time,
                    record.sql_type,
                    record.execute_time
                );
            }
        }
        println!("记录接收完成");
        count
    };

    let error_task = async {
        let mut count = 0;
        while let Some(errors) = error_rx.recv().await {
            count += errors.len();
            if !errors.is_empty() {
                println!("接收到 {} 个错误", errors.len());
                for error in errors {
                    println!("  - 第{}行错误: {}", error.line, error.error);
                }
            }
        }
        count
    };

    // 等待两个任务同时完成
    let (records_count, errors_count) = tokio::join!(record_task, error_task);
    total_records = records_count;
    total_errors = errors_count;

    println!("异步解析完成！");
    println!("总记录数: {}, 总错误数: {}", total_records, total_errors);

    Ok(())
}

#[cfg(not(feature = "async"))]
fn main() {
    println!("请使用 --features async 运行此示例");
}
