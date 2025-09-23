//! 同步解析器功能测试示例

use sqllog_analysis::prelude::*;
use tempfile::NamedTempFile;
use std::io::Write;

fn main() -> Result<()> {
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

    println!("开始同步解析测试...");

    let mut total_records = 0;
    let mut total_errors = 0;

    // 同步解析文件
    SyncSqllogParser::parse_with_hooks(temp_file.path(), 1000, |records, errors| {
        total_records += records.len();
        total_errors += errors.len();

        if !records.is_empty() {
            println!("处理了 {} 条记录", records.len());
            for record in records {
                println!("  - 时间: {}, SQL类型: {:?}, 执行时间: {:?}ms",
                    record.occurrence_time, record.sql_type, record.execute_time);
            }
        }

        if !errors.is_empty() {
            println!("处理了 {} 个错误", errors.len());
            for error in errors {
                println!("  - 第{}行错误: {}", error.line, error.error);
            }
        }
    })?;

    println!("同步解析完成！");
    println!("总记录数: {}, 总错误数: {}", total_records, total_errors);

    Ok(())
}