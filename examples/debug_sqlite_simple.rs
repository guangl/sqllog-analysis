//! 单独测试SQLite导出器

use sqllog_analysis::prelude::*;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<()> {
    println!("开始单独测试SQLite导出器...");

    // 清理并创建输出目录
    let _ = fs::remove_dir_all("output");
    fs::create_dir_all("output").unwrap();

    // 解析日志文件
    let mut result = ParseResult::new();
    SyncSqllogParser::parse_with_hooks(
        "sqllog/dmsql_OA01_20250916_200253.log",
        1000,
        |records, errors| {
            result.records.extend(records);
            result.errors.extend(errors);

            // 只解析前1000条就够了，用于测试
            if result.records.len() >= 1000 {
                return false; // 停止解析
            }
            true // 继续解析
        },
    )?;

    println!(
        "解析完成，记录数: {}, 错误数: {}",
        result.records.len(),
        result.errors.len()
    );

    if result.records.is_empty() {
        println!("没有解析到记录，退出测试");
        return Ok(());
    }

    // 创建SQLite导出器
    let mut exporter =
        SyncSqliteExporter::new(&PathBuf::from("output/debug_test.sqlite"))?;

    // 只测试前10条记录
    let test_records =
        &result.records[0..std::cmp::min(10, result.records.len())];
    println!("测试前 {} 条记录", test_records.len());

    // 导出记录
    exporter.export_batch(test_records)?;

    // 完成导出
    exporter.finalize()?;

    println!("SQLite单独测试完成");
    Ok(())
}
