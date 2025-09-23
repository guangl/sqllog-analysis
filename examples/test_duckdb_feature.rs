//! DuckDB 导出器功能测试示例

use std::any::type_name;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("✅ DuckDB 导出器测试:");
    println!("这个示例验证了 DuckDB 导出器的模块分离");

    // 同步 DuckDB 导出器类型测试
    #[cfg(feature = "exporter-duckdb")]
    {
        println!("\nDuckDB 导出器类型测试:");

        #[cfg(not(feature = "async"))]
        {
            use sqllog_analysis::exporter::DuckdbExporter;
            use sqllog_analysis::exporter::sync_impl::SyncDuckdbExporter;

            println!("DuckdbExporter type: {}", type_name::<DuckdbExporter>());
            println!("SyncDuckdbExporter type: {}", type_name::<SyncDuckdbExporter>());
            println!("✅ DuckdbExporter 正确指向同步版本");
        }

        #[cfg(feature = "async")]
        {
            use sqllog_analysis::exporter::DuckdbExporter;
            use sqllog_analysis::exporter::sync_impl::SyncDuckdbExporter;

            println!("DuckdbExporter type: {}", type_name::<DuckdbExporter>());
            println!("SyncDuckdbExporter type: {}", type_name::<SyncDuckdbExporter>());
            println!("✅ DuckdbExporter 正确指向异步版本");
        }
    }

    #[cfg(not(feature = "exporter-duckdb"))]
    {
        println!("❌ DuckDB 导出器功能未启用");
        println!("请使用: cargo run --example test_duckdb_feature --features exporter-duckdb");
    }

    println!("\n✅ DuckDB 模块分离测试完成！");

    println!("\n使用方法:");
    println!("- 默认同步 DuckDB: cargo run --example test_duckdb_feature --features exporter-duckdb");
    println!("- 异步 DuckDB: cargo run --example test_duckdb_feature --features \"async,exporter-duckdb\"");

    Ok(())
}