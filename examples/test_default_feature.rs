//! 测试默认 feature 配置的示例

fn main() {
    println!("✅ 功能分离测试:");
    println!("这个示例验证了 CSV、JSON 和 SQLite 导出器的模块分离");
    println!("");

    #[cfg(feature = "exporter-csv")]
    {
        use sqllog_analysis::exporter::{CsvExporter, SyncCsvExporter};

        println!("CSV 导出器类型测试:");
        println!("CsvExporter type: {}", std::any::type_name::<CsvExporter>());
        println!(
            "SyncCsvExporter type: {}",
            std::any::type_name::<SyncCsvExporter>()
        );

        let csv_type = std::any::type_name::<CsvExporter>();
        if csv_type.contains("SyncCsvExporter") {
            println!("✅ CsvExporter 正确指向同步版本");
        } else if csv_type.contains("AsyncCsvExporter") {
            println!("✅ CsvExporter 正确指向异步版本");
        } else {
            println!("❌ CsvExporter 类型不正确: {}", csv_type);
        }
        println!();
    }

    #[cfg(feature = "exporter-json")]
    {
        use sqllog_analysis::exporter::{JsonExporter, SyncJsonExporter};

        println!("JSON 导出器类型测试:");
        println!(
            "JsonExporter type: {}",
            std::any::type_name::<JsonExporter>()
        );
        println!(
            "SyncJsonExporter type: {}",
            std::any::type_name::<SyncJsonExporter>()
        );

        let json_type = std::any::type_name::<JsonExporter>();
        if json_type.contains("SyncJsonExporter") {
            println!("✅ JsonExporter 正确指向同步版本");
        } else if json_type.contains("AsyncJsonExporter") {
            println!("✅ JsonExporter 正确指向异步版本");
        } else {
            println!("❌ JsonExporter 类型不正确: {}", json_type);
        }
        println!();
    }

    #[cfg(feature = "exporter-sqlite")]
    {
        use sqllog_analysis::exporter::{MultiExporter, SyncMultiExporter};

        println!("多导出器类型测试:");
        println!(
            "MultiExporter type: {}",
            std::any::type_name::<MultiExporter>()
        );
        println!(
            "SyncMultiExporter type: {}",
            std::any::type_name::<SyncMultiExporter>()
        );

        let multi_type = std::any::type_name::<MultiExporter>();
        if multi_type.contains("SyncMultiExporter") {
            println!("✅ MultiExporter 正确指向同步版本");
        } else if multi_type.contains("AsyncMultiExporter") {
            println!("✅ MultiExporter 正确指向异步版本");
        } else {
            println!("❌ MultiExporter 类型不正确: {}", multi_type);
        }
        println!();
    }

    #[cfg(not(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite"
    )))]
    println!("⚠️ 没有启用任何导出器功能，请使用 --features 参数启用");

    println!("✅ 模块分离测试完成！");
    println!("");
    println!("使用方法:");
    println!(
        "- 默认同步 CSV: cargo run --example test_default_feature --features exporter-csv"
    );
    println!(
        "- 异步 CSV: cargo run --example test_default_feature --features \"async,exporter-csv\""
    );
    println!(
        "- 默认同步 JSON: cargo run --example test_default_feature --features exporter-json"
    );
    println!(
        "- 异步 JSON: cargo run --example test_default_feature --features \"async,exporter-json\""
    );
    println!(
        "- 同步 SQLite: cargo run --example test_default_feature --features exporter-sqlite"
    );
    println!(
        "- 异步 SQLite: cargo run --example test_default_feature --features \"async,exporter-sqlite\""
    );
    println!(
        "- 全功能组合: cargo run --example test_default_feature --features \"exporter-csv,exporter-json,exporter-sqlite\""
    );
}
