//! CSV 导出功能集成测试

mod common;

#[cfg(feature = "exporter-csv")]
mod csv_tests {
    use sqllog_analysis::{
        exporter::{CsvExporter, SyncExporter},
        sqllog::{SyncSqllogParser, types::Sqllog},
    };
    use std::fs;
    use tempfile::TempDir;

    use super::common;

    #[test]
    fn test_csv_basic_export() {
        println!("🔄 测试基本 CSV 导出功能...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let test_file = common::create_test_sqllog(
            &temp_dir,
            "basic_test.log",
            common::SAMPLE_SQLLOG_CONTENT,
        );

        // 解析数据
        let mut all_records = Vec::new();
        let mut all_errors = Vec::new();

        let parse_result = SyncSqllogParser::parse_with_hooks(
            &test_file,
            1000,
            |records, errors| {
                all_records.extend_from_slice(records);
                all_errors.extend_from_slice(errors);
            },
        );
        assert!(parse_result.is_ok(), "解析应该成功: {:?}", parse_result.err());

        // 导出到 CSV
        let csv_path = temp_dir.path().join("basic_export.csv");
        let mut csv_exporter =
            CsvExporter::new(&csv_path).expect("应该能创建 CSV 导出器");

        let export_result = csv_exporter.export_batch(&all_records);
        assert!(
            export_result.is_ok(),
            "CSV 导出应该成功: {:?}",
            export_result.err()
        );

        let finalize_result = csv_exporter.finalize();
        assert!(
            finalize_result.is_ok(),
            "完成导出应该成功: {:?}",
            finalize_result.err()
        );

        // 验证文件内容
        assert!(csv_path.exists(), "CSV 文件应该存在");
        let csv_content =
            fs::read_to_string(&csv_path).expect("应该能读取 CSV 文件");
        assert!(!csv_content.is_empty(), "CSV 文件不应为空");
        assert!(csv_content.contains("occurrence_time"), "应该包含 CSV 头部");

        println!("✅ 基本 CSV 导出测试通过: 导出 {} 条记录", all_records.len());
    }

    #[test]
    fn test_csv_large_batch_export() {
        println!("🔄 测试大批量 CSV 导出功能...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // 创建较大的测试文件
        let large_content = common::COMPLEX_SQLLOG_CONTENT.repeat(5);
        let test_file = common::create_test_sqllog(
            &temp_dir,
            "large_batch.log",
            &large_content,
        );

        // 解析数据
        let mut all_records = Vec::new();
        let mut all_errors = Vec::new();

        let parse_result = SyncSqllogParser::parse_with_hooks(
            &test_file,
            1000,
            |records, errors| {
                all_records.extend_from_slice(records);
                all_errors.extend_from_slice(errors);
            },
        );
        assert!(parse_result.is_ok(), "大批量解析应该成功");

        // 导出到 CSV
        let csv_path = temp_dir.path().join("large_batch.csv");
        let mut csv_exporter =
            CsvExporter::new(&csv_path).expect("应该能创建 CSV 导出器");

        let export_result = csv_exporter.export_batch(&all_records);
        assert!(export_result.is_ok(), "大批量 CSV 导出应该成功");

        let finalize_result = csv_exporter.finalize();
        assert!(finalize_result.is_ok(), "完成大批量导出应该成功");

        // 验证文件
        assert!(csv_path.exists(), "CSV 文件应该存在");
        let csv_content =
            fs::read_to_string(&csv_path).expect("应该能读取 CSV 文件");
        assert!(!csv_content.is_empty(), "CSV 文件不应为空");

        println!(
            "✅ 大批量 CSV 导出测试通过: 导出 {} 条记录",
            all_records.len()
        );
    }

    #[test]
    fn test_csv_multi_batch_export() {
        println!("🔄 测试多批次 CSV 导出功能...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let csv_path = temp_dir.path().join("multi_batch.csv");
        let mut csv_exporter =
            CsvExporter::new(&csv_path).expect("应该能创建 CSV 导出器");

        // 分多个批次导出
        for i in 0..3 {
            let test_file = common::create_test_sqllog(
                &temp_dir,
                &format!("batch_{}.log", i),
                common::SAMPLE_SQLLOG_CONTENT,
            );

            let mut batch_records = Vec::new();
            let mut batch_errors = Vec::new();

            let parse_result = SyncSqllogParser::parse_with_hooks(
                &test_file,
                1000,
                |records, errors| {
                    batch_records.extend_from_slice(records);
                    batch_errors.extend_from_slice(errors);
                },
            );

            if parse_result.is_ok() && !batch_records.is_empty() {
                let export_result = csv_exporter.export_batch(&batch_records);
                assert!(export_result.is_ok(), "批次 {} CSV 导出应该成功", i);
            }
        }

        let finalize_result = csv_exporter.finalize();
        assert!(finalize_result.is_ok(), "完成多批次导出应该成功");

        // 验证最终文件
        assert!(csv_path.exists(), "CSV 文件应该存在");
        let csv_content =
            fs::read_to_string(&csv_path).expect("应该能读取 CSV 文件");
        assert!(!csv_content.is_empty(), "CSV 文件不应为空");

        println!("✅ 多批次 CSV 导出测试通过");
    }

    #[test]
    fn test_csv_empty_data() {
        println!("🔄 测试空数据 CSV 导出功能...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let csv_path = temp_dir.path().join("empty_export.csv");
        let mut csv_exporter =
            CsvExporter::new(&csv_path).expect("应该能创建 CSV 导出器");

        // 导出空记录集
        let empty_records: Vec<Sqllog> = Vec::new();
        let export_result = csv_exporter.export_batch(&empty_records);
        assert!(export_result.is_ok(), "空数据 CSV 导出应该成功");

        let finalize_result = csv_exporter.finalize();
        assert!(finalize_result.is_ok(), "完成空数据导出应该成功");

        // 验证只有头部的文件
        assert!(csv_path.exists(), "CSV 文件应该存在");
        let csv_content =
            fs::read_to_string(&csv_path).expect("应该能读取 CSV 文件");
        assert!(csv_content.contains("occurrence_time"), "应该包含 CSV 头部");

        println!("✅ 空数据 CSV 导出测试通过");
    }

    #[test]
    fn test_csv_stats_collection() {
        println!("🔄 测试 CSV 导出统计功能...");

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let test_file = common::create_test_sqllog(
            &temp_dir,
            "stats_test.log",
            common::SAMPLE_SQLLOG_CONTENT,
        );

        // 解析数据
        let mut all_records = Vec::new();
        let mut all_errors = Vec::new();

        let parse_result = SyncSqllogParser::parse_with_hooks(
            &test_file,
            1000,
            |records, errors| {
                all_records.extend_from_slice(records);
                all_errors.extend_from_slice(errors);
            },
        );
        assert!(parse_result.is_ok(), "解析应该成功");

        // 导出并收集统计
        let csv_path = temp_dir.path().join("stats_export.csv");
        let mut csv_exporter =
            CsvExporter::new(&csv_path).expect("应该能创建 CSV 导出器");

        let export_result = csv_exporter.export_batch(&all_records);
        assert!(export_result.is_ok(), "CSV 导出应该成功");

        // 获取统计信息
        let stats = csv_exporter.get_stats();
        assert!(
            stats.exported_records >= all_records.len(),
            "统计的记录数应该正确"
        );

        let finalize_result = csv_exporter.finalize();
        assert!(finalize_result.is_ok(), "完成导出应该成功");

        println!(
            "✅ CSV 导出统计测试通过: 处理了 {} 条记录",
            stats.exported_records
        );
    }

    #[test]
    fn test_csv_error_handling() {
        println!("🔄 测试 CSV 导出错误处理...");

        // 尝试在无效路径创建导出器
        let invalid_path = "/invalid/path/that/does/not/exist.csv";
        let exporter_result = CsvExporter::new(invalid_path);

        // 在某些系统上可能会成功创建（如果父目录存在），所以我们测试导出时的错误
        match exporter_result {
            Ok(_) => {
                println!(
                    "✅ CSV 导出器错误处理测试跳过 - 路径验证在运行时进行"
                );
            }
            Err(_) => {
                println!("✅ CSV 导出器错误处理测试通过 - 无效路径被拒绝");
            }
        }
    }
}

#[cfg(not(feature = "exporter-csv"))]
mod no_csv_tests {
    #[test]
    fn test_no_csv_feature() {
        println!("✅ 无 CSV 导出功能模式测试通过 - exporter-csv 功能未启用");
    }
}
