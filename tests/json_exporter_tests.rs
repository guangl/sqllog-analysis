//! JSON导出器专门测试

#[cfg(feature = "exporter-json")]
mod json_exporter_tests {
    use sqllog_analysis::{
        exporter::{SyncJsonExporter, SyncExporter},
        sqllog::types::Sqllog,
    };
    use tempfile::tempdir;
    use std::fs;
    use serde_json::Value;

    // 创建测试用的Sqllog记录
    fn create_test_record(id: u32) -> Sqllog {
        Sqllog {
            occurrence_time: format!("2025-09-16 20:02:53.{:03}", 562 + id),
            ep: format!("EP[{}]", id % 10),
            session: Some(format!("0x6da8ccef{:03}", id)),
            thread: Some((4146217 + id).to_string()),
            user: Some("EDM_BASE".to_string()),
            trx_id: Some((122154453026 + id as u64).to_string()),
            statement: Some(format!("0x6da900ef{:03}", id)),
            appname: Some("test_app".to_string()),
            ip: Some("192.168.1.1".to_string()),
            sql_type: Some("SEL".to_string()),
            description: format!("JSON test SQL {}", id),
            execute_time: Some(15 + id as i64),
            rowcount: Some(5 + id as i64),
            execute_id: Some(id as i64),
        }
    }

    #[test]
    fn test_json_exporter_creation() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("test.json");

        let result = SyncJsonExporter::new(&json_path);
        assert!(result.is_ok());
    }

    #[test]
    fn test_json_exporter_single_record() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("single.json");

        let mut exporter = SyncJsonExporter::new(&json_path).unwrap();
        let record = create_test_record(1);

        exporter.export_record(&record).unwrap();
        exporter.finalize().unwrap();

        // 验证文件创建
        assert!(json_path.exists());

        // 验证JSON内容
        let content = fs::read_to_string(&json_path).unwrap();
        let json_value: Value = serde_json::from_str(&content).unwrap();

        let array = json_value.as_array().unwrap();
        assert_eq!(array.len(), 1);

        let record_json = &array[0];
        assert_eq!(record_json["occurrence_time"], "2025-09-16 20:02:53.563");
        assert_eq!(record_json["ep"], "EP[1]");
    }

    #[test]
    fn test_json_exporter_multiple_records() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("multiple.json");

        let mut exporter = SyncJsonExporter::new(&json_path).unwrap();

        // 导出多条记录
        for i in 1..=5 {
            let record = create_test_record(i);
            exporter.export_record(&record).unwrap();
        }

        exporter.finalize().unwrap();

        // 验证内容
        let content = fs::read_to_string(&json_path).unwrap();
        let json_value: Value = serde_json::from_str(&content).unwrap();

        let array = json_value.as_array().unwrap();
        assert_eq!(array.len(), 5);

        // 验证记录顺序
        for (index, record_json) in array.iter().enumerate() {
            let expected_id = index + 1;
            assert_eq!(record_json["execute_id"], expected_id);
        }
    }

    #[test]
    fn test_json_exporter_empty_fields() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("empty_fields.json");

        let mut exporter = SyncJsonExporter::new(&json_path).unwrap();

        // 创建包含空字段的记录
        let record = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.000".to_string(),
            ep: "EP[0]".to_string(),
            session: None,
            thread: None,
            user: None,
            trx_id: None,
            statement: None,
            appname: None,
            ip: None,
            sql_type: None,
            description: "Empty fields test".to_string(),
            execute_time: None,
            rowcount: None,
            execute_id: None,
        };

        exporter.export_record(&record).unwrap();
        exporter.finalize().unwrap();

        // 验证JSON处理空字段
        let content = fs::read_to_string(&json_path).unwrap();
        let json_value: Value = serde_json::from_str(&content).unwrap();

        let array = json_value.as_array().unwrap();
        let record_json = &array[0];

        assert_eq!(record_json["occurrence_time"], "2025-09-16 20:02:53.000");
        assert_eq!(record_json["description"], "Empty fields test");
        assert!(record_json["session"].is_null());
        assert!(record_json["execute_time"].is_null());
    }

    #[test]
    fn test_json_exporter_stats() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("stats.json");

        let mut exporter = SyncJsonExporter::new(&json_path).unwrap();

        // 初始状态
        let stats = exporter.get_stats();
        assert_eq!(stats.exported_records, 0);

        // 导出记录
        for i in 1..=10 {
            let record = create_test_record(i);
            exporter.export_record(&record).unwrap();
        }

        let stats = exporter.get_stats();
        assert_eq!(stats.exported_records, 10);

        exporter.finalize().unwrap();
    }

    #[test]
    fn test_json_exporter_large_batch() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("large_batch.json");

        let mut exporter = SyncJsonExporter::new(&json_path).unwrap();

        // 导出大批量数据
        for i in 1..=1000 {
            let record = create_test_record(i);
            exporter.export_record(&record).unwrap();
        }

        exporter.finalize().unwrap();

        // 验证大批量导出
        let content = fs::read_to_string(&json_path).unwrap();
        let json_value: Value = serde_json::from_str(&content).unwrap();

        let array = json_value.as_array().unwrap();
        assert_eq!(array.len(), 1000);

        // 验证统计信息
        let stats = exporter.get_stats();
        assert_eq!(stats.exported_records, 1000);
    }

    #[test]
    fn test_json_exporter_record_format() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("format_test.json");

        let mut exporter = SyncJsonExporter::new(&json_path).unwrap();

        // 创建包含所有字段的记录
        let record = Sqllog {
            occurrence_time: "2025-09-16 20:02:53.999".to_string(),
            ep: "EP[9]".to_string(),
            session: Some("0x6da8cceff".to_string()),
            thread: Some("9999".to_string()),
            user: Some("TEST_USER".to_string()),
            trx_id: Some("999999999999".to_string()),
            statement: Some("0x6da900eff".to_string()),
            appname: Some("format_app".to_string()),
            ip: Some("10.0.0.1".to_string()),
            sql_type: Some("UPD".to_string()),
            description: "Format validation SQL".to_string(),
            execute_time: Some(500),
            rowcount: Some(25),
            execute_id: Some(999),
        };

        exporter.export_record(&record).unwrap();
        exporter.finalize().unwrap();

        // 验证JSON格式和字段
        let content = fs::read_to_string(&json_path).unwrap();
        let json_value: Value = serde_json::from_str(&content).unwrap();

        let array = json_value.as_array().unwrap();
        let record_json = &array[0];

        assert_eq!(record_json["occurrence_time"], "2025-09-16 20:02:53.999");
        assert_eq!(record_json["ep"], "EP[9]");
        assert_eq!(record_json["session"], "0x6da8cceff");
        assert_eq!(record_json["thread"], "9999");
        assert_eq!(record_json["user"], "TEST_USER");
        assert_eq!(record_json["trx_id"], "999999999999");
        assert_eq!(record_json["statement"], "0x6da900eff");
        assert_eq!(record_json["appname"], "format_app");
        assert_eq!(record_json["ip"], "10.0.0.1");
        assert_eq!(record_json["sql_type"], "UPD");
        assert_eq!(record_json["description"], "Format validation SQL");
        assert_eq!(record_json["rowcount"], 25);
        assert_eq!(record_json["execute_id"], 999);
        assert_eq!(record_json["execute_time"], 500);
    }

    #[test]
    fn test_json_exporter_pretty_format() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("pretty.json");

        let mut exporter = SyncJsonExporter::new(&json_path).unwrap();

        let record = create_test_record(1);
        exporter.export_record(&record).unwrap();
        exporter.finalize().unwrap();

        // 验证JSON是格式化的（包含换行和缩进）
        let content = fs::read_to_string(&json_path).unwrap();

        // 应该包含适当的格式化
        assert!(content.contains("  {"));  // 有缩进
        assert!(content.contains("  \"occurrence_time\""));  // 字段有缩进
        assert!(content.contains("\n"));  // 有换行

        // 验证整体结构
        assert!(content.starts_with("[\n"));
        assert!(content.ends_with("\n]\n"));
    }

    #[test]
    fn test_json_exporter_finalize_without_records() {
        let temp_dir = tempdir().unwrap();
        let json_path = temp_dir.path().join("empty.json");

        let mut exporter = SyncJsonExporter::new(&json_path).unwrap();

        // 直接完成，不导出任何记录
        let result = exporter.finalize();
        assert!(result.is_ok());

        // 验证空JSON数组
        let content = fs::read_to_string(&json_path).unwrap();
        assert_eq!(content, "[\n\n]\n");  // 实际格式有额外换行

        // 验证可以解析为有效JSON
        let json_value: Value = serde_json::from_str(&content).unwrap();
        assert!(json_value.is_array());
        assert_eq!(json_value.as_array().unwrap().len(), 0);
    }
}