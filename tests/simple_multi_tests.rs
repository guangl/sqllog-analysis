//! 简化的多导出器和统计模块测试

#[cfg(any(feature = "exporter-csv", feature = "exporter-json"))]
mod simple_multi_tests {
    use sqllog_analysis::{
        exporter::{ExportStats, SyncMultiExporter},
        sqllog::types::Sqllog,
    };

    // 创建测试用的Sqllog记录
    fn create_test_record(id: u32) -> Sqllog {
        Sqllog {
            occurrence_time: format!("2025-09-16 20:02:53.{:03}", 562 + id),
            ep: format!("EP[{}]", id % 10),
            session: Some(format!("0x6da8ccef{}", id)),
            thread: Some((4146217 + id).to_string()),
            user: Some("EDM_BASE".to_string()),
            trx_id: Some((122154453026 + id as u64).to_string()),
            statement: Some(format!("0x6da900ef{}", id)),
            appname: Some("test_app".to_string()),
            ip: Some("127.0.0.1".to_string()),
            sql_type: Some("SEL".to_string()),
            description: format!("Test SQL {}", id),
            execute_time: Some(10 + id as i64),
            rowcount: Some(1 + id as i64),
            execute_id: Some(id as i64),
        }
    }

    #[test]
    fn test_multi_exporter_creation() {
        let multi_exporter = SyncMultiExporter::new();

        // 验证创建成功
        let stats = multi_exporter.get_all_stats();
        assert_eq!(stats.len(), 0); // 初始没有导出器
    }

    #[test]
    fn test_multi_exporter_empty_record_export() {
        let mut multi_exporter = SyncMultiExporter::new();

        let record = create_test_record(1);
        let result = multi_exporter.export_record(&record);

        // 没有导出器也应该成功
        assert!(result.is_ok());
    }

    #[test]
    fn test_multi_exporter_empty_finalize() {
        let mut multi_exporter = SyncMultiExporter::new();

        let result = multi_exporter.finalize_all();
        assert!(result.is_ok()); // 空导出器集合也应该能成功finalize
    }

    #[test]
    fn test_export_stats_creation() {
        let stats = ExportStats::new();

        assert_eq!(stats.exported_records, 0);
        assert_eq!(stats.failed_records, 0);
        assert!(stats.start_time.is_some());
        assert!(stats.end_time.is_none());
    }

    #[test]
    fn test_export_stats_manual_operations() {
        let mut stats = ExportStats::new();

        // 手动增加记录数
        stats.exported_records += 1;
        assert_eq!(stats.exported_records, 1);

        stats.exported_records += 2;
        assert_eq!(stats.exported_records, 3);

        // 手动增加失败数
        stats.failed_records += 1;
        assert_eq!(stats.failed_records, 1);
    }

    #[test]
    fn test_export_stats_timing() {
        let mut stats = ExportStats::new();

        // 添加小延迟确保时间差
        std::thread::sleep(std::time::Duration::from_millis(1));

        // 完成导出
        stats.finish();

        assert!(stats.end_time.is_some());
        assert!(stats.duration().is_some());
    // duration() is optional; just assert it's present to avoid a useless non-negative comparison
    assert!(stats.duration().is_some());
    }

    #[test]
    fn test_export_stats_success_rate() {
        let mut stats = ExportStats::new();

        // 3成功，1失败
        stats.exported_records = 3;
        stats.failed_records = 1;

        assert_eq!(stats.success_rate(), 75.0);

        // 全部成功
        stats.failed_records = 0;
        assert_eq!(stats.success_rate(), 100.0);

        // 没有记录时
        stats.exported_records = 0;
        assert_eq!(stats.success_rate(), 0.0);
    }

    #[test]
    fn test_export_stats_total_records() {
        let mut stats = ExportStats::new();

        stats.exported_records = 5;
        stats.failed_records = 3;

        assert_eq!(stats.total_records(), 8);
    }

    #[test]
    fn test_export_stats_merge() {
        let mut stats1 = ExportStats::new();
        let mut stats2 = ExportStats::new();

        // 为第一个统计添加数据
        stats1.exported_records = 2;
        stats1.failed_records = 1;

        // 为第二个统计添加数据
        stats2.exported_records = 3;
        stats2.failed_records = 2;

        // 合并统计
        stats1.merge(&stats2);

        assert_eq!(stats1.exported_records, 5); // 2 + 3
        assert_eq!(stats1.failed_records, 3); // 1 + 2
    }

    #[test]
    fn test_export_stats_reset() {
        let mut stats = ExportStats::new();

        stats.exported_records = 10;
        stats.failed_records = 2;
        stats.finish();

        // 重置统计
        stats.reset();

        assert_eq!(stats.exported_records, 0);
        assert_eq!(stats.failed_records, 0);
        assert!(stats.start_time.is_some());
        assert!(stats.end_time.is_none());
    }

    #[test]
    fn test_export_stats_display() {
        let mut stats = ExportStats::new();

        stats.exported_records = 2;
        stats.failed_records = 1;
        stats.finish();

        let display_str = format!("{}", stats);

        assert!(display_str.contains("成功: 2"));
        assert!(display_str.contains("失败: 1"));
        assert!(display_str.contains("成功率: 66.7%"));
    }

    #[test]
    fn test_export_stats_records_per_second() {
        let mut stats = ExportStats::new();

        stats.exported_records = 100;

        // 人工设置时间间隔
        let start_time =
            std::time::Instant::now() - std::time::Duration::from_secs(1);
        stats.start_time = Some(start_time);
        stats.finish();

        let rps = stats.records_per_second();
        assert!(rps.is_some());

        // 应该大约是100 records/second（允许一些误差）
        let rps_value = rps.unwrap();
        assert!(rps_value >= 90.0 && rps_value <= 110.0);
    }

    #[test]
    fn test_multi_exporter_batch_export() {
        let mut multi_exporter = SyncMultiExporter::new();

        let records: Vec<Sqllog> = (1..=5).map(create_test_record).collect();
        let result = multi_exporter.export_batch(&records);

        // 没有实际的导出器，但批量导出应该成功
        assert!(result.is_ok());
    }

    #[test]
    fn test_export_stats_edge_cases() {
        let stats = ExportStats::new();

        // 零记录的情况
        assert_eq!(stats.success_rate(), 0.0);
        assert_eq!(stats.total_records(), 0);
        assert!(stats.records_per_second().is_none());
    }

    #[test]
    fn test_export_stats_merge_timing() {
        let start1 = std::time::Instant::now();
        let start2 = start1 + std::time::Duration::from_millis(100);
        let end1 = start2 + std::time::Duration::from_millis(50);
        let end2 = end1 + std::time::Duration::from_millis(100);

        let mut stats1 = ExportStats::new();
        let mut stats2 = ExportStats::new();

        stats1.start_time = Some(start1);
        stats1.end_time = Some(end1);

        stats2.start_time = Some(start2);
        stats2.end_time = Some(end2);

        stats1.merge(&stats2);

        // 应该保持最早的开始时间和最晚的结束时间
        assert_eq!(stats1.start_time, Some(start1));
        assert_eq!(stats1.end_time, Some(end2));
    }
}
