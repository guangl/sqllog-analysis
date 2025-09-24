//! 同步导出器实现模块

use crate::error::Result;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;

/// 同步数据导出器的统一接口
pub trait SyncExporter: Send + Sync {
    /// 导出器名称
    fn name(&self) -> &str;

    /// 导出单个记录
    fn export_record(&mut self, record: &Sqllog) -> Result<()>;

    /// 批量导出记录
    fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
        for record in records {
            self.export_record(record)?;
        }
        Ok(())
    }

    /// 完成导出，清理资源
    fn finalize(&mut self) -> Result<()> {
        Ok(())
    }

    /// 获取导出统计信息
    fn get_stats(&self) -> ExportStats {
        ExportStats::default()
    }
}

#[cfg(feature = "exporter-csv")]
pub mod csv;
#[cfg(feature = "exporter-duckdb")]
pub mod duckdb;
#[cfg(feature = "exporter-json")]
pub mod json;
pub mod multi_exporter;
#[cfg(feature = "exporter-sqlite")]
pub mod sqlite;

#[cfg(feature = "exporter-csv")]
pub use csv::SyncCsvExporter;
#[cfg(feature = "exporter-duckdb")]
pub use duckdb::SyncDuckdbExporter;
#[cfg(feature = "exporter-json")]
pub use json::SyncJsonExporter;
pub use multi_exporter::SyncMultiExporter;
#[cfg(feature = "exporter-sqlite")]
pub use sqlite::SyncSqliteExporter;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqllog::types::Sqllog;

    // Dummy exporter that counts how many times export_record is called
    struct DummyCounter {
        pub count: usize,
    }

    impl DummyCounter {
        fn new() -> Self { Self { count: 0 } }
    }

    impl SyncExporter for DummyCounter {
        fn name(&self) -> &str { "DUMMY" }

        fn export_record(&mut self, _record: &Sqllog) -> crate::error::Result<()> {
            self.count += 1;
            Ok(())
        }
    }

    #[test]
    fn test_sync_exporter_default_export_batch() {
        let mut d = DummyCounter::new();
        let records = vec![Sqllog::default(), Sqllog::default(), Sqllog::default()];
        // call the default export_batch implementation provided by the trait
        d.export_batch(&records).unwrap();
        assert_eq!(d.count, 3);
    }

    #[test]
    fn test_sync_exporter_default_finalize_and_get_stats() {
        // Dummy exporter that doesn't override finalize or get_stats to exercise defaults
        struct DummyDefault;

        impl SyncExporter for DummyDefault {
            fn name(&self) -> &str { "DUMMY_DEF" }
            fn export_record(&mut self, _record: &Sqllog) -> crate::error::Result<()> { Ok(()) }
        }

        let mut d = DummyDefault;
        // default finalize should return Ok
        d.finalize().unwrap();

        // default get_stats should return default ExportStats
        let stats = d.get_stats();
        assert_eq!(stats.exported_records, 0);
        assert_eq!(stats.failed_records, 0);
    }
}
