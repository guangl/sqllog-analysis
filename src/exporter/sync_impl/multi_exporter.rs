//! 同步多导出器管理模块

use crate::error::Result;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use super::SyncExporter;

/// 同步多导出器管理器
pub struct SyncMultiExporter {
    exporters: Vec<Box<dyn SyncExporter>>,
    stats: Vec<ExportStats>,
}

impl SyncMultiExporter {
    /// 创建新的同步多导出器
    pub fn new() -> Self {
        Self { exporters: Vec::new(), stats: Vec::new() }
    }

    /// 添加导出器
    pub fn add_exporter<E>(&mut self, exporter: E)
    where
        E: SyncExporter + 'static,
    {
        self.exporters.push(Box::new(exporter));
        self.stats.push(ExportStats::new());
    }

    /// 导出单个记录到所有导出器
    pub fn export_record(&mut self, record: &Sqllog) -> Result<()> {
        for (i, exporter) in self.exporters.iter_mut().enumerate() {
            match exporter.export_record(record) {
                Ok(_) => {
                    if let Some(stats) = self.stats.get_mut(i) {
                        stats.exported_records += 1;
                    }
                }
                Err(_) => {
                    if let Some(stats) = self.stats.get_mut(i) {
                        stats.failed_records += 1;
                    }
                }
            }
        }
        Ok(())
    }

    /// 批量导出到所有导出器
    pub fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
        for record in records {
            self.export_record(record)?;
        }
        Ok(())
    }

    /// 完成所有导出器
    pub fn finalize_all(&mut self) -> Result<()> {
        for (i, exporter) in self.exporters.iter_mut().enumerate() {
            match exporter.finalize() {
                Ok(_) => {
                    if let Some(stats) = self.stats.get_mut(i) {
                        stats.finish();
                    }
                }
                Err(_) => {
                    if let Some(stats) = self.stats.get_mut(i) {
                        stats.failed_records += 1;
                    }
                }
            }
        }
        Ok(())
    }

    /// 获取所有导出器的统计信息
    pub fn get_all_stats(&self) -> Vec<(String, ExportStats)> {
        self.exporters
            .iter()
            .zip(self.stats.iter())
            .map(|(exporter, stats)| {
                (exporter.name().to_string(), stats.clone())
            })
            .collect()
    }

    /// 打印所有导出器的统计报告
    pub fn print_stats_report(&self) {
        println!("\n=== 同步导出统计报告 ===");
        for (name, stats) in self.get_all_stats() {
            println!("\n导出器: {}", name);
            println!("  {}", stats);
        }
        println!("======================\n");
    }
}

impl Default for SyncMultiExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqllog::types::Sqllog;

    struct DummyOk;

    impl SyncExporter for DummyOk {
        fn name(&self) -> &str { "OK" }
        fn export_record(&mut self, _record: &Sqllog) -> Result<()> { Ok(()) }
        fn finalize(&mut self) -> Result<()> { Ok(()) }
    }

    struct DummyErr;

    impl SyncExporter for DummyErr {
        fn name(&self) -> &str { "ERR" }
        fn export_record(&mut self, _record: &Sqllog) -> Result<()> { Err(crate::error::SqllogError::other("fail")) }
        fn finalize(&mut self) -> Result<()> { Err(crate::error::SqllogError::other("finalize fail")) }
    }

    #[test]
    fn test_multi_exporter_success_and_failure_paths() {
        let mut m = SyncMultiExporter::new();
        m.add_exporter(DummyOk);
        m.add_exporter(DummyErr);

        let r = Sqllog { occurrence_time: "t".into(), ep: "e".into(), ..Default::default() };
        // export_record should increment exported_records for first exporter and failed_records for second
        m.export_record(&r).unwrap();

        let stats = m.get_all_stats();
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].1.exported_records, 1);
        assert_eq!(stats[1].1.failed_records, 1);

        // finalize_all should call finalize; failure increments failed_records
        m.finalize_all().unwrap();
        let stats_after = m.get_all_stats();
        // first exporter should have end_time set (finish called)
        assert!(stats_after[0].1.end_time.is_some());
        // second exporter should have an extra failed record from finalize failure
        assert!(stats_after[1].1.failed_records >= 1);
        // ensure print_stats_report runs (prints to stdout)
        m.print_stats_report();
    }

    #[test]
    fn test_export_batch_propagates_to_all() {
        let mut m = SyncMultiExporter::new();
        m.add_exporter(DummyOk);
        m.add_exporter(DummyErr);

        let r1 = Sqllog { occurrence_time: "x".into(), ep: "e".into(), ..Default::default() };
        let r2 = Sqllog { occurrence_time: "y".into(), ep: "e".into(), ..Default::default() };

        m.export_batch(&[r1, r2]).unwrap();

        let stats = m.get_all_stats();
        assert_eq!(stats[0].1.exported_records, 2);
        assert_eq!(stats[1].1.failed_records, 2);
    }

    #[test]
    fn test_get_all_stats_names_and_print() {
        let mut m = SyncMultiExporter::new();
        m.add_exporter(DummyOk);
        m.add_exporter(DummyErr);

        let names: Vec<String> = m.get_all_stats().into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, vec!["OK".to_string(), "ERR".to_string()]);

        // ensure printing doesn't panic
        m.print_stats_report();
    }
}
