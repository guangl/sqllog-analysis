//! 同步多导出器管理模块

use crate::error::Result;
use crate::exporter::{ExportStats, SyncExporter};
use crate::sqllog::types::Sqllog;

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
