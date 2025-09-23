//! 异步多导出器管理模块

use crate::error::Result;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use super::AsyncExporter;
use futures::future;

/// 异步多导出器管理器
pub struct AsyncMultiExporter {
    exporters: Vec<Box<dyn AsyncExporter>>,
    stats: Vec<ExportStats>,
}

impl AsyncMultiExporter {
    /// 创建新的多导出器管理器
    pub fn new() -> Self {
        Self { exporters: Vec::new(), stats: Vec::new() }
    }

    /// 添加导出器
    pub fn add_exporter<E>(&mut self, exporter: E)
    where
        E: AsyncExporter + 'static,
    {
        self.exporters.push(Box::new(exporter));
        self.stats.push(ExportStats::new());
    }

    /// 并发导出单个记录到所有导出器
    pub async fn export_record(&mut self, record: &Sqllog) -> Result<()> {
        let futures = self.exporters.iter_mut().enumerate().map(
            |(i, exporter)| async move {
                let result = exporter.export_record(record).await;
                (i, result)
            },
        );

        let results = future::join_all(futures).await;

        for (i, result) in results {
            if let Some(stats) = self.stats.get_mut(i) {
                match result {
                    Ok(_) => stats.exported_records += 1,
                    Err(_) => stats.failed_records += 1,
                }
            }
        }

        Ok(())
    }

    /// 并发批量导出到所有导出器
    pub async fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
        let futures = self.exporters.iter_mut().enumerate().map(
            |(i, exporter)| async move {
                let result = exporter.export_batch(records).await;
                (i, result)
            },
        );

        let results = future::join_all(futures).await;

        for (i, result) in results {
            if let Some(stats) = self.stats.get_mut(i) {
                match result {
                    Ok(_) => stats.exported_records += records.len(),
                    Err(_) => stats.failed_records += records.len(),
                }
            }
        }

        Ok(())
    }

    /// 完成所有导出器
    pub async fn finalize_all(&mut self) -> Result<()> {
        let futures = self.exporters.iter_mut().enumerate().map(
            |(i, exporter)| async move {
                let result = exporter.finalize().await;
                (i, result)
            },
        );

        let results = future::join_all(futures).await;

        for (i, _result) in results {
            if let Some(stats) = self.stats.get_mut(i) {
                stats.finish();
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

    /// 打印统计报告
    pub fn print_stats_report(&self) {
        println!("\n=== 异步导出统计报告 ===");
        for (name, stats) in self.get_all_stats() {
            println!("\n导出器: {}", name);
            println!("  {}", stats);
        }
        println!("========================\n");
    }
}

impl Default for AsyncMultiExporter {
    fn default() -> Self {
        Self::new()
    }
}
