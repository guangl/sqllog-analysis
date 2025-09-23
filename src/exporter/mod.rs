//! 数据导出模块
//!
//! 提供统一的数据导出接口和多种导出格式支持
//! 默认提供同步版本的导出器，可通过 "async" feature 启用异步版本

use crate::error::Result;
use crate::sqllog::types::Sqllog;

// 重新导出核心组件
pub mod stats;
pub use stats::ExportStats;

// 异步和同步实现子模块
#[cfg(feature = "async")]
pub mod async_impl;
pub mod sync_impl;

// 默认导出同步版本（条件编译）
#[cfg(feature = "exporter-csv")]
pub use sync_impl::SyncCsvExporter;
#[cfg(feature = "exporter-json")]
pub use sync_impl::SyncJsonExporter;
pub use sync_impl::SyncMultiExporter;
#[cfg(feature = "exporter-sqlite")]
pub use sync_impl::SyncSqliteExporter;

// 当启用 async feature 时导出异步版本
#[cfg(all(feature = "async", feature = "exporter-csv"))]
pub use async_impl::AsyncCsvExporter;
#[cfg(all(feature = "async", feature = "exporter-json"))]
pub use async_impl::AsyncJsonExporter;
#[cfg(feature = "async")]
pub use async_impl::AsyncMultiExporter;
#[cfg(all(feature = "async", feature = "exporter-sqlite"))]
pub use async_impl::AsyncSqliteExporter;

// 为了向后兼容，提供默认别名 (当没有启用 async 时)
#[cfg(all(not(feature = "async"), feature = "exporter-csv"))]
pub use sync_impl::SyncCsvExporter as CsvExporter;
#[cfg(all(not(feature = "async"), feature = "exporter-json"))]
pub use sync_impl::SyncJsonExporter as JsonExporter;
#[cfg(not(feature = "async"))]
pub use sync_impl::SyncMultiExporter as MultiExporter;
#[cfg(all(not(feature = "async"), feature = "exporter-sqlite"))]
pub use sync_impl::SyncSqliteExporter as SqliteExporter;

// 当启用 async feature 时，覆盖默认别名为异步版本
#[cfg(all(feature = "async", feature = "exporter-csv"))]
pub use async_impl::AsyncCsvExporter as CsvExporter;
#[cfg(all(feature = "async", feature = "exporter-json"))]
pub use async_impl::AsyncJsonExporter as JsonExporter;
#[cfg(feature = "async")]
pub use async_impl::AsyncMultiExporter as MultiExporter;
#[cfg(all(feature = "async", feature = "exporter-sqlite"))]
pub use async_impl::AsyncSqliteExporter as SqliteExporter;

// 条件导入 async-trait
#[cfg(feature = "async")]
use async_trait::async_trait;

// 重新导出子模块（条件编译）
#[cfg(feature = "exporter-excel")]
pub mod excel;

#[cfg(feature = "exporter-duckdb")]
pub mod duckdb;

#[cfg(feature = "exporter-postgres")]
pub mod postgres;

#[cfg(feature = "exporter-oracle")]
pub mod oracle;

/// 数据导出器的统一接口 (异步版本)
#[cfg(feature = "async")]
#[async_trait]
pub trait AsyncExporter: Send + Sync {
    /// 导出器名称
    fn name(&self) -> &str;

    /// 导出单个记录
    async fn export_record(&mut self, record: &Sqllog) -> Result<()>;

    /// 批量导出记录
    async fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
        for record in records {
            self.export_record(record).await?;
        }
        Ok(())
    }

    /// 完成导出，清理资源
    async fn finalize(&mut self) -> Result<()> {
        Ok(())
    }

    /// 获取导出统计信息
    fn get_stats(&self) -> ExportStats {
        ExportStats::default()
    }
}

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
