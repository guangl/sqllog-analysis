//! 数据导出模块
//!
//! 提供统一的数据导出接口和多种导出格式支持

// 重新导出核心组件
pub mod stats;
pub use stats::ExportStats;

// 同步实现子模块
pub mod sync_impl;

// 导出同步版本
#[cfg(feature = "exporter-csv")]
pub use sync_impl::SyncCsvExporter;
#[cfg(feature = "exporter-duckdb")]
pub use sync_impl::SyncDuckdbExporter;
#[cfg(feature = "exporter-json")]
pub use sync_impl::SyncJsonExporter;
#[cfg(feature = "exporter-sqlite")]
pub use sync_impl::SyncSqliteExporter;
pub use sync_impl::{SyncExporter, SyncMultiExporter};

// 为了向后兼容，提供默认别名
#[cfg(feature = "exporter-csv")]
pub use sync_impl::SyncCsvExporter as CsvExporter;
#[cfg(feature = "exporter-duckdb")]
pub use sync_impl::SyncDuckdbExporter as DuckdbExporter;
#[cfg(feature = "exporter-json")]
pub use sync_impl::SyncJsonExporter as JsonExporter;
pub use sync_impl::SyncMultiExporter as MultiExporter;
#[cfg(feature = "exporter-sqlite")]
pub use sync_impl::SyncSqliteExporter as SqliteExporter;
