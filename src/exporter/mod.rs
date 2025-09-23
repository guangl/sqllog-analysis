//! 数据导出模块
//!
//! 提供统一的数据导出接口和多种导出格式支持
//! 默认提供同步版本的导出器，可通过 "async" feature 启用异步版本

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
#[cfg(feature = "exporter-duckdb")]
pub use sync_impl::SyncDuckdbExporter;
#[cfg(feature = "exporter-json")]
pub use sync_impl::SyncJsonExporter;
#[cfg(feature = "exporter-sqlite")]
pub use sync_impl::SyncSqliteExporter;
pub use sync_impl::{SyncExporter, SyncMultiExporter};

// 当启用 async feature 时导出异步版本
#[cfg(all(feature = "async", feature = "exporter-csv"))]
pub use async_impl::AsyncCsvExporter;
#[cfg(all(feature = "async", feature = "exporter-duckdb"))]
pub use async_impl::AsyncDuckdbExporter;
#[cfg(all(feature = "async", feature = "exporter-json"))]
pub use async_impl::AsyncJsonExporter;
#[cfg(all(feature = "async", feature = "exporter-sqlite"))]
pub use async_impl::AsyncSqliteExporter;
#[cfg(feature = "async")]
pub use async_impl::{AsyncExporter, AsyncMultiExporter};

// 为了向后兼容，提供默认别名 (当没有启用 async 时)
#[cfg(all(not(feature = "async"), feature = "exporter-csv"))]
pub use sync_impl::SyncCsvExporter as CsvExporter;
#[cfg(all(not(feature = "async"), feature = "exporter-duckdb"))]
pub use sync_impl::SyncDuckdbExporter as DuckdbExporter;
#[cfg(all(not(feature = "async"), feature = "exporter-json"))]
pub use sync_impl::SyncJsonExporter as JsonExporter;
#[cfg(not(feature = "async"))]
pub use sync_impl::SyncMultiExporter as MultiExporter;
#[cfg(all(not(feature = "async"), feature = "exporter-sqlite"))]
pub use sync_impl::SyncSqliteExporter as SqliteExporter;

// 当启用 async feature 时，覆盖默认别名为异步版本
#[cfg(all(feature = "async", feature = "exporter-csv"))]
pub use async_impl::AsyncCsvExporter as CsvExporter;
#[cfg(all(feature = "async", feature = "exporter-duckdb"))]
pub use async_impl::AsyncDuckdbExporter as DuckdbExporter;
#[cfg(all(feature = "async", feature = "exporter-json"))]
pub use async_impl::AsyncJsonExporter as JsonExporter;
#[cfg(feature = "async")]
pub use async_impl::AsyncMultiExporter as MultiExporter;
#[cfg(all(feature = "async", feature = "exporter-sqlite"))]
pub use async_impl::AsyncSqliteExporter as SqliteExporter;

// 重新导出子模块（条件编译）
