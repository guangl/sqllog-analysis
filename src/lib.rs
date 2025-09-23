// 核心模块 - 始终可用
pub mod core;
pub mod sqllog;

// 处理模块 - 根据配置的功能启用
pub mod processing;

// 导出模块 - 需要任何导出功能
#[cfg(any(
    feature = "export-csv",
    feature = "export-json",
    feature = "export-sqlite",
    feature = "export-duckdb"
))]
pub mod export;
