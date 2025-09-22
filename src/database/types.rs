// 数据库模块类型定义
//
// 定义数据库相关的枚举、结构体和常量

use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// 支持的数据库类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DatabaseType {
    /// `DuckDB` 数据库（内存或磁盘模式）
    #[default]
    DuckDb,
    // 未来可扩展其他数据库类型
    // Sqlite,
    // PostgreSql,
    // MySql,
}

/// 数据导出格式
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportFormat {
    /// JSON 格式 (.json)
    Json,
    /// CSV 格式 (.csv)
    Csv,
}

impl FromStr for ExportFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "csv" => Ok(Self::Csv),
            _ => Err(format!("不支持的导出格式: {s}")),
        }
    }
}

impl ExportFormat {
    /// 获取文件扩展名
    /// 获取文件扩展名
    #[must_use]
    pub const fn extension(&self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Csv => "csv",
        }
    }

    /// 获取 MIME 类型
    /// 获取 MIME 类型
    #[must_use]
    pub const fn mime_type(&self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Csv => "text/csv",
        }
    }
}

/// 数据库连接信息
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseMode {
    /// 内存模式 - 数据存储在内存中，程序结束后丢失
    InMemory,
    /// 磁盘模式 - 数据持久化到指定文件
    Disk { path: String },
}

/// 数据库信息
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseInfo {
    /// 数据库类型
    pub db_type: DatabaseType,
    /// 数据库模式（内存/磁盘）
    pub mode: DatabaseMode,
    /// 是否已初始化
    pub initialized: bool,
    /// 数据库版本信息
    pub version: Option<String>,
    /// 记录总数
    pub record_count: u64,
}

impl DatabaseInfo {
    /// 创建新的数据库信息
    #[must_use]
    pub const fn new(db_type: DatabaseType, mode: DatabaseMode) -> Self {
        Self {
            db_type,
            mode,
            initialized: false,
            version: None,
            record_count: 0,
        }
    }

    /// 更新记录数
    pub fn update_count(&mut self, count: u64) {
        self.record_count = count;
    }

    /// 标记为已初始化
    pub fn mark_initialized(&mut self, version: Option<String>) {
        self.initialized = true;
        self.version = version;
    }
}

/// 数据库操作统计信息
#[derive(Debug, Default, Clone)]
pub struct DatabaseStats {
    /// 插入的记录总数
    pub inserted_records: usize,
    /// 插入操作次数
    pub insert_operations: usize,
    /// 总耗时（毫秒）
    pub total_duration_ms: u64,
    /// 平均每次插入时间（毫秒）
    pub avg_duration_ms: u64,
}

impl DatabaseStats {
    /// 添加插入操作统计
    pub fn add_insert(&mut self, records: usize, duration_ms: u64) {
        self.inserted_records += records;
        self.insert_operations += 1;
        self.total_duration_ms += duration_ms;

        // 重新计算平均时间
        if self.insert_operations > 0 {
            self.avg_duration_ms =
                self.total_duration_ms / self.insert_operations as u64;
        }
    }

    /// 获取每秒插入记录数
    /// 计算记录处理速度（记录/秒）
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn records_per_second(&self) -> f64 {
        if self.total_duration_ms > 0 {
            let records_f64 = self.inserted_records as f64;
            let duration_f64 = self.total_duration_ms as f64;

            (records_f64 * 1000.0) / duration_f64
        } else {
            0.0
        }
    }
}
