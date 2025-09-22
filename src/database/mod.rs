// 数据库模块 - 提供多数据库支持的抽象层
//
// 该模块提供：
// - 可扩展的数据库抽象接口
// - DuckDB 实现（支持内存和磁盘模式）
// - 批量数据插入功能
// - 多格式数据导出功能
// - 独立数据库并发处理

mod duckdb_impl;
mod types;

use crate::{config, sqllog::Sqllog};
use anyhow::Result;

pub use duckdb_impl::{
    DuckDbProvider, IndependentDatabaseStats,
    process_file_with_independent_database,
    process_files_with_independent_databases,
};
pub use types::*;

/// 数据库提供者抽象接口
///
/// 所有数据库实现都必须实现此 trait，提供统一的操作接口
pub trait DatabaseProvider: Send {
    /// 初始化数据库连接和表结构
    /// 初始化数据库
    ///
    /// # Errors
    /// 当数据库初始化失败时返回错误
    fn initialize(&mut self) -> Result<()>;

    /// 获取记录总数
    /// 统计记录数
    ///
    /// # Errors
    /// 当数据库查询失败时返回错误
    fn count_records(&self) -> Result<u64>;

    /// 导出数据到指定格式
    /// 导出数据到指定格式和路径
    ///
    /// # Errors
    /// 当文件导出失败时返回错误
    fn export_data(
        &self,
        format: ExportFormat,
        output_path: &str,
    ) -> Result<()>;

    /// 检查数据库是否已初始化
    fn is_initialized(&self) -> bool;

    /// 获取数据库类型信息
    fn database_info(&self) -> DatabaseInfo;

    /// 关闭数据库连接
    /// 关闭数据库连接
    ///
    /// # Errors
    /// 当数据库关闭失败时返回错误
    fn close(&mut self) -> Result<()>;

    /// 完成数据插入后创建索引优化查询性能
    /// 完成数据库架构设置
    ///
    /// # Errors
    /// 当数据库架构完成失败时返回错误
    fn finalize_schema(&mut self) -> Result<()> {
        // 默认实现：什么都不做
        Ok(())
    }
}

/// 简化的数据库管理器
///
/// 用于独立数据库处理
pub struct DatabaseManager {
    provider: DuckDbProvider,
}

impl DatabaseManager {
    /// 创建新的数据库管理器
    /// 创建新的数据库管理器
    ///
    /// # Errors
    /// 当数据库提供者创建失败时返回错误
    pub fn new(config: &config::RuntimeConfig) -> Result<Self> {
        let provider = DuckDbProvider::new(config)?;

        Ok(Self { provider })
    }

    /// 初始化数据库
    /// 初始化数据库
    ///
    /// # Errors
    /// 当数据库初始化失败时返回错误
    pub fn initialize(&mut self) -> Result<()> {
        self.provider.initialize()
    }

    /// 批量插入记录
    /// 批量插入记录
    ///
    /// # Errors
    /// 当批量插入失败时返回错误
    pub fn insert_batch(&mut self, records: &[Sqllog]) -> Result<usize> {
        self.provider.insert_batch(records)
    }

    /// 批量插入记录 (别名方法)
    /// 批量插入记录（别名方法）
    ///
    /// # Errors
    /// 当批量插入失败时返回错误
    pub fn batch_insert(&mut self, records: &[Sqllog]) -> Result<usize> {
        self.insert_batch(records)
    }

    /// 完成数据库架构设置
    /// 完成数据库架构设置
    ///
    /// # Errors
    /// 当数据库架构完成失败时返回错误
    pub fn finalize_schema(&mut self) -> Result<()> {
        self.provider.finalize_schema()
    }

    /// 获取记录总数
    /// 统计记录数
    ///
    /// # Errors
    /// 当数据库查询失败时返回错误
    pub fn count_records(&self) -> Result<u64> {
        self.provider.count_records()
    }

    /// 获取数据库信息
    pub fn database_info(&self) -> DatabaseInfo {
        self.provider.database_info()
    }

    /// 执行原始 SQL 语句（用于数据库合并等高级操作）
    /// 执行 SQL 语句
    ///
    /// # Errors
    /// 当 SQL 执行失败时返回错误
    pub fn execute_sql(&mut self, sql: &str) -> Result<()> {
        self.provider.execute_sql(sql)
    }
}
