//! DuckDB 同步数据库导出器

use super::SyncExporter;
use crate::error::SqllogError;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use duckdb::Connection;
use std::path::{Path, PathBuf};

/// DuckDB 同步数据库导出器
pub struct SyncDuckdbExporter {
    connection: std::sync::Mutex<Connection>,
    stats: ExportStats,
    db_path: PathBuf,
}

impl SyncDuckdbExporter {
    /// 创建新的同步 DuckDB 导出器
    pub fn new(db_path: &Path) -> Result<Self, SqllogError> {
        let conn = Connection::open(db_path)?;

        // 仅创建表，索引将在 finalize 时创建以提高插入性能
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS sqllogs (
                occurrence_time VARCHAR NOT NULL,
                ep VARCHAR,
                session VARCHAR,
                thread VARCHAR,
                user VARCHAR,
                trx_id VARCHAR,
                statement TEXT,
                appname VARCHAR,
                ip VARCHAR,
                sql_type VARCHAR,
                description TEXT NOT NULL,
                execute_time INTEGER,
                rowcount INTEGER,
                execute_id INTEGER
            )
            "#,
            [],
        )?;

        Ok(Self {
            connection: std::sync::Mutex::new(conn),
            stats: ExportStats::new(),
            db_path: db_path.to_path_buf(),
        })
    }

    /// 插入记录到数据库 (同步版本) - 使用 append_rows 批量插入优化
    pub fn insert_records(
        &mut self,
        records: &[Sqllog],
    ) -> Result<(), SqllogError> {
        if records.is_empty() {
            return Ok(());
        }

        let conn = self.connection.lock().unwrap();

        // 使用 DuckDB 的 append_rows 方法进行批量插入
        let mut appender = conn.appender("sqllogs")?;

        // 准备批量数据：将所有记录转换为数组引用的向量
        let batch_data: Vec<[&dyn duckdb::ToSql; 14]> = records
            .iter()
            .map(|record| {
                [
                    &record.occurrence_time as &dyn duckdb::ToSql,
                    &record.ep as &dyn duckdb::ToSql,
                    &record.session as &dyn duckdb::ToSql,
                    &record.thread as &dyn duckdb::ToSql,
                    &record.user as &dyn duckdb::ToSql,
                    &record.trx_id as &dyn duckdb::ToSql,
                    &record.statement as &dyn duckdb::ToSql,
                    &record.appname as &dyn duckdb::ToSql,
                    &record.ip as &dyn duckdb::ToSql,
                    &record.sql_type as &dyn duckdb::ToSql,
                    &record.description as &dyn duckdb::ToSql,
                    &record.execute_time as &dyn duckdb::ToSql,
                    &record.rowcount as &dyn duckdb::ToSql,
                    &record.execute_id as &dyn duckdb::ToSql,
                ]
            })
            .collect();

        // 批量插入所有行
        appender.append_rows(batch_data.iter())?;
        appender.flush()?;

        // 显式释放appender
        drop(appender);
        drop(conn);

        self.stats.exported_records += records.len();

        Ok(())
    }

    /// 创建索引以提高查询性能 (同步版本)
    fn create_indexes_sync(conn: &mut Connection) -> Result<(), SqllogError> {
        let indexes = vec![
            "CREATE INDEX IF NOT EXISTS idx_sqllogs_occurrence_time ON sqllogs(occurrence_time)",
            "CREATE INDEX IF NOT EXISTS idx_sqllogs_user ON sqllogs(user)",
            "CREATE INDEX IF NOT EXISTS idx_sqllogs_sql_type ON sqllogs(sql_type)",
            "CREATE INDEX IF NOT EXISTS idx_sqllogs_execute_time ON sqllogs(execute_time)",
        ];

        for index_sql in indexes {
            conn.execute(index_sql, [])?;
        }

        #[cfg(feature = "logging")]
        tracing::debug!("DuckDB 索引创建完成");

        Ok(())
    }
}

impl SyncExporter for SyncDuckdbExporter {
    fn name(&self) -> &str {
        "DuckDB"
    }

    fn export_record(
        &mut self,
        record: &Sqllog,
    ) -> Result<(), crate::error::SqllogError> {
        self.export_batch(&[record.clone()])
    }

    fn export_batch(
        &mut self,
        records: &[Sqllog],
    ) -> Result<(), crate::error::SqllogError> {
        self.insert_records(records)
    }

    fn finalize(&mut self) -> Result<(), crate::error::SqllogError> {
        let mut conn = self.connection.lock().unwrap();
        Self::create_indexes_sync(&mut conn)?;
        Ok(())
    }

    fn get_stats(&self) -> crate::exporter::ExportStats {
        self.stats.clone()
    }
}
