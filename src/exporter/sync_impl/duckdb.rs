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
        let conn =
            Connection::open(db_path).map_err(|e| SqllogError::Duckdb)?;

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
        )
        .map_err(|e| SqllogError::Duckdb)?;

        Ok(Self {
            connection: std::sync::Mutex::new(conn),
            stats: ExportStats::new(),
            db_path: db_path.to_path_buf(),
        })
    }

    /// 插入记录到数据库 (同步版本)
    pub fn insert_records(
        &mut self,
        records: &[Sqllog],
    ) -> Result<(), SqllogError> {
        if records.is_empty() {
            return Ok(());
        }

        let mut conn =
            self.connection.lock().map_err(|e| SqllogError::Duckdb)?;

        let tx = conn.transaction().map_err(|e| SqllogError::Duckdb)?;

        // 预处理 SQL 语句
        let mut stmt = tx.prepare("
            INSERT INTO sqllogs (
                occurrence_time, ep, session, thread, user, trx_id, statement,
                appname, ip, sql_type, description, execute_time, rowcount, execute_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ").map_err(|e| SqllogError::Duckdb)?;

        // 批量执行
        for record in records {
            let params: [&dyn duckdb::ToSql; 14] = [
                &record.occurrence_time.to_string(),
                &record.ep.to_string(),
                &record.session,
                &record.thread,
                &record.user,
                &record.trx_id,
                &record.statement,
                &record.appname,
                &record.ip,
                &record.sql_type,
                &record.description,
                &record.execute_time,
                &record.rowcount,
                &record.execute_id,
            ];

            stmt.execute(params).map_err(|e| SqllogError::Duckdb)?;
        }

        // 先提交事务
        drop(stmt);
        tx.commit().map_err(|e| SqllogError::Duckdb)?;

        self.stats.exported_records += records.len();

        #[cfg(feature = "logging")]
        tracing::debug!("DuckDB插入 {} 条记录", records.len());

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
            conn.execute(index_sql, []).map_err(|e| SqllogError::Duckdb)?;
        }

        #[cfg(feature = "logging")]
        tracing::debug!("DuckDB 索引创建完成");

        Ok(())
    }
}

impl SyncExporter for SyncDuckdbExporter {
    fn name(&self) -> &str {
        "SyncDuckdbExporter"
    }

    fn export_batch(
        &mut self,
        records: &[Sqllog],
    ) -> Result<(), crate::error::SqllogError> {
        self.insert_records(records)
    }

    fn finalize(&mut self) -> Result<(), crate::error::SqllogError> {
        let mut conn =
            self.connection.lock().map_err(|e| SqllogError::Duckdb)?;
        Self::create_indexes_sync(&mut conn)?;
        Ok(())
    }

    fn get_stats(&self) -> crate::exporter::ExportStats {
        self.stats.clone()
    }
}
