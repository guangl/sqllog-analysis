//! DuckDB 异步数据库导出器

use super::AsyncExporter;
use crate::error::SqllogError;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use async_trait::async_trait;
use duckdb::Connection;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// DuckDB 异步数据库导出器
pub struct AsyncDuckdbExporter {
    connection: Arc<Mutex<Connection>>,
    stats: ExportStats,
    db_path: PathBuf,
}

impl AsyncDuckdbExporter {
    /// 创建新的 DuckDB 导出器
    pub async fn new(db_path: &Path) -> Result<Self, SqllogError> {
        let conn = Connection::open(db_path).map_err(|e| {
            SqllogError::other(format!("DuckDB connection error: {}", e))
        })?;

        let connection = Arc::new(Mutex::new(conn));

        // 初始化数据库结构和性能优化设置
        let conn_lock = connection.lock().await;

        // 仅创建表，索引将在 finalize 时创建以提高插入性能
        conn_lock
            .execute(
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
            .map_err(|e| {
                SqllogError::other(format!(
                    "DuckDB table creation error: {}",
                    e
                ))
            })?;

        drop(conn_lock);

        Ok(Self {
            connection,
            stats: ExportStats::new(),
            db_path: db_path.to_path_buf(),
        })
    }

    /// 插入记录到数据库
    pub async fn insert_records(
        &mut self,
        records: &[Sqllog],
    ) -> Result<(), SqllogError> {
        if records.is_empty() {
            return Ok(());
        }

        let records = records.to_vec();
        let connection = Arc::clone(&self.connection);

        // 在阻塞任务中执行数据库操作
        let result = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            let mut conn = rt.block_on(connection.lock());

            let tx = conn.transaction()
                .map_err(|e| SqllogError::other(format!("DuckDB transaction error: {}", e)))?;

            // 预处理 SQL 语句
            let mut stmt = tx.prepare("
                INSERT INTO sqllogs (
                    occurrence_time, ep, session, thread, user, trx_id, statement,
                    appname, ip, sql_type, description, execute_time, rowcount, execute_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ").map_err(|e| SqllogError::other(format!("DuckDB prepare statement error: {}", e)))?;

            // 批量执行
            for record in &records {
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

                stmt.execute(params)
                    .map_err(|e| SqllogError::other(format!("DuckDB execute statement error: {}", e)))?;
            }

            // 先提交事务
            drop(stmt);
            tx.commit()
                .map_err(|e| SqllogError::other(format!("DuckDB transaction commit error: {}", e)))?;

            Ok::<usize, SqllogError>(records.len())
        }).await
        .map_err(|e| SqllogError::other(format!("Spawn blocking error: {}", e)))?;

        let record_count = result?;
        self.stats.exported_records += record_count;

        #[cfg(feature = "logging")]
        tracing::debug!("DuckDB插入 {} 条记录", record_count);

        Ok(())
    }

    /// 创建索引以提高查询性能
    async fn create_indexes(conn: &mut Connection) -> Result<(), SqllogError> {
        let indexes = vec![
            "CREATE INDEX IF NOT EXISTS idx_sqllogs_occurrence_time ON sqllogs(occurrence_time)",
            "CREATE INDEX IF NOT EXISTS idx_sqllogs_user ON sqllogs(user)",
            "CREATE INDEX IF NOT EXISTS idx_sqllogs_sql_type ON sqllogs(sql_type)",
            "CREATE INDEX IF NOT EXISTS idx_sqllogs_execute_time ON sqllogs(execute_time)",
        ];

        for index_sql in indexes {
            conn.execute(index_sql, []).map_err(|e| {
                SqllogError::other(format!(
                    "DuckDB index creation error: {}",
                    e
                ))
            })?;
        }

        #[cfg(feature = "logging")]
        tracing::debug!("DuckDB 索引创建完成");

        Ok(())
    }
}

#[async_trait]
impl AsyncExporter for AsyncDuckdbExporter {
    fn name(&self) -> &str {
        "AsyncDuckdbExporter"
    }

    async fn export_record(
        &mut self,
        record: &Sqllog,
    ) -> Result<(), crate::error::SqllogError> {
        self.insert_records(&[record.clone()]).await
    }

    async fn export_batch(
        &mut self,
        records: &[Sqllog],
    ) -> Result<(), crate::error::SqllogError> {
        self.insert_records(records).await
    }

    async fn finalize(&mut self) -> Result<(), crate::error::SqllogError> {
        let connection = Arc::clone(&self.connection);
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            let mut conn = rt.block_on(connection.lock());
            let _ = Self::create_indexes(&mut conn);
        })
        .await
        .map_err(|e| {
            SqllogError::other(format!("Finalize spawn blocking error: {}", e))
        })?;
        Ok(())
    }

    fn get_stats(&self) -> crate::exporter::ExportStats {
        self.stats.clone()
    }
}
