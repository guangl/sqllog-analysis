//! SQLite 异步数据库导出器

use super::AsyncExporter;
use crate::error::SqllogError;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use async_trait::async_trait;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// SQLite 异步数据库导出器
pub struct AsyncSqliteExporter {
    connection: Arc<Mutex<Connection>>,
    stats: ExportStats,
    db_path: PathBuf,
}

impl AsyncSqliteExporter {
    /// 创建新的 SQLite 导出器 (单线程模式，直接写入主数据库)
    pub async fn new(db_path: &Path) -> Result<Self, SqllogError> {
        Self::create_exporter(db_path).await
    }

    /// 内部方法：创建导出器
    async fn create_exporter(db_path: &Path) -> Result<Self, SqllogError> {
        let conn = Connection::open(db_path).map_err(SqllogError::Sqlite)?;

        let connection = Arc::new(Mutex::new(conn));

        // 初始化数据库结构和性能优化设置
        let conn_lock = connection.lock().await;

        // 仅创建表，索引将在 finalize 时创建以提高插入性能
        conn_lock
            .execute(
                r#"
            CREATE TABLE IF NOT EXISTS sqllogs (
                occurrence_time TEXT NOT NULL,
                ep TEXT,
                session TEXT,
                thread TEXT,
                user TEXT,
                trx_id TEXT,
                statement TEXT,
                appname TEXT,
                ip TEXT,
                sql_type TEXT,
                description TEXT NOT NULL,
                execute_time INTEGER,
                rowcount INTEGER,
                execute_id INTEGER
            )
            "#,
                [],
            )
            .map_err(SqllogError::Sqlite)?;

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

            // 设置性能优化参数
            conn.execute("PRAGMA synchronous = OFF", []).map_err(SqllogError::Sqlite)?;
            conn.execute("PRAGMA journal_mode = MEMORY", []).map_err(SqllogError::Sqlite)?;

            let tx = conn.transaction().map_err(SqllogError::Sqlite)?;

            // 预处理 SQL 语句
            let mut stmt = tx.prepare("
                INSERT INTO sqllogs (
                    occurrence_time, ep, session, thread, user, trx_id, statement,
                    appname, ip, sql_type, description, execute_time, rowcount, execute_id
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            ").map_err(SqllogError::Sqlite)?;

            // 批量执行，使用数组参数避免重复参数绑定开销
            for record in &records {
                let params: [&dyn rusqlite::ToSql; 14] = [
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

                stmt.execute(&params).map_err(SqllogError::Sqlite)?;
            }

            // 先提交事务
            drop(stmt);
            tx.commit().map_err(SqllogError::Sqlite)?;

            // 恢复默认设置
            conn.execute("PRAGMA synchronous = NORMAL", []).map_err(SqllogError::Sqlite)?;

            Ok::<usize, SqllogError>(records.len())
        }).await
        .map_err(|e| SqllogError::other(format!("Spawn blocking error: {}", e)))?;

        let record_count = result?;
        self.stats.exported_records += record_count;

        #[cfg(feature = "logging")]
        tracing::debug!("SQLite插入 {} 条记录", record_count);

        Ok(())
    }
}

#[async_trait]
impl AsyncExporter for AsyncSqliteExporter {
    fn name(&self) -> &str {
        "SQLite"
    }

    async fn export_record(
        &mut self,
        record: &Sqllog,
    ) -> Result<(), SqllogError> {
        self.insert_records(&[record.clone()]).await
    }

    async fn export_batch(
        &mut self,
        records: &[Sqllog],
    ) -> Result<(), SqllogError> {
        self.insert_records(records).await
    }

    async fn finalize(&mut self) -> Result<(), SqllogError> {
        // 只有主数据库（非临时数据库）才需要创建索引
        // 在数据导入完成后创建索引以提高导入性能
        let connection = Arc::clone(&self.connection);
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            let conn = rt.block_on(connection.lock());

            #[cfg(feature = "logging")]
            tracing::info!("开始创建 SQLite 索引...");

            // 创建索引以提高查询性能
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_occurrence_time ON sqllogs(occurrence_time)",
                [],
            ).map_err(SqllogError::Sqlite)?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sql_type ON sqllogs(sql_type)",
                [],
            ).map_err(SqllogError::Sqlite)?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_user ON sqllogs(user)",
                [],
            ).map_err(SqllogError::Sqlite)?;

            #[cfg(feature = "logging")]
            tracing::info!("SQLite 索引创建完成");

            Ok::<(), SqllogError>(())
        }).await
        .map_err(|e| SqllogError::other(format!("Spawn blocking error: {}", e)))??;

        self.stats.finish();

        #[cfg(feature = "logging")]
        tracing::info!(
            "SQLite导出完成: {} 条记录",
            self.stats.exported_records
        );

        Ok(())
    }

    fn get_stats(&self) -> ExportStats {
        self.stats.clone()
    }
}
