//! SQLite 同步数据库导出器

use super::SyncExporter;
use crate::error::SqllogError;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use rusqlite::Connection;
use std::path::{Path, PathBuf};

/// SQLite 同步数据库导出器
pub struct SyncSqliteExporter {
    connection: std::sync::Mutex<Connection>,
    stats: ExportStats,
    db_path: PathBuf,
}

impl SyncSqliteExporter {
    /// 创建新的同步 SQLite 导出器 (单线程模式，直接写入主数据库)
    pub fn new(db_path: &Path) -> Result<Self, SqllogError> {
        Self::create_exporter(db_path)
    }

    /// 内部方法：创建导出器
    fn create_exporter(db_path: &Path) -> Result<Self, SqllogError> {
        #[cfg(feature = "logging")]
        tracing::info!("创建SQLite导出器: {}", db_path.display());

        let conn = Connection::open(db_path).map_err(SqllogError::Sqlite)?;

        #[cfg(feature = "logging")]
        tracing::debug!("SQLite连接已建立，开始创建表结构");

        // 仅创建表，索引将在 finalize 时创建以提高插入性能
        conn.execute(
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

        #[cfg(feature = "logging")]
        tracing::debug!("SQLite表结构创建完成");

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

        #[cfg(feature = "logging")]
        tracing::trace!("开始插入 {} 条SQLite记录", records.len());

        let mut conn = self.connection.lock().unwrap();
        let tx = conn.transaction().map_err(SqllogError::Sqlite)?;

        let mut successful = 0;
        let mut failed = 0;

        for record in records.iter() {
            let result = tx.execute(
                "INSERT INTO sqllogs (occurrence_time, ep, session, thread, user, trx_id, statement, appname, ip, sql_type, description, execute_time, rowcount, execute_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rusqlite::params![
                    record.occurrence_time,
                    record.ep,
                    record.session,
                    record.thread,
                    record.user,
                    record.trx_id,
                    record.statement,
                    record.appname,
                    record.ip,
                    record.sql_type,
                    record.description,
                    record.execute_time,
                    record.rowcount,
                    record.execute_id,
                ]
            );

            match result {
                Ok(_) => successful += 1,
                Err(e) => {
                    failed += 1;
                    #[cfg(feature = "logging")]
                    tracing::warn!("SQLite插入记录失败: {}", e);
                }
            }
        }

        tx.commit().map_err(SqllogError::Sqlite)?;

        #[cfg(feature = "logging")]
        tracing::trace!(
            "SQLite批次插入完成: 成功 {} 条, 失败 {} 条",
            successful,
            failed
        );

        self.stats.exported_records += successful;
        self.stats.failed_records += failed;

        Ok(())
    }
}

impl SyncExporter for SyncSqliteExporter {
    fn name(&self) -> &str {
        "SQLite"
    }

    fn export_record(&mut self, record: &Sqllog) -> Result<(), SqllogError> {
        self.export_batch(&[record.clone()])
    }

    fn export_batch(&mut self, records: &[Sqllog]) -> Result<(), SqllogError> {
        self.insert_records(records)
    }

    fn finalize(&mut self) -> Result<(), SqllogError> {
        // 只有主数据库（非临时数据库）才需要创建索引
        // 在数据导入完成后创建索引以提高导入性能
        let conn = self.connection.lock().unwrap();

        #[cfg(feature = "logging")]
        tracing::info!("开始创建 SQLite 索引...");

        // 创建索引以提高查询性能
        conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_occurrence_time ON sqllogs(occurrence_time)",
                [],
            )
            .map_err(SqllogError::Sqlite)?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_user ON sqllogs(user)",
            [],
        )
        .map_err(SqllogError::Sqlite)?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sql_type ON sqllogs(sql_type)",
            [],
        )
        .map_err(SqllogError::Sqlite)?;

        #[cfg(feature = "logging")]
        tracing::info!("SQLite 索引创建完成");

        self.stats.finish();

        #[cfg(feature = "logging")]
        tracing::info!(
            "同步SQLite导出完成: {} 条记录",
            self.stats.exported_records
        );

        Ok(())
    }

    fn get_stats(&self) -> ExportStats {
        self.stats.clone()
    }
}
