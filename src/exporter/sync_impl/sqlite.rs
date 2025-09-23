//! SQLite 同步数据库导出器

use crate::error::SqllogError;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use super::SyncExporter;
use rusqlite::Connection;
use std::path::{Path, PathBuf};

/// SQLite 同步数据库导出器
pub struct SyncSqliteExporter {
    connection: std::sync::Mutex<Connection>,
    stats: ExportStats,
    is_temp_db: bool,
    db_path: PathBuf,
}

impl SyncSqliteExporter {
    /// 创建新的同步 SQLite 导出器 (单线程模式，直接写入主数据库)
    pub fn new(db_path: &Path) -> Result<Self, SqllogError> {
        Self::create_exporter(db_path, false)
    }

    /// 创建临时数据库导出器 (多线程模式，写入临时数据库)
    pub fn new_temp(temp_db_path: &Path) -> Result<Self, SqllogError> {
        Self::create_exporter(temp_db_path, true)
    }

    /// 内部方法：创建导出器
    fn create_exporter(
        db_path: &Path,
        is_temp: bool,
    ) -> Result<Self, SqllogError> {
        let conn = Connection::open(db_path).map_err(SqllogError::Sqlite)?;

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

        Ok(Self {
            connection: std::sync::Mutex::new(conn),
            stats: ExportStats::new(),
            is_temp_db: is_temp,
            db_path: db_path.to_path_buf(),
        })
    }

    /// 合并多个临时数据库到主数据库 (同步版本)
    pub fn merge_temp_databases_sync(
        main_db_path: &Path,
        temp_db_paths: &[PathBuf],
    ) -> Result<(), SqllogError> {
        let mut main_conn =
            Connection::open(main_db_path).map_err(SqllogError::Sqlite)?;

        // 设置主数据库性能优化参数
        main_conn
            .execute("PRAGMA synchronous = OFF", [])
            .map_err(SqllogError::Sqlite)?;
        main_conn
            .execute("PRAGMA journal_mode = MEMORY", [])
            .map_err(SqllogError::Sqlite)?;

        // 创建主表
        main_conn
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

        #[cfg(feature = "logging")]
        tracing::info!("开始合并 {} 个临时数据库", temp_db_paths.len());

        let tx = main_conn.transaction().map_err(SqllogError::Sqlite)?;

        for (i, temp_db_path) in temp_db_paths.iter().enumerate() {
            let attach_name = format!("temp_db_{}", i);

            // 附加临时数据库
            tx.execute(
                &format!(
                    "ATTACH DATABASE '{}' AS {}",
                    temp_db_path.display(),
                    attach_name
                ),
                [],
            )
            .map_err(SqllogError::Sqlite)?;

            // 复制数据
            tx.execute(
                &format!(
                    "INSERT INTO main.sqllogs SELECT * FROM {}.sqllogs",
                    attach_name
                ),
                [],
            )
            .map_err(SqllogError::Sqlite)?;

            // 分离数据库
            tx.execute(&format!("DETACH DATABASE {}", attach_name), [])
                .map_err(SqllogError::Sqlite)?;

            #[cfg(feature = "logging")]
            tracing::debug!("已合并临时数据库: {}", temp_db_path.display());
        }

        tx.commit().map_err(SqllogError::Sqlite)?;

        #[cfg(feature = "logging")]
        tracing::info!("开始创建主数据库索引...");

        // 创建索引以提高查询性能
        main_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_occurrence_time ON sqllogs(occurrence_time)",
            [],
        ).map_err(SqllogError::Sqlite)?;

        main_conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_sql_type ON sqllogs(sql_type)",
                [],
            )
            .map_err(SqllogError::Sqlite)?;

        main_conn
            .execute("CREATE INDEX IF NOT EXISTS idx_user ON sqllogs(user)", [])
            .map_err(SqllogError::Sqlite)?;

        // 恢复默认设置
        main_conn
            .execute("PRAGMA synchronous = NORMAL", [])
            .map_err(SqllogError::Sqlite)?;

        #[cfg(feature = "logging")]
        tracing::info!("数据库合并完成，索引创建完成");

        Ok(())
    }

    /// 清理临时数据库文件 (同步版本)
    pub fn cleanup_temp_databases_sync(
        temp_db_paths: &[PathBuf],
    ) -> Result<(), SqllogError> {
        for temp_db_path in temp_db_paths {
            if temp_db_path.exists() {
                std::fs::remove_file(temp_db_path).map_err(|e| {
                    SqllogError::other(format!("删除临时文件失败: {}", e))
                })?;

                #[cfg(feature = "logging")]
                tracing::debug!("已删除临时数据库: {}", temp_db_path.display());
            }
        }
        Ok(())
    }

    /// 插入记录到数据库 (同步版本)
    pub fn insert_records(
        &mut self,
        records: &[Sqllog],
    ) -> Result<(), SqllogError> {
        if records.is_empty() {
            return Ok(());
        }

        let mut conn = self.connection.lock().unwrap();

        // 设置性能优化参数
        conn.execute("PRAGMA synchronous = OFF", [])
            .map_err(SqllogError::Sqlite)?;
        conn.execute("PRAGMA journal_mode = MEMORY", [])
            .map_err(SqllogError::Sqlite)?;

        let tx = conn.transaction().map_err(SqllogError::Sqlite)?;

        // 预处理 SQL 语句
        let mut stmt = tx
            .prepare(
                r#"
            INSERT INTO sqllogs (
                occurrence_time, ep, session, thread, user, trx_id, statement,
                appname, ip, sql_type, description, execute_time, rowcount, execute_id
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            "#,
            )
            .map_err(SqllogError::Sqlite)?;

        // 批量执行，使用数组参数避免重复参数绑定开销
        for record in records {
            let params: [&dyn rusqlite::ToSql; 14] = [
                &record.occurrence_time,
                &record.ep,
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
        conn.execute("PRAGMA synchronous = NORMAL", [])
            .map_err(SqllogError::Sqlite)?;

        self.stats.exported_records += records.len();

        #[cfg(feature = "logging")]
        tracing::debug!("SQLite插入 {} 条记录", records.len());

        Ok(())
    }
}

impl SyncExporter for SyncSqliteExporter {
    fn name(&self) -> &str {
        "SQLite"
    }

    fn export_record(&mut self, record: &Sqllog) -> Result<(), SqllogError> {
        self.insert_records(&[record.clone()])
    }

    fn export_batch(&mut self, records: &[Sqllog]) -> Result<(), SqllogError> {
        self.insert_records(records)
    }

    fn finalize(&mut self) -> Result<(), SqllogError> {
        // 只有主数据库（非临时数据库）才需要创建索引
        if !self.is_temp_db {
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
        }

        self.stats.finish();

        #[cfg(feature = "logging")]
        if self.is_temp_db {
            tracing::info!(
                "同步SQLite临时数据库导出完成: {} 条记录",
                self.stats.exported_records
            );
        } else {
            tracing::info!(
                "同步SQLite导出完成: {} 条记录",
                self.stats.exported_records
            );
        }

        Ok(())
    }

    fn get_stats(&self) -> ExportStats {
        self.stats.clone()
    }
}
