//! DuckDB 同步数据库导出器

use crate::error::SqllogError;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use super::SyncExporter;
use duckdb::Connection;
use std::path::{Path, PathBuf};

/// DuckDB 同步数据库导出器
pub struct SyncDuckdbExporter {
    connection: std::sync::Mutex<Connection>,
    stats: ExportStats,
    is_temp_db: bool,
    db_path: PathBuf,
}

impl SyncDuckdbExporter {
    /// 创建新的同步 DuckDB 导出器 (单线程模式，直接写入主数据库)
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
        let conn = Connection::open(db_path)
            .map_err(|e| SqllogError::other(format!("DuckDB connection error: {}", e)))?;

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
        .map_err(|e| SqllogError::other(format!("DuckDB table creation error: {}", e)))?;

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
        let mut main_conn = Connection::open(main_db_path)
            .map_err(|e| SqllogError::other(format!("DuckDB main connection error: {}", e)))?;

        // 创建主表
        main_conn
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
            .map_err(|e| SqllogError::other(format!("DuckDB main table creation error: {}", e)))?;

        // 合并每个临时数据库
        for temp_db_path in temp_db_paths {
            if !temp_db_path.exists() {
                #[cfg(feature = "logging")]
                tracing::warn!("临时数据库文件不存在: {}", temp_db_path.display());
                continue;
            }

            // 附加临时数据库并复制数据
            let attach_sql = format!(
                "ATTACH DATABASE '{}' AS temp_db",
                temp_db_path.display()
            );

            main_conn
                .execute(&attach_sql, [])
                .map_err(|e| SqllogError::other(format!("DuckDB attach database error: {}", e)))?;

            // 复制数据
            main_conn
                .execute(
                    "INSERT INTO sqllogs SELECT * FROM temp_db.sqllogs",
                    [],
                )
                .map_err(|e| SqllogError::other(format!("DuckDB data copy error: {}", e)))?;

            // 分离临时数据库
            main_conn
                .execute("DETACH DATABASE temp_db", [])
                .map_err(|e| SqllogError::other(format!("DuckDB detach database error: {}", e)))?;

            #[cfg(feature = "logging")]
            tracing::info!("已合并临时数据库: {}", temp_db_path.display());
        }

        // 创建索引以提高查询性能
        Self::create_indexes_sync(&mut main_conn)?;

        #[cfg(feature = "logging")]
        tracing::info!("DuckDB 数据库合并完成: {}", main_db_path.display());

        // 删除临时文件
        for temp_db_path in temp_db_paths {
            if temp_db_path.exists() {
                let _ = std::fs::remove_file(temp_db_path);
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

        let mut conn = self.connection.lock()
            .map_err(|e| SqllogError::other(format!("DuckDB connection lock error: {}", e)))?;

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

            stmt.execute(params)
                .map_err(|e| SqllogError::other(format!("DuckDB execute statement error: {}", e)))?;
        }

        // 先提交事务
        drop(stmt);
        tx.commit()
            .map_err(|e| SqllogError::other(format!("DuckDB transaction commit error: {}", e)))?;

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
            conn.execute(index_sql, [])
                .map_err(|e| SqllogError::other(format!("DuckDB index creation error: {}", e)))?;
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

    fn export_record(&mut self, record: &Sqllog) -> Result<(), crate::error::SqllogError> {
        self.insert_records(&[record.clone()])
    }

    fn export_batch(&mut self, records: &[Sqllog]) -> Result<(), crate::error::SqllogError> {
        self.insert_records(records)
    }

    fn finalize(&mut self) -> Result<(), crate::error::SqllogError> {
        if !self.is_temp_db {
            let mut conn = self.connection.lock()
                .map_err(|e| SqllogError::other(format!("DuckDB connection lock error: {}", e)))?;
            Self::create_indexes_sync(&mut conn)?;
        }
        Ok(())
    }

    fn get_stats(&self) -> crate::exporter::ExportStats {
        self.stats.clone()
    }
}