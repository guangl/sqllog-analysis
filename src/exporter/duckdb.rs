//! DuckDB 导出器实现

use crate::error::Result;
use crate::exporter::{ExportStats, Exporter};
use crate::sqllog::types::Sqllog;
use async_trait::async_trait;
use std::path::Path;
use tokio::task;

/// DuckDB 导出器
pub struct DuckdbExporter {
    path: std::path::PathBuf,
    connection: Option<duckdb::Connection>,
    stats: ExportStats,
    batch_size: usize,
    batch_buffer: Vec<Sqllog>,
}

impl DuckdbExporter {
    /// 创建新的 DuckDB 导出器
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let path_clone = path_buf.clone();

        let connection = task::spawn_blocking(move || -> Result<duckdb::Connection> {
            let conn = duckdb::Connection::open(path_clone)?;

            // 创建表结构
            conn.execute(
                "CREATE TABLE IF NOT EXISTS sqllog (
                    id INTEGER PRIMARY KEY,
                    occurrence_time TIMESTAMP NOT NULL,
                    session_id VARCHAR NOT NULL,
                    thread_id VARCHAR NOT NULL,
                    user_name VARCHAR NOT NULL,
                    trx_id VARCHAR NOT NULL,
                    statement_id VARCHAR NOT NULL,
                    sql_type VARCHAR,
                    sql_text TEXT NOT NULL,
                    exec_time_ms INTEGER,
                    row_count INTEGER,
                    exec_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )",
                [],
            )?;

            // 创建索引
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_occurrence_time ON sqllog(occurrence_time)",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sql_type ON sqllog(sql_type)",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_session_id ON sqllog(session_id)",
                [],
            )?;

            Ok(conn)
        }).await??;

        Ok(Self {
            path: path_buf,
            connection: Some(connection),
            stats: ExportStats::new(),
            batch_size: 1000,
            batch_buffer: Vec::new(),
        })
    }

    /// 设置批次大小
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// 刷新批次缓冲区
    async fn flush_batch(&mut self) -> Result<()> {
        if self.batch_buffer.is_empty() {
            return Ok(());
        }

        let records = std::mem::take(&mut self.batch_buffer);
        let records_count = records.len();
        let connection = self.connection.take().unwrap();

        let connection =
            task::spawn_blocking(move || -> Result<duckdb::Connection> {
                let tx = connection.transaction()?;

                {
                    let mut stmt = tx.prepare(
                        "INSERT INTO sqllog (
                        occurrence_time, session_id, thread_id, user_name,
                        trx_id, statement_id, sql_type, sql_text,
                        exec_time_ms, row_count, exec_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    )?;

                    for record in records {
                        let exec_time: Option<i32> =
                            record.exec_time_ms.map(|v| v as i32);
                        let row_count: Option<i32> =
                            record.row_count.map(|v| v as i32);
                        let exec_id: Option<i32> =
                            record.exec_id.map(|v| v as i32);

                        stmt.execute(duckdb::params![
                            record
                                .occurrence_time
                                .format("%Y-%m-%d %H:%M:%S%.3f")
                                .to_string(),
                            record.session_id,
                            record.thread_id,
                            record.user_name,
                            record.trx_id,
                            record.statement_id,
                            record.sql_type,
                            record.sql_text,
                            exec_time,
                            row_count,
                            exec_id,
                        ])?;
                    }
                }

                tx.commit()?;
                Ok(connection)
            })
            .await??;

        self.connection = Some(connection);
        self.stats.exported_records += records_count;

        #[cfg(feature = "logging")]
        tracing::debug!("DuckDB批次写入: {} 条记录", records_count);

        Ok(())
    }
}

#[async_trait]
impl Exporter for DuckdbExporter {
    fn name(&self) -> &str {
        "DuckDB"
    }

    async fn export_record(&mut self, record: &Sqllog) -> Result<()> {
        self.batch_buffer.push(record.clone());

        if self.batch_buffer.len() >= self.batch_size {
            self.flush_batch().await?;
        }

        #[cfg(feature = "logging")]
        if (self.stats.exported_records + self.batch_buffer.len()) % 1000 == 0 {
            tracing::debug!(
                "DuckDB导出进度: {} 条记录",
                self.stats.exported_records + self.batch_buffer.len()
            );
        }

        Ok(())
    }

    async fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
        for record in records {
            self.batch_buffer.push(record.clone());

            if self.batch_buffer.len() >= self.batch_size {
                self.flush_batch().await?;
            }
        }

        #[cfg(feature = "logging")]
        tracing::debug!("DuckDB批量缓存: {} 条记录", records.len());

        Ok(())
    }

    async fn finalize(&mut self) -> Result<()> {
        // 刷新剩余的记录
        self.flush_batch().await?;
        self.stats.finish();

        #[cfg(feature = "logging")]
        tracing::info!(
            "DuckDB导出完成: {} 条记录",
            self.stats.exported_records
        );

        Ok(())
    }

    fn get_stats(&self) -> ExportStats {
        let mut stats = self.stats.clone();
        // 包含缓冲区中的记录数
        stats.exported_records += self.batch_buffer.len();
        stats
    }
}
