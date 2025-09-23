//! CSV 导出器实现 (异步版本)

use crate::error::Result;
use crate::exporter::{AsyncExporter, ExportStats};
use crate::sqllog::types::Sqllog;
use async_trait::async_trait;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter as AsyncBufWriter};

/// CSV 导出器 (异步版本)
pub struct AsyncCsvExporter {
    writer: AsyncBufWriter<File>,
    stats: ExportStats,
    header_written: bool,
}

impl AsyncCsvExporter {
    /// 创建新的 CSV 导出器
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path).await?;
        let writer = AsyncBufWriter::new(file);

        Ok(Self { writer, stats: ExportStats::new(), header_written: false })
    }

    /// 写入 CSV 头部
    async fn write_header(&mut self) -> Result<()> {
        let header = "occurrence_time,ep,session,thread,user,trx_id,statement,appname,ip,sql_type,description,execute_time,rowcount,execute_id\n";
        self.writer.write_all(header.as_bytes()).await?;
        self.header_written = true;
        Ok(())
    }

    /// 转义 CSV 字段
    fn escape_csv_field(field: &str) -> String {
        if field.contains(',') || field.contains('"') || field.contains('\n') {
            format!("\"{}\"", field.replace('"', "\"\""))
        } else {
            field.to_string()
        }
    }

    /// 将记录转换为 CSV 行
    fn record_to_csv_line(&self, record: &Sqllog) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            Self::escape_csv_field(&record.occurrence_time),
            Self::escape_csv_field(&record.ep),
            Self::escape_csv_field(&record.session.as_deref().unwrap_or("")),
            Self::escape_csv_field(&record.thread.as_deref().unwrap_or("")),
            Self::escape_csv_field(&record.user.as_deref().unwrap_or("")),
            Self::escape_csv_field(&record.trx_id.as_deref().unwrap_or("")),
            Self::escape_csv_field(&record.statement.as_deref().unwrap_or("")),
            Self::escape_csv_field(&record.appname.as_deref().unwrap_or("")),
            Self::escape_csv_field(&record.ip.as_deref().unwrap_or("")),
            Self::escape_csv_field(&record.sql_type.as_deref().unwrap_or("")),
            Self::escape_csv_field(&record.description),
            record.execute_time.unwrap_or(0),
            record.rowcount.unwrap_or(0),
            record.execute_id.unwrap_or(0)
        )
    }

    /// 直接插入记录数组 (异步版本)
    pub async fn insert_records(&mut self, records: &[Sqllog]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        if !self.header_written {
            self.write_header().await?;
        }

        let mut buffer = String::new();
        for record in records {
            buffer.push_str(&self.record_to_csv_line(record));
        }

        self.writer.write_all(buffer.as_bytes()).await?;
        self.stats.exported_records += records.len();

        #[cfg(feature = "logging")]
        tracing::debug!("CSV批量导出: {} 条记录", records.len());

        Ok(())
    }
}

#[async_trait]
impl AsyncExporter for AsyncCsvExporter {
    fn name(&self) -> &str {
        "CSV"
    }

    async fn export_record(&mut self, record: &Sqllog) -> Result<()> {
        if !self.header_written {
            self.write_header().await?;
        }

        let line = self.record_to_csv_line(record);
        self.writer.write_all(line.as_bytes()).await?;
        self.stats.exported_records += 1;

        #[cfg(feature = "logging")]
        if self.stats.exported_records % 1000 == 0 {
            tracing::debug!(
                "CSV导出进度: {} 条记录",
                self.stats.exported_records
            );
        }

        Ok(())
    }

    async fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
        self.insert_records(records).await
    }

    async fn finalize(&mut self) -> Result<()> {
        self.writer.flush().await?;
        self.stats.finish();

        #[cfg(feature = "logging")]
        tracing::info!("CSV导出完成: {} 条记录", self.stats.exported_records);

        Ok(())
    }

    fn get_stats(&self) -> ExportStats {
        self.stats.clone()
    }
}