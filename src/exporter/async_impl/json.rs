//! JSON 导出器实现 (异步版本)

use crate::error::Result;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use super::AsyncExporter;
use async_trait::async_trait;
use serde_json;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter as AsyncBufWriter};

/// JSON 导出器 (异步版本)
pub struct AsyncJsonExporter {
    writer: AsyncBufWriter<File>,
    stats: ExportStats,
    first_record: bool,
}

impl AsyncJsonExporter {
    /// 创建新的 JSON 导出器
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path).await?;
        let mut writer = AsyncBufWriter::new(file);

        // 写入 JSON 数组开始符
        writer.write_all(b"[\n").await?;

        Ok(Self { writer, stats: ExportStats::new(), first_record: true })
    }

    /// 将记录转换为 JSON 值
    fn record_to_json(&self, record: &Sqllog) -> Result<String> {
        let json_value = serde_json::json!({
            "occurrence_time": record.occurrence_time,
            "ep": record.ep,
            "session": record.session,
            "thread": record.thread,
            "user": record.user,
            "trx_id": record.trx_id,
            "statement": record.statement,
            "appname": record.appname,
            "ip": record.ip,
            "sql_type": record.sql_type,
            "description": record.description,
            "execute_time": record.execute_time,
            "rowcount": record.rowcount,
            "execute_id": record.execute_id
        });

        Ok(serde_json::to_string_pretty(&json_value)?)
    }

    /// 直接插入记录数组 (异步版本)
    pub async fn insert_records(&mut self, records: &[Sqllog]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        for record in records {
            // 如果不是第一条记录，添加逗号分隔符
            if !self.first_record {
                self.writer.write_all(b",\n").await?;
            } else {
                self.first_record = false;
            }

            let json_str = self.record_to_json(record)?;

            // 缩进 JSON 对象
            let indented = json_str
                .lines()
                .map(|line| format!("  {}", line))
                .collect::<Vec<_>>()
                .join("\n");

            self.writer.write_all(indented.as_bytes()).await?;
        }

        self.stats.exported_records += records.len();

        #[cfg(feature = "logging")]
        tracing::debug!("JSON批量导出: {} 条记录", records.len());

        Ok(())
    }
}

#[async_trait]
impl AsyncExporter for AsyncJsonExporter {
    fn name(&self) -> &str {
        "JSON"
    }

    async fn export_record(&mut self, record: &Sqllog) -> Result<()> {
        // 如果不是第一条记录，添加逗号分隔符
        if !self.first_record {
            self.writer.write_all(b",\n").await?;
        } else {
            self.first_record = false;
        }

        let json_str = self.record_to_json(record)?;

        // 缩进 JSON 对象
        let indented = json_str
            .lines()
            .map(|line| format!("  {}", line))
            .collect::<Vec<_>>()
            .join("\n");

        self.writer.write_all(indented.as_bytes()).await?;
        self.stats.exported_records += 1;

        #[cfg(feature = "logging")]
        if self.stats.exported_records % 1000 == 0 {
            tracing::debug!(
                "JSON导出进度: {} 条记录",
                self.stats.exported_records
            );
        }

        Ok(())
    }

    async fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
        self.insert_records(records).await
    }

    async fn finalize(&mut self) -> Result<()> {
        // 写入 JSON 数组结束符
        self.writer.write_all(b"\n]\n").await?;
        self.writer.flush().await?;
        self.stats.finish();

        #[cfg(feature = "logging")]
        tracing::info!("JSON导出完成: {} 条记录", self.stats.exported_records);

        Ok(())
    }

    fn get_stats(&self) -> ExportStats {
        self.stats.clone()
    }
}
