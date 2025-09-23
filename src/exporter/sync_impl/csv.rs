//! CSV 导出器实现 (同步版本)

use super::SyncExporter;
use crate::error::Result;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use std::io::{BufWriter, Write};
use std::path::Path;

/// 同步 CSV 导出器
pub struct SyncCsvExporter {
    writer: BufWriter<std::fs::File>,
    stats: ExportStats,
    header_written: bool,
}

impl SyncCsvExporter {
    /// 创建新的同步 CSV 导出器
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = std::fs::File::create(path)?;
        let writer = BufWriter::new(file);

        Ok(Self { writer, stats: ExportStats::new(), header_written: false })
    }

    /// 写入 CSV 头部
    fn write_header(&mut self) -> Result<()> {
        let header = "occurrence_time,ep,session,thread,user,trx_id,statement,appname,ip,sql_type,description,execute_time,rowcount,execute_id\n";
        self.writer.write_all(header.as_bytes())?;
        self.header_written = true;
        Ok(())
    }

    /// 转义 CSV 字段
    fn escape_csv_field(field: &str) -> String {
        if field.contains(',')
            || field.contains('"')
            || field.contains('\n')
            || field.contains('\r')
        {
            format!("\"{}\"", field.replace('"', "\"\""))
        } else {
            field.to_string()
        }
    }

    /// 格式化记录为 CSV 行
    fn format_record(&self, record: &Sqllog) -> String {
        let fields = [
            Self::escape_csv_field(&record.occurrence_time),
            Self::escape_csv_field(&record.ep),
            Self::escape_csv_field(
                &record.session.as_ref().unwrap_or(&String::new()),
            ),
            Self::escape_csv_field(
                &record.thread.as_ref().unwrap_or(&String::new()),
            ),
            Self::escape_csv_field(
                &record.user.as_ref().unwrap_or(&String::new()),
            ),
            Self::escape_csv_field(
                &record.trx_id.as_ref().unwrap_or(&String::new()),
            ),
            Self::escape_csv_field(
                &record.statement.as_ref().unwrap_or(&String::new()),
            ),
            Self::escape_csv_field(
                &record.appname.as_ref().unwrap_or(&String::new()),
            ),
            Self::escape_csv_field(
                &record.ip.as_ref().unwrap_or(&String::new()),
            ),
            Self::escape_csv_field(
                &record.sql_type.as_ref().unwrap_or(&String::new()),
            ),
            Self::escape_csv_field(&record.description),
            record.execute_time.map(|t| t.to_string()).unwrap_or_default(),
            record.rowcount.map(|r| r.to_string()).unwrap_or_default(),
            record.execute_id.map(|id| id.to_string()).unwrap_or_default(),
        ];

        format!("{}\n", fields.join(","))
    }

    /// 直接插入记录数组 (同步版本)
    pub fn insert_records(&mut self, records: &[Sqllog]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        if !self.header_written {
            self.write_header()?;
        }

        for record in records {
            let csv_line = self.format_record(record);
            self.writer.write_all(csv_line.as_bytes())?;
        }

        self.stats.exported_records += records.len();

        #[cfg(feature = "logging")]
        tracing::debug!("CSV批量导出: {} 条记录", records.len());

        Ok(())
    }
}

impl SyncExporter for SyncCsvExporter {
    fn name(&self) -> &str {
        "CSV"
    }

    fn export_record(&mut self, record: &Sqllog) -> Result<()> {
        if !self.header_written {
            self.write_header()?;
        }

        let csv_line = self.format_record(record);
        self.writer.write_all(csv_line.as_bytes())?;
        self.stats.exported_records += 1;

        Ok(())
    }

    fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
        self.insert_records(records)
    }

    fn finalize(&mut self) -> Result<()> {
        // 如果还没有写入头部，说明没有数据，但仍需要写入头部
        if !self.header_written {
            self.write_header()?;
        }

        self.writer.flush()?;
        self.stats.finish();

        #[cfg(feature = "logging")]
        tracing::info!("CSV导出完成: {} 条记录", self.stats.exported_records);

        Ok(())
    }

    fn get_stats(&self) -> ExportStats {
        self.stats.clone()
    }
}
