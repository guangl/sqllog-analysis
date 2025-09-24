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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::NamedTempFile;

    #[test]
    fn test_escape_csv_field_various() {
        assert_eq!(SyncCsvExporter::escape_csv_field("simple"), "simple");
        assert_eq!(
            SyncCsvExporter::escape_csv_field("with,comma"),
            "\"with,comma\""
        );
        assert_eq!(
            SyncCsvExporter::escape_csv_field("quote\"here"),
            "\"quote\"\"here\""
        );
        assert_eq!(
            SyncCsvExporter::escape_csv_field("line\nbreak"),
            "\"line\nbreak\""
        );
    }

    #[test]
    fn test_write_header_on_finalize_and_export() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut exporter = SyncCsvExporter::new(path).unwrap();
        // finalize when no records -> should write header
        exporter.finalize().unwrap();

        let mut s = String::new();
        tmp.reopen().unwrap().read_to_string(&mut s).unwrap();
        assert!(s.contains("occurrence_time,ep,session"));

        // now test export_record writes header and a record
        let tmp2 = NamedTempFile::new().unwrap();
        let path2 = tmp2.path();
        let mut exporter2 = SyncCsvExporter::new(path2).unwrap();

        let record = Sqllog {
            occurrence_time: "t".into(),
            ep: "e".into(),
            description: "d".into(),
            ..Default::default()
        };

        exporter2.export_record(&record).unwrap();
        exporter2.finalize().unwrap();

        let mut s2 = String::new();
        tmp2.reopen().unwrap().read_to_string(&mut s2).unwrap();
        assert!(s2.contains("occurrence_time,ep,session"));
        assert!(s2.contains("t"));
    }

    #[test]
    fn test_insert_records_empty_and_format_optional_fields() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut exporter = SyncCsvExporter::new(path).unwrap();

        // insert empty slice -> no header written, file stays empty
        exporter.insert_records(&[]).unwrap();
        let mut s = String::new();
        tmp.reopen().unwrap().read_to_string(&mut s).unwrap();
        assert!(s.is_empty());

        // now test format_record with optional numeric fields present
        let exporter2 = SyncCsvExporter::new(path).unwrap();
        let record = Sqllog {
            occurrence_time: "t".into(),
            ep: "e".into(),
            execute_time: Some(123),
            rowcount: Some(5),
            execute_id: Some(7),
            ..Default::default()
        };

        let line = exporter2.format_record(&record);
        assert!(line.contains("123"));
        assert!(line.contains("5"));
        assert!(line.contains("7"));
    }

    #[test]
    fn test_export_batch_writes_multiple_records() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut exporter = SyncCsvExporter::new(path).unwrap();

        let r1 = Sqllog {
            occurrence_time: "a".into(),
            ep: "e".into(),
            ..Default::default()
        };
        let r2 = Sqllog {
            occurrence_time: "b".into(),
            ep: "e".into(),
            ..Default::default()
        };

        exporter.export_batch(&[r1, r2]).unwrap();
        exporter.finalize().unwrap();

        let mut s = String::new();
        tmp.reopen().unwrap().read_to_string(&mut s).unwrap();
        // header should be present and two records should be written
        assert!(s.contains("occurrence_time,ep,session"));
        assert!(s.contains("a"));
        assert!(s.contains("b"));
    }

    #[test]
    fn test_export_record_writes_header_once() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut exporter = SyncCsvExporter::new(path).unwrap();

        let r1 = Sqllog {
            occurrence_time: "p".into(),
            ep: "e".into(),
            ..Default::default()
        };
        let r2 = Sqllog {
            occurrence_time: "q".into(),
            ep: "e".into(),
            ..Default::default()
        };

        exporter.export_record(&r1).unwrap();
        exporter.export_record(&r2).unwrap();
        exporter.finalize().unwrap();

        let mut s = String::new();
        tmp.reopen().unwrap().read_to_string(&mut s).unwrap();
        // header should only appear once
        let header_count = s.matches("occurrence_time,ep,session").count();
        assert_eq!(header_count, 1);
        assert!(s.contains("p"));
        assert!(s.contains("q"));
    }

    #[test]
    fn test_finalize_writes_header_when_never_written() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut exporter = SyncCsvExporter::new(path).unwrap();
        // never exported any record, header_written should be false
        exporter.finalize().unwrap();

        let mut s = String::new();
        tmp.reopen().unwrap().read_to_string(&mut s).unwrap();
        assert!(s.contains("occurrence_time,ep,session"));
    }
}
