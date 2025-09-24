//! JSON 导出器实现 (同步版本)

use crate::error::Result;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use super::SyncExporter;
use serde_json;
use std::io::{BufWriter, Write};
use std::path::Path;

/// 同步 JSON 导出器
pub struct SyncJsonExporter {
    writer: BufWriter<std::fs::File>,
    stats: ExportStats,
    first_record: bool,
}

impl SyncJsonExporter {
    /// 创建新的同步 JSON 导出器
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = std::fs::File::create(path)?;
        let mut writer = BufWriter::new(file);

        // 写入 JSON 数组开始符
        writer.write_all(b"[\n")?;

        Ok(Self { writer, stats: ExportStats::new(), first_record: true })
    }

    /// 将记录转换为 JSON 字符串
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

    /// 直接插入记录数组 (同步版本)
    pub fn insert_records(&mut self, records: &[Sqllog]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        for record in records {
            // 如果不是第一个记录，添加逗号分隔符
            if !self.first_record {
                self.writer.write_all(b",\n")?;
            } else {
                self.first_record = false;
            }

            let json_str = self.record_to_json(record)?;

            // 为每行添加适当的缩进
            let indented_json = json_str
                .lines()
                .map(|line| format!("  {}", line))
                .collect::<Vec<_>>()
                .join("\n");

            self.writer.write_all(indented_json.as_bytes())?;
        }

        self.stats.exported_records += records.len();

        #[cfg(feature = "logging")]
        tracing::debug!("JSON批量导出: {} 条记录", records.len());

        Ok(())
    }
}

impl SyncExporter for SyncJsonExporter {
    fn name(&self) -> &str {
        "JSON"
    }

    fn export_record(&mut self, record: &Sqllog) -> Result<()> {
        // 如果不是第一个记录，添加逗号分隔符
        if !self.first_record {
            self.writer.write_all(b",\n")?;
        } else {
            self.first_record = false;
        }

        let json_str = self.record_to_json(record)?;

        // 为每行添加适当的缩进
        let indented_json = json_str
            .lines()
            .map(|line| format!("  {}", line))
            .collect::<Vec<_>>()
            .join("\n");

        self.writer.write_all(indented_json.as_bytes())?;
        self.stats.exported_records += 1;

        Ok(())
    }

    fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
        self.insert_records(records)
    }

    fn finalize(&mut self) -> Result<()> {
        // 写入 JSON 数组结束符
        self.writer.write_all(b"\n]\n")?;
        self.writer.flush()?;
        self.stats.finish();

        #[cfg(feature = "logging")]
        tracing::info!("JSON导出完成: {} 条记录", self.stats.exported_records);

        Ok(())
    }

    fn get_stats(&self) -> ExportStats {
        self.stats.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Read;

    #[test]
    fn test_json_first_and_comma() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut exporter = SyncJsonExporter::new(path).unwrap();

        let r1 = Sqllog { occurrence_time: "t1".into(), ep: "e".into(), ..Default::default() };
        let r2 = Sqllog { occurrence_time: "t2".into(), ep: "e".into(), ..Default::default() };

        exporter.insert_records(&[r1.clone()]).unwrap();
        exporter.insert_records(&[r2.clone()]).unwrap();
        exporter.finalize().unwrap();

        let mut s = String::new();
        tmp.reopen().unwrap().read_to_string(&mut s).unwrap();
        assert!(s.starts_with("[\n"));
        assert!(s.contains(",\n  {"));
        assert!(s.contains("\n]\n"));
    }

    #[test]
    fn test_json_export_record_and_finalize() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut exporter = SyncJsonExporter::new(path).unwrap();

        let r1 = Sqllog { occurrence_time: "t1".into(), ep: "e".into(), ..Default::default() };
        let r2 = Sqllog { occurrence_time: "t2".into(), ep: "e".into(), ..Default::default() };

        exporter.export_record(&r1).unwrap();
        exporter.export_record(&r2).unwrap();
        exporter.finalize().unwrap();

        let mut s = String::new();
        tmp.reopen().unwrap().read_to_string(&mut s).unwrap();
        assert!(s.contains("t1"));
        assert!(s.contains("t2"));
    }

    #[test]
    fn test_record_to_json_and_indentation() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut exporter = SyncJsonExporter::new(path).unwrap();

        let r = Sqllog {
            occurrence_time: "tjson".into(),
            ep: "ejson".into(),
            description: "djson".into(),
            ..Default::default()
        };

        // test internal conversion
        let js = exporter.record_to_json(&r).unwrap();
        assert!(js.contains("tjson"));
        assert!(js.contains("ejson"));

        // test insert_records writes indentation
        exporter.insert_records(&[r.clone()]).unwrap();
        exporter.finalize().unwrap();
        let mut s = String::new();
        tmp.reopen().unwrap().read_to_string(&mut s).unwrap();
        assert!(s.contains("  {"));
    }

    #[test]
    fn test_export_batch_writes_multiple_records() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut exporter = SyncJsonExporter::new(path).unwrap();

        let r1 = Sqllog { occurrence_time: "a".into(), ep: "e".into(), ..Default::default() };
        let r2 = Sqllog { occurrence_time: "b".into(), ep: "e".into(), ..Default::default() };

        exporter.export_batch(&[r1, r2]).unwrap();
        exporter.finalize().unwrap();

        let mut s = String::new();
        tmp.reopen().unwrap().read_to_string(&mut s).unwrap();
        assert!(s.contains("[\n"));
        assert!(s.contains("a"));
        assert!(s.contains("b"));
    }

    #[test]
    fn test_insert_records_empty_noop() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut exporter = SyncJsonExporter::new(path).unwrap();
        // insert empty slice should be a no-op
        exporter.insert_records(&[]).unwrap();
        exporter.finalize().unwrap();

        let mut s = String::new();
        tmp.reopen().unwrap().read_to_string(&mut s).unwrap();
        // should still have JSON array delimiters
        assert!(s.starts_with("[\n"));
        assert!(s.ends_with("\n]\n"));
    }
}