use crate::error::Result;
use crate::sqllog::{parser::SqllogParser, types::Sqllog};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

/// 文件解析的结果
#[derive(Debug, Default, Clone)]
pub struct ParseResult {
    /// 成功解析的记录
    pub records: Vec<Sqllog>,
    /// 解析错误的记录
    pub errors: Vec<ParseError>,
}

/// 解析错误信息
#[derive(Debug, Clone)]
pub struct ParseError {
    /// 行号
    pub line: usize,
    /// 原始内容
    pub content: String,
    /// 错误信息
    pub error: String,
}

impl ParseResult {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.errors.is_empty()
    }

    pub fn total_count(&self) -> usize {
        self.records.len() + self.errors.len()
    }
}

/// 同步 SQL 日志文件解析器
pub struct SyncSqllogParser;

impl SyncSqllogParser {
    /// 流式解析文件，使用回调函数处理分块数据
    ///
    /// # 参数
    /// - `path`: 要解析的日志文件路径
    /// - `chunk_size`: 每次回调处理的记录数量
    /// - `hook`: 回调函数，接收解析的记录和错误
    pub fn parse_with_hooks<P, F>(
        path: P,
        chunk_size: usize,
        mut hook: F,
    ) -> Result<()>
    where
        P: AsRef<Path>,
        F: FnMut(&[Sqllog], &[ParseError]),
    {
        let path_ref = path.as_ref();
        #[cfg(feature = "logging")]
        tracing::debug!(
            "开始流式解析文件: {}, chunk_size = {}",
            path_ref.display(),
            chunk_size
        );

        let file = File::open(path_ref)?;
        let reader = BufReader::new(file);

        let mut records = Vec::new();
        let mut raw_errors = Vec::new(); // 使用原始的错误格式
        let mut line_num = 1usize;
        let mut content = String::new();
        let mut has_first_row = false;

        for line in reader.lines() {
            let line_str = match line {
                Ok(line) => line,
                Err(e) => {
                    #[cfg(feature = "logging")]
                    tracing::warn!("读取行 {} 时出错: {}", line_num, e);
                    #[cfg(not(feature = "logging"))]
                    let _ = e; // 避免未使用变量警告
                    line_num += 1;
                    continue;
                }
            };

            // 处理每一行
            SqllogParser::process_line(
                &line_str,
                &mut has_first_row,
                &mut content,
                &mut line_num,
                &mut records,
                &mut raw_errors,
            );

            // 检查是否需要分块处理（records 或 errors 达到阈值时都触发）
            if chunk_size > 0
                && (records.len() + raw_errors.len()) >= chunk_size
            {
                #[cfg(feature = "logging")]
                tracing::trace!(
                    "触发分块处理: {} 条记录 + {} 个错误 >= {} (chunk_size)",
                    records.len(),
                    raw_errors.len(),
                    chunk_size
                );

                // 转换错误格式并调用回调
                let errors: Vec<ParseError> = raw_errors
                    .iter()
                    .map(|(line, content, error)| ParseError {
                        line: *line,
                        content: content.clone(),
                        error: error.to_string(),
                    })
                    .collect();

                hook(&records, &errors);

                #[cfg(feature = "logging")]
                tracing::debug!(
                    "完成分块处理: {} 条记录, {} 个错误",
                    records.len(),
                    errors.len()
                );

                records.clear();
                raw_errors.clear();
            }
        }

        // 处理最后的内容
        if !content.is_empty() {
            #[cfg(feature = "logging")]
            tracing::trace!(
                "处理文件末尾剩余内容，长度: {} 字符",
                content.len()
            );

            SqllogParser::flush_content(
                &content,
                line_num,
                &mut records,
                &mut raw_errors,
            );
        }

        // 处理剩余的记录和错误
        if !records.is_empty() || !raw_errors.is_empty() {
            #[cfg(feature = "logging")]
            tracing::trace!(
                "处理剩余数据: {} 条记录, {} 个错误",
                records.len(),
                raw_errors.len()
            );

            // 转换错误格式并调用回调
            let errors: Vec<ParseError> = raw_errors
                .iter()
                .map(|(line, content, error)| ParseError {
                    line: *line,
                    content: content.clone(),
                    error: error.to_string(),
                })
                .collect();

            hook(&records, &errors);

            #[cfg(feature = "logging")]
            tracing::debug!(
                "完成最终批次处理: {} 条记录, {} 个错误",
                records.len(),
                errors.len()
            );
        }

        #[cfg(feature = "logging")]
        tracing::info!(
            "流式解析文件完成: {}, 总处理行数: {}",
            path_ref.display(),
            line_num
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    // Helper to create a temp file with given content and return its path
    fn write_temp_file(name: &str, content: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(name);
        // ignore result for overwrite during tests
        let _ = fs::write(&p, content);
        p
    }

    #[test]
    fn test_parse_with_hooks_file_not_found_returns_err() {
        let missing =
            std::env::temp_dir().join("this_file_should_not_exist_12345.log");
        // ensure the file does not exist
        let _ = fs::remove_file(&missing);

        let res = SyncSqllogParser::parse_with_hooks(
            missing,
            1,
            |_records, _errors| {},
        );
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_with_hooks_chunk_size_one_calls_hook_per_record() {
        // a minimal valid segment matching the parser regex with EXECTIME line
        let segment = "2025-09-21 12:00:00.000 (EP[1] sess:0x1 thrd:1 user:root trxid:1 stmt:0x1) [SEL]: select 1\nEXECTIME: 100(ms) ROWCOUNT: 1 EXEC_ID: 123.";
        let path = write_temp_file("sqllog_test_chunk1.log", segment);

        let hook_calls: Arc<Mutex<Vec<(usize, usize)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let hook_calls_clone = hook_calls.clone();

        let res = SyncSqllogParser::parse_with_hooks(
            &path,
            1,
            move |records, errors| {
                let mut guard = hook_calls_clone.lock().unwrap();
                guard.push((records.len(), errors.len()));
            },
        );

        // cleanup
        let _ = fs::remove_file(&path);

        assert!(res.is_ok());
        let guard = hook_calls.lock().unwrap();
        // with chunk_size=1 and a single record, hook should be called once with 1 record
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].0, 1);
        assert_eq!(guard[0].1, 0);
    }

    #[test]
    fn test_parse_with_hooks_mixed_valid_and_invalid_segments() {
        // valid segment followed by an invalid segment that still starts with a timestamp
        // so the parser treats it as a new segment but fails to parse its fields
        let valid = "2025-09-21 12:00:00.000 (EP[1] sess:0x1 thrd:1 user:root trxid:1 stmt:0x1) [SEL]: select 1\nEXECTIME: 50(ms) ROWCOUNT: 1 EXEC_ID: 11.";
        // starts with a timestamp but missing required parentheses/fields
        let invalid =
            "2025-09-21 12:00:01.000 malformed header without expected fields";
        let content = format!("{}\n{}\n", valid, invalid);
        let path = write_temp_file("sqllog_test_mixed.log", &content);

        let stats: Arc<Mutex<(usize, usize)>> =
            Arc::new(Mutex::new((0usize, 0usize)));
        let stats_clone = stats.clone();

        let res = SyncSqllogParser::parse_with_hooks(
            &path,
            2,
            move |records, errors| {
                let mut g = stats_clone.lock().unwrap();
                g.0 += records.len();
                g.1 += errors.len();
            },
        );

        // cleanup
        let _ = fs::remove_file(&path);

        assert!(res.is_ok());
        let g = stats.lock().unwrap();
        // we expect one successful record and one error for the invalid segment
        assert!(g.0 >= 1);
        assert!(g.1 >= 1);
    }

    #[test]
    fn test_parse_with_hooks_ignores_whitespace_only_content() {
        let content = "\n\n  \n\r\n";
        let path = write_temp_file("sqllog_test_ws.log", content);

        let calls: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let calls_clone = calls.clone();

        let res = SyncSqllogParser::parse_with_hooks(
            &path,
            1,
            move |_records, _errors| {
                let mut g = calls_clone.lock().unwrap();
                *g += 1;
            },
        );

        // cleanup
        let _ = fs::remove_file(&path);

        assert!(res.is_ok());
        let g = calls.lock().unwrap();
        // no hook calls expected for whitespace-only file
        assert_eq!(*g, 0);
    }
}
