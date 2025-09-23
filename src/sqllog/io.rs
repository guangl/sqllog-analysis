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

/// SQL 日志文件解析器
pub struct SqllogFileParser;

impl SqllogFileParser {
    /// 使用回调函数解析文件
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
        log::debug!(
            "开始解析文件: {}, chunk_size = {}",
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
                    log::warn!("读取行 {} 时出错: {}", line_num, e);
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
                records.clear();
                raw_errors.clear();
            }
        }

        // 处理最后的内容
        if !content.is_empty() {
            SqllogParser::flush_content(
                &content,
                line_num,
                &mut records,
                &mut raw_errors,
            );
        }

        // 处理剩余的记录和错误
        if !records.is_empty() || !raw_errors.is_empty() {
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
        }

        log::debug!(
            "文件解析完成，共处理 {} 条记录，{} 个错误",
            records.len(),
            raw_errors.len()
        );
        Ok(())
    }
}
