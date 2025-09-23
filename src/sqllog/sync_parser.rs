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
