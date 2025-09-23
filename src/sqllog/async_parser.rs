//! 异步 SQL 日志解析模块
//!
//! 提供基于 tokio 的流式文件解析功能

use crate::error::Result;
use crate::sqllog::{
    parser::SqllogParser, sync_parser::ParseError, types::Sqllog,
};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

/// 异步 SQL 日志文件流式解析器
pub struct AsyncSqllogParser;

impl AsyncSqllogParser {
    /// 流式处理大文件，使用生产者-消费者模式
    ///
    /// 返回两个通道接收器：一个用于接收解析的记录，一个用于接收解析错误
    ///
    /// # 参数
    /// - `path`: 要解析的日志文件路径
    /// - `chunk_size`: 每次发送的记录数量
    ///
    /// # 返回
    /// 返回记录接收器和错误接收器的元组
    pub async fn parse_with_hooks<P>(
        path: P,
        chunk_size: usize,
    ) -> Result<(
        tokio::sync::mpsc::UnboundedReceiver<Vec<Sqllog>>,
        tokio::sync::mpsc::UnboundedReceiver<Vec<ParseError>>,
    )>
    where
        P: AsRef<Path> + Send + 'static,
    {
        // 使用无界通道，简化 API，让消费者控制背压
        let (record_tx, record_rx) =
            tokio::sync::mpsc::unbounded_channel::<Vec<Sqllog>>();
        let (error_tx, error_rx) =
            tokio::sync::mpsc::unbounded_channel::<Vec<ParseError>>();

        let path_owned = path.as_ref().to_path_buf();

        #[cfg(feature = "logging")]
        tracing::debug!(
            "开始流式解析文件: {}, chunk_size = {}",
            path_owned.display(),
            chunk_size
        );

        tokio::spawn(async move {
            let result = Self::parse_stream_internal(
                &path_owned,
                chunk_size,
                record_tx,
                error_tx,
            )
            .await;

            if let Err(e) = result {
                #[cfg(feature = "logging")]
                tracing::error!("流式解析文件失败: {}", e);
            } else {
                #[cfg(feature = "logging")]
                tracing::debug!("流式解析文件完成: {}", path_owned.display());
            }
        });

        Ok((record_rx, error_rx))
    }

    /// 内部流式解析实现
    async fn parse_stream_internal<P>(
        path: P,
        chunk_size: usize,
        record_tx: tokio::sync::mpsc::UnboundedSender<Vec<Sqllog>>,
        error_tx: tokio::sync::mpsc::UnboundedSender<Vec<ParseError>>,
    ) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut records = Vec::new();
        let mut raw_errors = Vec::new();
        let mut line_num = 1usize;
        let mut content = String::new();
        let mut has_first_row = false;

        while let Some(line) = lines.next_line().await? {
            // 处理每一行
            SqllogParser::process_line(
                &line,
                &mut has_first_row,
                &mut content,
                &mut line_num,
                &mut records,
                &mut raw_errors,
            );

            // 检查是否需要发送数据块 - 分别检查记录和错误
            let mut should_yield = false;

            // 发送记录数据块
            if records.len() >= chunk_size {
                if let Err(_) = record_tx.send(records.clone()) {
                    #[cfg(feature = "logging")]
                    tracing::warn!("记录接收器已关闭，停止发送记录");
                    return Ok(());
                }
                records.clear();
                should_yield = true;
            }

            // 发送错误数据块
            if raw_errors.len() >= chunk_size {
                let errors: Vec<ParseError> = raw_errors
                    .iter()
                    .map(|(line, content, error)| ParseError {
                        line: *line,
                        content: content.clone(),
                        error: error.to_string(),
                    })
                    .collect();

                if let Err(_) = error_tx.send(errors) {
                    #[cfg(feature = "logging")]
                    tracing::warn!("错误接收器已关闭，停止发送错误");
                    return Ok(());
                }
                raw_errors.clear();
                should_yield = true;
            }

            // 定期让出控制权，保持异步响应性
            if should_yield || line_num % 100 == 0 {
                tokio::task::yield_now().await;
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

        // 发送剩余的记录和错误
        if !records.is_empty() {
            let _ = record_tx.send(records);
        }

        if !raw_errors.is_empty() {
            let errors: Vec<ParseError> = raw_errors
                .iter()
                .map(|(line, content, error)| ParseError {
                    line: *line,
                    content: content.clone(),
                    error: error.to_string(),
                })
                .collect();

            let _ = error_tx.send(errors);
        }

        Ok(())
    }
}
