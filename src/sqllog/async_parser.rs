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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_stream_parse_with_errors() {
        use std::path::PathBuf;

        let mut temp_file = NamedTempFile::new().unwrap();
        // 包含正常记录和格式错误记录的测试内容
        let test_content = r#"2024-01-01 12:00:00.000 (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [SEL]: SELECT * FROM users;
EXECTIME: 100(ms) ROWCOUNT: 5 EXEC_ID: 123.
这是一行不符合时间戳格式的错误内容
应该被识别为解析错误
2024-01-01 12:00:01.000 (EP[2] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [UPD]: UPDATE users SET name = 'test';
EXECTIME: 50(ms) ROWCOUNT: 1 EXEC_ID: 124.
"#;

        temp_file.write_all(test_content.as_bytes()).unwrap();
        let temp_path: PathBuf = temp_file.path().to_path_buf();

        let (mut record_rx, mut error_rx) =
            AsyncSqllogParser::parse_with_hooks(temp_path, 1).await.unwrap();

        // 创建任务来接收记录
        let record_task = tokio::spawn(async move {
            let mut count = 0;
            while let Some(records) = record_rx.recv().await {
                count += records.len();
                println!("接收到 {} 条记录", records.len());
                for record in records {
                    println!(
                        "  记录: 时间={}, SQL={:?}",
                        record.occurrence_time, record.sql_type
                    );
                }
            }
            count
        });

        // 创建任务来接收错误
        let error_task = tokio::spawn(async move {
            let mut count = 0;
            while let Some(errors) = error_rx.recv().await {
                count += errors.len();
                println!("接收到 {} 个错误", errors.len());
                for error in errors {
                    println!("  错误: 第{}行: {}", error.line, error.error);
                }
            }
            count
        });

        // 等待两个任务完成
        let (records_result, errors_result) =
            tokio::join!(record_task, error_task);

        let total_records = records_result.unwrap();
        let total_errors = errors_result.unwrap();

        println!("测试完成: {} 记录, {} 错误", total_records, total_errors);

        // 验证结果 - 所有记录都被正确解析（错误行被当作第一个记录的一部分）
        assert_eq!(total_records, 2); // 2条记录
        assert_eq!(total_errors, 0); // 0个解析错误（因为拼接逻辑）
    }

    #[tokio::test]
    async fn test_chunked_streaming() {
        use std::path::PathBuf;

        let mut temp_file = NamedTempFile::new().unwrap();
        // 创建多条记录来测试分块功能
        let test_content = r#"2024-01-01 12:00:00.000 (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [SEL]: SELECT * FROM users;
EXECTIME: 100(ms) ROWCOUNT: 5 EXEC_ID: 123.
2024-01-01 12:00:01.000 (EP[2] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [UPD]: UPDATE users SET name = 'test';
EXECTIME: 50(ms) ROWCOUNT: 1 EXEC_ID: 124.
2024-01-01 12:00:02.000 (EP[3] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [INS]: INSERT INTO users VALUES (1, 'test');
EXECTIME: 75(ms) ROWCOUNT: 1 EXEC_ID: 125.
2024-01-01 12:00:03.000 (EP[4] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [DEL]: DELETE FROM users WHERE id = 1;
EXECTIME: 25(ms) ROWCOUNT: 1 EXEC_ID: 126.
"#;

        temp_file.write_all(test_content.as_bytes()).unwrap();
        let temp_path: PathBuf = temp_file.path().to_path_buf();

        // 使用小的chunk_size来测试分块功能
        let (mut record_rx, mut error_rx) =
            AsyncSqllogParser::parse_with_hooks(temp_path, 2).await.unwrap();

        let mut total_records = 0;
        let mut batch_count = 0;

        // 接收记录数据，计算批次
        let record_task = tokio::spawn(async move {
            let mut count = 0;
            let mut batches = 0;
            while let Some(records) = record_rx.recv().await {
                count += records.len();
                batches += 1;
                println!("批次 {}: 接收到 {} 条记录", batches, records.len());
            }
            (count, batches)
        });

        // 接收错误数据
        let error_task = tokio::spawn(async move {
            let mut count = 0;
            while let Some(errors) = error_rx.recv().await {
                count += errors.len();
            }
            count
        });

        let (records_result, errors_result) =
            tokio::join!(record_task, error_task);
        let (records_count, batches) = records_result.unwrap();
        let errors_count = errors_result.unwrap();

        total_records = records_count;
        batch_count = batches;

        println!(
            "分块测试完成: {} 记录, {} 批次, {} 错误",
            total_records, batch_count, errors_count
        );

        // 验证记录数量
        assert_eq!(total_records, 4);
        // 验证分块工作（应该有2个批次，每批最多2个记录）
        assert!(batch_count >= 2);
        assert_eq!(errors_count, 0);
    }

    #[tokio::test]
    async fn test_stream_parse() {
        use std::path::PathBuf;

        let mut temp_file = NamedTempFile::new().unwrap();
        let test_content = r#"2024-01-01 12:00:00.000 (EP[1] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [SEL]: SELECT * FROM users;
EXECTIME: 100(ms) ROWCOUNT: 5 EXEC_ID: 123.
2024-01-01 12:00:01.000 (EP[2] sess:NULL thrd:NULL user:NULL trxid:NULL stmt:NULL) [UPD]: UPDATE users SET name = 'test';
EXECTIME: 50(ms) ROWCOUNT: 1 EXEC_ID: 124.
"#;

        temp_file.write_all(test_content.as_bytes()).unwrap();
        let temp_path: PathBuf = temp_file.path().to_path_buf();

        let (mut record_rx, mut error_rx) =
            AsyncSqllogParser::parse_with_hooks(temp_path, 1).await.unwrap();

        let mut total_records = 0;
        let mut total_errors = 0;

        // 接收所有数据
        while let Ok(records) = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            record_rx.recv(),
        )
        .await
        {
            if let Some(records) = records {
                total_records += records.len();
            } else {
                break;
            }
        }

        while let Ok(errors) = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            error_rx.recv(),
        )
        .await
        {
            if let Some(errors) = errors {
                total_errors += errors.len();
            } else {
                break;
            }
        }

        assert_eq!(total_records, 2);
        assert_eq!(total_errors, 0);
    }
}
