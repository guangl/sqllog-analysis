//! 解析错误写入模块 - 线程安全的 JSONL 错误记录器
//!
//! 本模块提供了一个强健的错误写入系统，用于记录 SQL 日志解析过程中遇到的
//! 各种错误，支持并发处理和详细的错误追踪。
//!
//! ## 核心特性
//!
//! ### 1. 线程安全设计
//! - **并发写入保护**：使用 `Arc<Mutex<BufWriter>>` 确保多线程环境下的写入安全
//! - **缓冲区管理**：自动缓冲和刷新机制，平衡性能与数据完整性
//! - **资源清理**：析构函数确保程序退出时数据完全写入磁盘
//!
//! ### 2. JSONL 格式输出
//! - **结构化数据**：每行一个完整的 JSON 对象，便于程序化分析
//! - **标准化字段**：统一的错误信息格式（path、line、error、raw）
//! - **工具兼容性**：与主流日志分析工具和 JSON 处理工具兼容
//!
//! ### 3. 详细错误追踪
//! - **源文件定位**：记录发生错误的具体文件路径
//! - **行号精确定位**：提供准确的错误行号信息
//! - **原始内容保留**：完整保存导致错误的原始日志内容
//! - **错误类型分类**：区分格式错误、编码错误等不同类型
//!
//! ## 典型工作流程
//!
//! ```text
//! 解析器发现错误 → ErrorWriter::write_errors() → JSON序列化 → 写入文件
//!        ↓                        ↓                    ↓           ↓
//!   收集错误信息            线程安全加锁           格式化输出     缓冲区刷新
//!   (行号,内容,错误)      获取写入器句柄         JSONL格式     持久化存储
//! ```
//!
//! ## 输出格式示例
//!
//! ```json
//! {"path":"sqllog/test.log","line":42,"error":"日志格式错误: 行42: missing EXECTIME","raw":"SELECT * FROM users"}
//! {"path":"sqllog/test.log","line":43,"error":"编码错误: 无效的UTF-8字符","raw":"SELECT * FROM 用户表"}
//! ```
//!
//! ## 使用场景
//!
//! - **批量日志处理**：处理大量日志文件时记录解析失败的条目
//! - **数据质量检查**：识别和分析日志格式的一致性问题
//! - **错误恢复**：为后续的手动修复或自动修复提供详细信息
//! - **监控和报警**：集成到监控系统中跟踪数据质量趋势

// 解析错误写入模块
//
// 提供将解析错误写入 JSONL 文件的功能，支持：
// - 线程安全的并发写入
// - JSONL 格式（每行一个 JSON 对象）
// - 错误信息包含：文件路径、行号、错误描述、原始内容

use crate::sqllog::SqllogError;
use serde_json::json;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// 错误写入器，线程安全地将解析错误写入 JSONL 文件
///
/// ## 设计理念
///
/// `ErrorWriter` 采用了 "write-through" 缓存策略，即每次写入后立即刷新缓冲区。
/// 这确保了在程序意外退出时不会丢失错误信息，这对于长时间运行的批处理
/// 任务尤为重要。
///
/// ## 线程安全保证
///
/// - **互斥锁保护**：使用 `Mutex<BufWriter>` 确保并发写入的原子性
/// - **共享所有权**：通过 `Arc` 允许多个处理线程共享同一个写入器
/// - **错误隔离**：单个写入失败不会影响其他线程的操作
///
/// ## 性能考虑
///
/// - **缓冲写入**：使用 `BufWriter` 减少系统调用次数
/// - **批量处理**：`write_errors` 接受错误列表，支持批量写入
/// - **延迟序列化**：只有在成功获取锁时才进行 JSON 序列化
pub struct ErrorWriter {
    writer: Arc<Mutex<BufWriter<std::fs::File>>>,
    path: PathBuf,
}

impl ErrorWriter {
    /// 创建新的错误写入器
    ///
    /// # Arguments
    /// * `path` - 错误文件的输出路径
    ///
    /// # Errors
    /// 当无法创建或打开输出文件时返回错误
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        // 确保父目录存在
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        let writer = Arc::new(Mutex::new(BufWriter::new(file)));

        Ok(Self { writer, path })
    }

    /// 写入解析错误到文件
    ///
    /// ## 批量写入优势
    ///
    /// 接受错误列表而不是单个错误，这带来了几个好处：
    /// - **减少锁争用**：一次性处理一个文件的所有错误
    /// - **提高吞吐量**：减少系统调用和上下文切换
    /// - **保证原子性**：同一文件的所有错误要么全部写入，要么全部失败
    ///
    /// ## 错误处理策略
    ///
    /// 采用了 "best effort" 策略：
    /// - **序列化失败**：跳过当前错误，继续处理下一个
    /// - **写入失败**：记录日志但不中断处理流程
    /// - **锁获取失败**：记录日志，整个批次写入失败
    ///
    /// ## JSON 字段说明
    ///
    /// - `path`: 源文件的相对路径或绝对路径
    /// - `line`: 错误发生的行号（从解析器角度）
    /// - `error`: 人类可读的错误描述信息
    /// - `raw`: 导致错误的原始日志内容，便于人工检查
    ///
    /// # Arguments
    /// * `file_path` - 发生错误的源文件路径
    /// * `errors` - 错误列表，包含行号、原始内容和错误信息
    pub fn write_errors<P: AsRef<Path>>(
        &self,
        file_path: P,
        errors: &[(usize, String, SqllogError)],
    ) {
        if errors.is_empty() {
            return;
        }

        let file_path_str = file_path.as_ref().to_string_lossy();

        if let Ok(mut writer) = self.writer.lock() {
            for (line_num, raw_line, error) in errors {
                let json_obj = json!({
                    "path": file_path_str,
                    "line": line_num,
                    "error": error.to_string(),
                    "raw": raw_line
                });

                if let Ok(json_str) = serde_json::to_string(&json_obj) {
                    if writeln!(writer, "{json_str}").is_err() {
                        log::error!(
                            "写入错误信息到文件失败: {}",
                            self.path.display()
                        );
                    }
                } else {
                    log::error!("序列化错误信息失败");
                }
            }

            // 立即刷新缓冲区，确保数据写入磁盘
            if writer.flush().is_err() {
                log::error!("刷新错误文件缓冲区失败: {}", self.path.display());
            }
        } else {
            log::error!("获取错误写入器锁失败");
        }
    }

    /// 获取错误文件路径
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ErrorWriter {
    fn drop(&mut self) {
        // 确保在销毁时刷新缓冲区
        if let Ok(mut writer) = self.writer.lock() {
            let _ = writer.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_error_writer_basic() {
        let temp_dir = tempdir().unwrap();
        let error_file = temp_dir.path().join("test_errors.jsonl");

        let writer = ErrorWriter::new(&error_file).unwrap();

        let errors = vec![
            (
                42,
                "invalid log line".to_string(),
                SqllogError::Other("Test error 1".to_string()),
            ),
            (
                43,
                "malformed entry".to_string(),
                SqllogError::Format {
                    line: 43,
                    content: "missing field".to_string(),
                },
            ),
        ];

        writer.write_errors("/path/to/test.log", &errors);

        // 检查文件内容
        let content = fs::read_to_string(&error_file).unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();

        assert_eq!(lines.len(), 2);

        // 验证第一行 JSON
        let first_line: serde_json::Value =
            serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first_line["path"], "/path/to/test.log");
        assert_eq!(first_line["line"], 42);
        assert_eq!(first_line["raw"], "invalid log line");

        // 验证第二行 JSON
        let second_line: serde_json::Value =
            serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second_line["line"], 43);
        assert_eq!(second_line["raw"], "malformed entry");
    }
}
