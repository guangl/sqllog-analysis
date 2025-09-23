//! 错误类型定义

use std::fmt;

/// SQL日志解析器的结果类型
pub type Result<T> = std::result::Result<T, SqllogError>;

/// SQL日志解析错误类型
#[derive(Debug)]
pub enum SqllogError {
    /// IO错误
    Io(std::io::Error),
    /// UTF-8编码错误
    Utf8(std::str::Utf8Error),
    /// 正则表达式错误
    Regex(regex::Error),
    /// 格式错误
    Format { line: usize, content: String },
    /// 其他错误
    Other(String),
}

impl fmt::Display for SqllogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqllogError::Io(e) => write!(f, "IO错误: {}", e),
            SqllogError::Utf8(e) => write!(f, "UTF-8编码错误: {}", e),
            SqllogError::Regex(e) => write!(f, "正则表达式错误: {}", e),
            SqllogError::Format { line, content } => {
                write!(f, "格式错误 (行{}): {}", line, content)
            }
            SqllogError::Other(msg) => write!(f, "未知错误: {}", msg),
        }
    }
}

impl std::error::Error for SqllogError {}

impl From<std::io::Error> for SqllogError {
    fn from(error: std::io::Error) -> Self {
        SqllogError::Io(error)
    }
}

impl From<std::str::Utf8Error> for SqllogError {
    fn from(error: std::str::Utf8Error) -> Self {
        SqllogError::Utf8(error)
    }
}

impl From<regex::Error> for SqllogError {
    fn from(error: regex::Error) -> Self {
        SqllogError::Regex(error)
    }
}
