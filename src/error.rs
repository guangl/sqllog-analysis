//! 错误类型定义
//!
//! 这个模块定义了库中使用的所有错误类型，使用 thiserror 提供丰富的错误信息。

/// SQL日志解析器的结果类型
pub type Result<T> = std::result::Result<T, SqllogError>;

/// SQL日志解析错误类型
#[derive(Debug, thiserror::Error)]
pub enum SqllogError {
    /// IO错误
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),

    /// UTF-8编码错误
    #[error("UTF-8编码错误: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    /// 正则表达式错误
    #[error("正则表达式错误: {0}")]
    Regex(#[from] regex::Error),

    /// 格式错误
    #[error("格式错误 (行{line}): {content}")]
    Format { line: usize, content: String },

    /// 解析错误
    #[error("解析错误: {message}")]
    Parse { message: String },

    /// 配置错误
    #[error("配置错误: {0}")]
    Config(String),

    /// 日志错误（仅在启用 logging feature 时可用）
    #[cfg(feature = "logging")]
    #[error("日志错误: {0}")]
    Log(#[from] crate::logging::LogError),

    /// 其他错误
    #[error("未知错误: {0}")]
    Other(String),
}

impl SqllogError {
    /// 创建一个格式错误
    pub fn format_error(line: usize, content: String) -> Self {
        #[cfg(feature = "logging")]
        {
            crate::logging::ensure_logger_initialized();
            tracing::error!("格式错误发生在第{}行: {}", line, content);
        }
        Self::Format { line, content }
    }

    /// 创建一个解析错误
    pub fn parse_error<S: Into<String>>(message: S) -> Self {
        let message = message.into();
        #[cfg(feature = "logging")]
        {
            crate::logging::ensure_logger_initialized();
            tracing::error!("解析错误: {}", message);
        }
        Self::Parse { message }
    }

    /// 创建一个配置错误
    pub fn config_error<S: Into<String>>(message: S) -> Self {
        let message = message.into();
        #[cfg(feature = "logging")]
        {
            crate::logging::ensure_logger_initialized();
            tracing::error!("配置错误: {}", message);
        }
        Self::Config(message)
    }

    /// 创建一个其他类型错误
    pub fn other<S: Into<String>>(message: S) -> Self {
        let message = message.into();
        #[cfg(feature = "logging")]
        {
            crate::logging::ensure_logger_initialized();
            tracing::error!("未知错误: {}", message);
        }
        Self::Other(message)
    }

    /// 检查是否为 IO 错误
    pub fn is_io_error(&self) -> bool {
        matches!(self, SqllogError::Io(_))
    }

    /// 检查是否为格式错误
    pub fn is_format_error(&self) -> bool {
        matches!(self, SqllogError::Format { .. })
    }

    /// 检查是否为解析错误
    pub fn is_parse_error(&self) -> bool {
        matches!(self, SqllogError::Parse { .. })
    }

    /// 检查是否为配置错误
    pub fn is_config_error(&self) -> bool {
        matches!(self, SqllogError::Config(_))
    }

    /// 检查是否为其他错误
    pub fn is_other_error(&self) -> bool {
        matches!(self, SqllogError::Other(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_error_creation() {
        let format_err =
            SqllogError::format_error(10, "invalid format".to_string());
        assert!(format_err.is_format_error());

        let parse_err = SqllogError::parse_error("parse failed");
        assert!(parse_err.is_parse_error());

        let config_err = SqllogError::config_error("config missing");
        assert!(!config_err.is_io_error());
    }

    #[test]
    fn test_error_from() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let sqllog_err: SqllogError = io_err.into();
        assert!(sqllog_err.is_io_error());
    }

    #[test]
    fn test_error_display() {
        let err =
            SqllogError::Format { line: 42, content: "bad line".to_string() };

        let display = format!("{}", err);
        assert!(display.contains("42"));
        assert!(display.contains("bad line"));
    }
}
