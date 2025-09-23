use core::num;
use std::{io, result, str};
use thiserror::Error;

/// 通用结果类型，统一错误处理
pub type SResult<T> = result::Result<T, SqllogError>;

// 简短类型别名，表示 description 中解析出的三个可选数字
pub type DescNumbers = (Option<i64>, Option<i64>, Option<i64>);

/// 日志解析相关错误类型
#[derive(Error, Debug)]
pub enum SqllogError {
    /// IO 错误（文件读写）
    #[error("IO错误: {0}")]
    Io(#[from] io::Error),

    /// UTF8 解码错误
    #[error("UTF8解码错误: {0}")]
    Utf8(#[from] str::Utf8Error),

    /// 正则表达式解析错误
    #[error("正则解析错误: {0}")]
    Regex(#[from] regex::Error),

    /// 字段解析错误（数字等）
    #[error("字段解析错误: {0}")]
    ParseInt(#[from] num::ParseIntError),

    /// 日志格式错误，包含行号和内容
    #[error("日志格式错误: 行{line}: {content}")]
    Format { line: usize, content: String },

    /// 其他未知错误
    #[error("未知错误: {0}")]
    Other(String),
}

/// 每月天数（非闰年），用于日期合法性校验
pub const DAYS_IN_MONTH: [u8; 12] =
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// 单条 SQL 日志结构体，包含所有解析字段
#[derive(Default, Debug, Clone, serde::Serialize)]
pub struct Sqllog {
    /// 日志发生时间
    pub occurrence_time: String,
    /// EP 标识
    pub ep: String,
    /// 会话 ID
    pub session: Option<String>,
    /// 线程 ID
    pub thread: Option<String>,
    /// 用户名
    pub user: Option<String>,
    /// 事务 ID
    pub trx_id: Option<String>,
    /// 语句指针
    pub statement: Option<String>,
    /// 应用名
    pub appname: Option<String>,
    /// 客户端 IP
    pub ip: Option<String>,
    /// SQL 类型（INS/DEL/UPD/SEL/ORA）
    pub sql_type: Option<String>,
    /// 语句描述（原始文本）
    pub description: String,
    /// 执行时间（毫秒）
    pub execute_time: Option<i64>,
    /// 影响行数
    pub rowcount: Option<i64>,
    /// 执行 ID
    pub execute_id: Option<i64>,
}
