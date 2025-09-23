//! SQL 日志类型定义

use serde::{Deserialize, Serialize};

// 简短类型别名，表示 description 中解析出的三个可选数字
pub type DescNumbers = (Option<i64>, Option<i64>, Option<i64>);

/// 每月天数（非闰年），用于日期合法性校验
pub const DAYS_IN_MONTH: [u8; 12] =
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// 单条 SQL 日志结构体，包含所有解析字段
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
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

impl Sqllog {
    /// 创建一个新的空的 Sqllog 实例
    pub fn new() -> Self {
        Self::default()
    }

    /// 获取所有字段名称，用于导出时的表头
    pub fn field_names() -> Vec<&'static str> {
        vec![
            "occurrence_time",
            "ep",
            "session",
            "thread",
            "user",
            "trx_id",
            "statement",
            "appname",
            "ip",
            "sql_type",
            "description",
            "execute_time",
            "rowcount",
            "execute_id",
        ]
    }

    /// 获取字段值，用于导出
    pub fn field_values(&self) -> Vec<String> {
        vec![
            self.occurrence_time.clone(),
            self.ep.clone(),
            self.session.clone().unwrap_or_default(),
            self.thread.clone().unwrap_or_default(),
            self.user.clone().unwrap_or_default(),
            self.trx_id.clone().unwrap_or_default(),
            self.statement.clone().unwrap_or_default(),
            self.appname.clone().unwrap_or_default(),
            self.ip.clone().unwrap_or_default(),
            self.sql_type.clone().unwrap_or_default(),
            self.description.clone(),
            self.execute_time.map(|v| v.to_string()).unwrap_or_default(),
            self.rowcount.map(|v| v.to_string()).unwrap_or_default(),
            self.execute_id.map(|v| v.to_string()).unwrap_or_default(),
        ]
    }
}
