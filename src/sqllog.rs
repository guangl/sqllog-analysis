// 引入常用库和依赖
use lazy_static::lazy_static; // 用于静态正则表达式
use memchr::memchr; // 高效查找字节分隔符
use regex::Regex; // 正则表达式解析日志
use std::path::Path; // 文件路径处理
use thiserror::Error; // 错误类型派生

/// 通用结果类型，统一错误处理
pub type SResult<T> = std::result::Result<T, SqllogError>;

/// 日志解析相关错误类型
#[derive(Error, Debug)]
pub enum SqllogError {
    /// IO 错误（文件读写）
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),

    /// UTF8 解码错误
    #[error("UTF8解码错误: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    /// 正则表达式解析错误
    #[error("正则解析错误: {0}")]
    Regex(#[from] regex::Error),

    /// 字段解析错误（数字等）
    #[error("字段解析错误: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    /// 日志格式错误，包含行号和内容
    #[error("日志格式错误: 行{line}: {content}")]
    Format { line: usize, content: String },

    /// 其他未知错误
    #[error("未知错误: {0}")]
    Other(String),
}

/// 每月天数（非闰年），用于日期合法性校验
const DAYS_IN_MONTH: [u8; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// 单条 SQL 日志结构体，包含所有解析字段
pub struct Sqllog {
    /// 日志发生时间
    pub occurrence_time: String,
    /// EP 标识
    pub ep: i32,
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
    pub execute_time: Option<u64>,
    /// 影响行数
    pub rowcount: Option<u64>,
    /// 执行 ID
    pub execute_id: Option<u64>,
}

impl Sqllog {
    /// 构造函数，组装所有字段
    pub fn new(
        occurrence_time: String,
        ep: i32,
        session: Option<String>,
        thread: Option<String>,
        user: Option<String>,
        trx_id: Option<String>,
        statement: Option<String>,
        appname: Option<String>,
        ip: Option<String>,
        sql_type: Option<String>,
        description: String,
        execute_time: Option<u64>,
        rowcount: Option<u64>,
        execute_id: Option<u64>,
    ) -> Self {
        Self {
            occurrence_time,
            ep,
            session,
            thread,
            user,
            trx_id,
            statement,
            appname,
            ip,
            sql_type,
            description,
            execute_time,
            rowcount,
            execute_id,
        }
    }

    /// 从单段日志文本解析出 Sqllog 结构体
    ///
    /// # 参数
    /// * `segment` - 日志文本段
    /// * `line_num` - 当前段落所在行号（用于错误提示）
    ///
    /// # 返回
    /// * Ok(Some(Sqllog)) - 解析成功
    /// * Ok(None) - 解析失败但不报错
    /// * Err(SqllogError) - 格式错误
    pub fn from_line(segment: &str, line_num: usize) -> SResult<Option<Self>> {
        // 静态正则表达式，提升性能
        lazy_static! {
            static ref SQLLOG_RE: Regex = Regex::new(
                r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \(EP\[(\d+)\] sess:(NULL|0x[0-9a-f]+) thrd:(-1|NULL|\d+) user:(NULL|\w+) trxid:(NULL|\d+) stmt:(NULL|0x[0-9a-f]+)(?:\sappname:(.*?))?(?:\sip(?::(?:::ffff:)?([0-9]{1,3}(?:\.[0-9]{1,3}){3}))?)?\)\s(?:\[(INS|DEL|ORA|UPD|SEL)\]:?\s)?(.*)"
            ).unwrap();
            static ref DESC_RE: Regex = Regex::new(
                r"EXECTIME:\s*(\d+)\(ms\)\s*ROWCOUNT:\s*(\d+)\s*EXEC_ID:\s*(\d+)\."
            ).unwrap();
        }

        // 只对完整段做正则匹配
        if let Some(caps) = SQLLOG_RE.captures(segment) {
            let occurrence_time =
                caps.get(1)
                    .map(|m| m.as_str().to_string())
                    .ok_or_else(|| SqllogError::Format {
                        line: line_num,
                        content: segment.to_string(),
                    })?;
            let ep = caps
                .get(2)
                .ok_or_else(|| SqllogError::Format {
                    line: line_num,
                    content: segment.to_string(),
                })?
                .as_str()
                .parse()
                .map_err(|_| SqllogError::Format {
                    line: line_num,
                    content: segment.to_string(),
                })?;

            // 处理可能为 NULL 的字段
            let session = match caps.get(3).map(|m| m.as_str()) {
                Some("NULL") => None,
                Some(s) => Some(s.to_string()),
                None => {
                    return Err(SqllogError::Format {
                        line: line_num,
                        content: segment.to_string(),
                    });
                }
            };
            let thread = match caps.get(4).map(|m| m.as_str()) {
                Some("NULL") => None,
                Some("-1") => Some("-1".to_string()),
                Some(s) => Some(s.to_string()),
                None => {
                    return Err(SqllogError::Format {
                        line: line_num,
                        content: segment.to_string(),
                    });
                }
            };
            let user = match caps.get(5).map(|m| m.as_str()) {
                Some("NULL") => None,
                Some(s) => Some(s.to_string()),
                None => {
                    return Err(SqllogError::Format {
                        line: line_num,
                        content: segment.to_string(),
                    });
                }
            };
            let trx_id = match caps.get(6).map(|m| m.as_str()) {
                Some("NULL") => None,
                Some(s) => Some(s.to_string()),
                None => {
                    return Err(SqllogError::Format {
                        line: line_num,
                        content: segment.to_string(),
                    });
                }
            };
            let statement = match caps.get(7).map(|m| m.as_str()) {
                Some("NULL") => None,
                Some(s) => Some(s.to_string()),
                None => {
                    return Err(SqllogError::Format {
                        line: line_num,
                        content: segment.to_string(),
                    });
                }
            };
            let appname = caps.get(8).and_then(|m| {
                let s = m.as_str();
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            });
            let ip = caps.get(9).and_then(|m| {
                let s = m.as_str();
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            });
            let sql_type = caps.get(10).map(|m| m.as_str().to_string());
            let description = caps
                .get(11)
                .map(|m| m.as_str().to_string())
                .ok_or_else(|| SqllogError::Format {
                    line: line_num,
                    content: segment.to_string(),
                })?;
            let (execute_time, rowcount, execute_id) =
                if let Some(desc_caps) = DESC_RE.captures(&description) {
                    (
                        Some(
                            desc_caps
                                .get(1)
                                .ok_or_else(|| SqllogError::Format {
                                    line: line_num,
                                    content: description.clone(),
                                })?
                                .as_str()
                                .parse()
                                .map_err(|_| SqllogError::Format {
                                    line: line_num,
                                    content: description.clone(),
                                })?,
                        ),
                        Some(
                            desc_caps
                                .get(2)
                                .ok_or_else(|| SqllogError::Format {
                                    line: line_num,
                                    content: description.clone(),
                                })?
                                .as_str()
                                .parse()
                                .map_err(|_| SqllogError::Format {
                                    line: line_num,
                                    content: description.clone(),
                                })?,
                        ),
                        Some(
                            desc_caps
                                .get(3)
                                .ok_or_else(|| SqllogError::Format {
                                    line: line_num,
                                    content: description.clone(),
                                })?
                                .as_str()
                                .parse()
                                .map_err(|_| SqllogError::Format {
                                    line: line_num,
                                    content: description.clone(),
                                })?,
                        ),
                    )
                } else {
                    (None, None, None)
                };

            Ok(Some(Self::new(
                occurrence_time,
                ep,
                session,
                thread,
                user,
                trx_id,
                statement,
                appname,
                ip,
                sql_type,
                description,
                execute_time,
                rowcount,
                execute_id,
            )))
        } else {
            Err(SqllogError::Format {
                line: line_num,
                content: segment.to_string(),
            })
        }
    }

    /// 进度打印辅助函数
    ///
    /// # 参数
    /// * `current` - 当前处理字节数
    /// * `total` - 文件总字节数
    /// * `last_percent` - 上次打印的进度百分比
    fn print_progress(current: usize, total: usize, last_percent: &mut u8) {
        let percent = ((current as f64 / total as f64) * 100.0) as u8;
        if percent >= *last_percent + 5 {
            print!("\r处理进度: {}% ", percent);
            std::io::Write::flush(&mut std::io::stdout()).ok();
            *last_percent = percent;
        }
    }

    /// 段落处理辅助函数
    ///
    /// # 参数
    /// * `buf` - 当前段落缓冲区
    /// * `line_num` - 当前段落行号
    /// * `sqllogs` - 结果累加 Vec
    fn handle_segment(buf: &mut String, line_num: usize, sqllogs: &mut Vec<Sqllog>) -> SResult<()> {
        if !buf.is_empty() {
            match Sqllog::from_line(buf, line_num) {
                Ok(Some(log)) => sqllogs.push(log),
                Ok(None) => {}
                Err(e) => return Err(e),
            }
            buf.clear();
        }
        Ok(())
    }

    /// 从文件批量解析 SQL 日志，自动分段
    ///
    /// # 参数
    /// * `path` - 文件路径
    ///
    /// # 返回
    /// * Ok(Vec<Sqllog>) - 解析成功
    /// * Err(SqllogError) - 解析失败
    pub fn from_file<P: AsRef<Path>>(path: P) -> SResult<Vec<Self>> {
        let data = std::fs::read(path.as_ref())?;
        let file_name = path
            .as_ref()
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        println!("开始处理文件: {}", file_name);
        let total = data.len();
        if total == 0 {
            // 空文件直接返回 Ok(Vec::new)
            return Ok(Vec::new());
        }

        let mut last_percent = 0u8;
        let mut sqllogs = Vec::with_capacity(1_000_000);
        let mut has_first_row = false;
        let mut offset = 0;
        let mut segment_buf = String::new();
        let mut line_num = 1;

        while offset < total {
            let end = match memchr(b'\n', &data[offset..]) {
                Some(e) => offset + e,
                None => total,
            };
            let line = &data[offset..end];
            offset = end + 1;

            // 去除行首空白
            let line_trimmed = match line.iter().position(|&b| b != b' ' && b != b'\t') {
                Some(pos) => &line[pos..],
                None => line,
            };

            Sqllog::print_progress(offset, total, &mut last_percent);

            let line_str = match std::str::from_utf8(line_trimmed) {
                Ok(s) => s,
                Err(e) => return Err(SqllogError::Utf8(e)),
            };

            // 只统计原始行的 is_first_row
            if let Some(prefix) = line_str.get(0..23) {
                if is_first_row(prefix) {
                    has_first_row = true;
                }
            }

            // 新段落开始，处理上一段
            if let Some(prefix) = line_str.get(0..23) {
                if is_first_row(prefix) {
                    Sqllog::handle_segment(&mut segment_buf, line_num, &mut sqllogs)?;
                    line_num = 1;
                }
            }

            if !segment_buf.is_empty() {
                segment_buf.push('\n');
            }
            segment_buf.push_str(line_str);
            line_num += 1;
        }

        if !has_first_row {
            return Err(SqllogError::Other("无有效日志行".to_string()));
        }

        // 文件结尾最后一段
        Sqllog::handle_segment(&mut segment_buf, line_num, &mut sqllogs)?;

        println!(
            "\n文件 {} 处理完成，共解析 {} 条记录",
            file_name,
            sqllogs.len()
        );
        Ok(sqllogs)
    }

    /// 以人类可读格式打印单条日志内容
    pub fn display(&self) {
        println!(
            "Occurrence Time: {}, EP: {}, Session: {:?}, Thread: {:?}, User: {:?}, Trx ID: {:?}, Statement: {:?}, Appname: {:?}, IP: {:?}, SQL Type: {:?}, Description: {}, Execute Time: {:?}, Rowcount: {:?}, Execute ID: {:?}",
            self.occurrence_time,
            self.ep,
            self.session,
            self.thread,
            self.user,
            self.trx_id,
            self.statement,
            self.appname,
            self.ip,
            self.sql_type,
            self.description,
            self.execute_time,
            self.rowcount,
            self.execute_id
        );
    }
}

/// 判断年份是否为闰年
#[inline(always)]
const fn is_leap_year(year: u16) -> bool {
    (year & 3 == 0 && year % 100 != 0) || year % 400 == 0
}

/// 判断一行是否为 SQL 日志的首行（时间戳格式）
///
/// # 参数
/// * `s` - 待判断的字符串
///
/// # 返回
/// * true - 是首行
/// * false - 非首行
pub fn is_first_row(s: &str) -> bool {
    // 首先检查长度是否正确 (23个字符)
    if s.len() != 23 {
        return false;
    }

    let b = s.as_bytes();

    // 检查所有分隔符位置
    if !(b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b' '
        && b[13] == b':'
        && b[16] == b':'
        && b[19] == b'.')
    {
        return false;
    }

    // 检查所有数字位
    // 年
    if !b[0].is_ascii_digit()
        || !b[1].is_ascii_digit()
        || !b[2].is_ascii_digit()
        || !b[3].is_ascii_digit()
    {
        return false;
    }

    // 月日时分秒毫秒
    if !b[5].is_ascii_digit()
        || !b[6].is_ascii_digit()
        || !b[8].is_ascii_digit()
        || !b[9].is_ascii_digit()
        || !b[11].is_ascii_digit()
        || !b[12].is_ascii_digit()
        || !b[14].is_ascii_digit()
        || !b[15].is_ascii_digit()
        || !b[17].is_ascii_digit()
        || !b[18].is_ascii_digit()
        || !b[20].is_ascii_digit()
        || !b[21].is_ascii_digit()
        || !b[22].is_ascii_digit()
    {
        return false;
    }

    // 年份合法性校验
    let year = (b[0] - b'0') as u16 * 1000
        + (b[1] - b'0') as u16 * 100
        + (b[2] - b'0') as u16 * 10
        + (b[3] - b'0') as u16;
    if year == 0 {
        return false;
    }

    // 月份合法性校验
    let month = (b[5] - b'0') * 10 + (b[6] - b'0');
    if month == 0 || month > 12 {
        return false;
    }

    // 获取每月最大天数
    let mut max_days = DAYS_IN_MONTH[month as usize - 1];
    if month == 2 && is_leap_year(year) {
        max_days += 1;
    }

    // 日期合法性校验
    let day = (b[8] - b'0') * 10 + (b[9] - b'0');
    if day == 0 || day > max_days {
        return false;
    }

    // 时分秒合法性校验
    let hour = (b[11] - b'0') * 10 + (b[12] - b'0');
    let minute = (b[14] - b'0') * 10 + (b[15] - b'0');
    let second = (b[17] - b'0') * 10 + (b[18] - b'0');

    // 一次性检查所有时间字段
    hour <= 23 && minute <= 59 && second <= 59
}
