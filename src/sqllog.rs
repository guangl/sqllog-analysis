use lazy_static::lazy_static; // 用于静态正则表达式
use log::{error, info, trace};
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
#[derive(Default, Debug)]
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
    ///
    /// # Errors
    /// 返回 `Err(SqllogError)` 可能原因：
    /// - 日志格式不匹配（正则未捕获所有字段）
    /// - 字段解析失败（如数字转换、UTF8 解码等）
    /// - 其他解析异常
    pub fn from_line(segment: &str, line_num: usize) -> SResult<Option<Self>> {
        // 静态正则表达式，提升性能
        lazy_static! {
            static ref SQLLOG_RE: Regex = Regex::new(r"(?s)(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \(EP\[(\d+)\] sess:(NULL|0x[0-9a-f]+) thrd:(-1|NULL|\d+) user:(NULL|\w+) trxid:(NULL|\d+) stmt:(NULL|0x[0-9a-f]+)(?:\sappname:(.*?))?(?:\sip(?::(?:::ffff:)?([0-9]{1,3}(?:\.[0-9]{1,3}){3}))?)?\)\s(?:\[(INS|DEL|ORA|UPD|SEL)\]:?\s)?((?:.|\n)*)"
            ).unwrap();
            static ref DESC_RE: Regex = Regex::new(r"EXECTIME:\s*(\d+)\(ms\)\s*ROWCOUNT:\s*(\d+)\s*EXEC_ID:\s*(\d+)."
            ).unwrap();
        }

        // 只对完整段做正则匹配
        if let Some(caps) = SQLLOG_RE.captures(segment) {
            trace!("行{line_num} 匹配到 SQLLOG 正则，开始解析字段");
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

            trace!("行{line_num} 字段解析成功");
            Ok(Some(Self {
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
            }))
        } else {
            trace!("行{line_num} 未匹配到 SQLLOG 正则，内容: {segment}");
            Err(SqllogError::Format {
                line: line_num,
                content: segment.to_string(),
            })
        }
    }

    /// 从文件批量解析 SQL 日志，自动分段
    ///
    /// # 参数
    /// * `path` - 文件路径
    ///
    /// # 返回
    /// * Ok(Vec<Sqllog>) - 解析成功
    /// * Err(SqllogError) - 解析失败
    ///
    /// # Errors
    /// 返回的错误元组中，`SqllogError` 可能原因：
    /// - 文件读取失败（IO 错误）
    /// - 日志行 UTF8 解码失败
    /// - 日志格式不匹配或字段解析失败
    pub fn from_file_with_errors<P: AsRef<Path>>(
        path: P,
    ) -> (Vec<Self>, Vec<(usize, String, SqllogError)>) {
        let data = match std::fs::read(path.as_ref()) {
            Ok(d) => d,
            Err(e) => {
                error!("文件读取失败: {}, 错误: {}", path.as_ref().display(), e);
                return (
                    Vec::new(),
                    vec![(0, format!("IO错误: {e}"), SqllogError::Io(e))],
                );
            }
        };
        let file_name = path
            .as_ref()
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        trace!("开始处理文件: {file_name}");
        let total = data.len();
        if total == 0 {
            return (Vec::new(), Vec::new());
        }

        let mut last_percent = 0u8;
        let mut sqllogs = Vec::with_capacity(1_000_000);
        let mut errors = Vec::new();
        let mut has_first_row = false;
        let mut offset = 0;
        let mut segment_buf = String::new();
        let mut line_num = 1;

        while offset < total {
            let end = memchr(b'\n', &data[offset..]).map_or(total, |e| offset + e);
            let line = &data[offset..end];
            offset = end + 1;

            let line_trimmed = line
                .iter()
                .position(|&b| b != b' ' && b != b'\t')
                .map_or(line, |pos| &line[pos..]);

            Self::print_progress(offset, total, &mut last_percent);

            let line_str = match std::str::from_utf8(line_trimmed) {
                Ok(s) => s,
                Err(e) => {
                    errors.push((line_num, format!("{line_trimmed:?}"), SqllogError::Utf8(e)));
                    has_first_row = true; // 只要有任何行被处理，避免被覆盖为 Other
                    continue;
                }
            };

            let is_new_segment = line_str.get(0..23).is_some_and(is_first_row);

            if is_new_segment {
                has_first_row = true;
                if !segment_buf.is_empty() {
                    match Self::from_line(&segment_buf, line_num) {
                        Ok(Some(log)) => sqllogs.push(log),
                        Ok(None) => {
                            errors.push((
                                line_num,
                                segment_buf.clone(),
                                SqllogError::Format {
                                    line: line_num,
                                    content: segment_buf.clone(),
                                },
                            ));
                        }
                        Err(e) => errors.push((line_num, segment_buf.clone(), e)),
                    }
                    segment_buf.clear();
                }
                line_num = 1;
            }

            // 无论是否新 segment，都合并当前行到 segment_buf
            if !segment_buf.is_empty() {
                segment_buf.push('\n');
            }
            segment_buf.push_str(line_str);
            line_num += 1;
        }

        if !has_first_row {
            return (
                Vec::new(),
                vec![(
                    0,
                    "无有效日志行".to_string(),
                    SqllogError::Other("无有效日志行".to_string()),
                )],
            );
        }

        // 文件结尾最后一段
        if !segment_buf.is_empty() {
            match Self::from_line(&segment_buf, line_num) {
                Ok(Some(log)) => sqllogs.push(log),
                Ok(None) => {
                    errors.push((
                        line_num,
                        segment_buf.clone(),
                        SqllogError::Format {
                            line: line_num,
                            content: segment_buf.clone(),
                        },
                    ));
                }
                Err(e) => errors.push((line_num, segment_buf.clone(), e)),
            }
        }

        info!(
            "文件 {} 处理完成，共解析 {} 条记录，{} 条错误",
            file_name,
            sqllogs.len(),
            errors.len()
        );
        (sqllogs, errors)
    }

    /// 进度打印辅助函数
    ///
    /// # 参数
    /// * `current` - 当前处理字节数
    /// * `total` - 文件总字节数
    /// * `last_percent` - 上次打印的进度百分比
    pub fn print_progress(current: usize, total: usize, last_percent: &mut u8) {
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let percent = ((current as f64 / total as f64) * 100.0) as u8;
        if percent >= *last_percent + 5 {
            print!("\r处理进度: {percent}% ");
            std::io::Write::flush(&mut std::io::stdout()).ok();
            *last_percent = percent;
        }
    }
}

/// 判断年份是否为闰年
/// 判断年份是否为闰年
#[must_use]
const fn is_leap_year(year: u16) -> bool {
    (year.trailing_zeros() >= 2 && year % 100 != 0) || year % 400 == 0
}

/// 判断一行是否为 SQL 日志的首行（时间戳格式）
///
/// # 参数
/// * `s` - 待判断的字符串
///
/// # 返回
/// * `true` - 是首行
/// * `false` - 非首行
#[must_use]
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
    let year = u16::from(b[0] - b'0') * 1000
        + u16::from(b[1] - b'0') * 100
        + u16::from(b[2] - b'0') * 10
        + u16::from(b[3] - b'0');
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
