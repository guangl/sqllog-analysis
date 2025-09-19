use lazy_static::lazy_static;
use memchr::memchr;
use regex::Regex;
use std::path::Path;
use thiserror::Error;

pub type SResult<T> = std::result::Result<T, SqllogError>;

#[derive(Error, Debug)]
pub enum SqllogError {
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("UTF8解码错误: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("正则解析错误: {0}")]
    Regex(#[from] regex::Error),
    #[error("字段解析错误: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("日志格式错误: 行{line}: {content}")]
    Format { line: usize, content: String },
    #[error("未知错误: {0}")]
    Other(String),
}

// 每月天数（非闰年）
const DAYS_IN_MONTH: [u8; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

#[inline(always)]
const fn is_leap_year(year: u16) -> bool {
    (year & 3 == 0 && year % 100 != 0) || year % 400 == 0
}

pub fn is_first_row(s: &str) -> bool {
    // 首先检查长度是否正确 (23个字符)
    if s.len() != 23 {
        return false;
    }

    let b = s.as_bytes();

    // 检查所有分隔符 - 编译器会优化这个模式匹配
    if !(b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b' '
        && b[13] == b':'
        && b[16] == b':'
        && b[19] == b'.')
    {
        return false;
    }

    // 检查所有数字位 - 使用模式匹配来保持代码清晰
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

    // 年
    let year = (b[0] - b'0') as u16 * 1000
        + (b[1] - b'0') as u16 * 100
        + (b[2] - b'0') as u16 * 10
        + (b[3] - b'0') as u16;
    if year == 0 {
        return false;
    }

    // 月
    let month = (b[5] - b'0') * 10 + (b[6] - b'0');
    if month == 0 || month > 12 {
        return false;
    }

    // 快速获取每月天数
    let mut max_days = DAYS_IN_MONTH[month as usize - 1];
    if month == 2 && is_leap_year(year) {
        max_days += 1;
    }

    // 日
    let day = (b[8] - b'0') * 10 + (b[9] - b'0');
    if day == 0 || day > max_days {
        return false;
    }

    // 时分秒
    let hour = (b[11] - b'0') * 10 + (b[12] - b'0');
    let minute = (b[14] - b'0') * 10 + (b[15] - b'0');
    let second = (b[17] - b'0') * 10 + (b[18] - b'0');

    // 一次性检查所有时间字段
    hour <= 23 && minute <= 59 && second <= 59
}

pub struct Sqllog {
    // 这里可以添加 sqllog 相关的字段
    pub occurrence_time: String,
    pub ep: i32,
    pub session: Option<String>,
    pub thread: Option<String>,
    pub user: Option<String>,
    pub trx_id: Option<String>,
    pub statement: Option<String>,
    pub appname: Option<String>,
    pub ip: Option<String>,
    pub sql_type: Option<String>,
    pub description: String,
    pub execute_time: Option<u64>,
    pub rowcount: Option<u64>,
    pub execute_id: Option<u64>,
}

impl Sqllog {
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

    pub fn from_line(line: &str, line_num: usize) -> SResult<Option<Self>> {
        lazy_static! {
            static ref SQLLOG_RE: Regex = Regex::new(
                r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \(EP\[(\d+)\] sess:(NULL|0x[0-9a-f]+) thrd:(-1|NULL|\d+) user:(NULL|\w+) trxid:(NULL|\d+) stmt:(NULL|0x[0-9a-f]+)(?:\sappname:(.*?))?(?:\sip:::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}))?\)\s(?:\[(INS|DEL|ORA|UPD|SEL)\]:?\s)?(.*)").unwrap();
            static ref DESC_RE: Regex = Regex::new(
                r"EXECTIME:\s*(\d+)\(ms\)\s*ROWCOUNT:\s*(\d+)\s*EXEC_ID:\s*(\d+)\.").unwrap();
        }

        // 首先检查开头的字符模式
        if line.len() < 23 {
            return Err(SqllogError::Format {
                line: line_num,
                content: line.to_string(),
            });
        }
        let datetime_str = &line[0..23];
        if !is_first_row(datetime_str) {
            return Err(SqllogError::Format {
                line: line_num,
                content: line.to_string(),
            });
        }

        if let Some(caps) = SQLLOG_RE.captures(line) {
            let occurrence_time =
                caps.get(1)
                    .map(|m| m.as_str().to_string())
                    .ok_or_else(|| SqllogError::Format {
                        line: line_num,
                        content: line.to_string(),
                    })?;
            let ep = caps
                .get(2)
                .ok_or_else(|| SqllogError::Format {
                    line: line_num,
                    content: line.to_string(),
                })?
                .as_str()
                .parse()
                .map_err(|_| SqllogError::Format {
                    line: line_num,
                    content: line.to_string(),
                })?;

            // 处理可能为 NULL 的字段
            let session = match caps.get(3).map(|m| m.as_str()) {
                Some("NULL") => None,
                Some(s) => Some(s.to_string()),
                None => {
                    return Err(SqllogError::Format {
                        line: line_num,
                        content: line.to_string(),
                    });
                }
            };
            let thread = match caps.get(4).map(|m| m.as_str()) {
                Some("NULL") | Some("-1") => None,
                Some(s) => Some(s.to_string()),
                None => {
                    return Err(SqllogError::Format {
                        line: line_num,
                        content: line.to_string(),
                    });
                }
            };
            let user = match caps.get(5).map(|m| m.as_str()) {
                Some("NULL") => None,
                Some(s) => Some(s.to_string()),
                None => {
                    return Err(SqllogError::Format {
                        line: line_num,
                        content: line.to_string(),
                    });
                }
            };
            let trx_id = match caps.get(6).map(|m| m.as_str()) {
                Some("NULL") => None,
                Some(s) => Some(s.to_string()),
                None => {
                    return Err(SqllogError::Format {
                        line: line_num,
                        content: line.to_string(),
                    });
                }
            };
            let statement = match caps.get(7).map(|m| m.as_str()) {
                Some("NULL") => None,
                Some(s) => Some(s.to_string()),
                None => {
                    return Err(SqllogError::Format {
                        line: line_num,
                        content: line.to_string(),
                    });
                }
            };
            let appname = caps.get(8).map(|m| m.as_str().to_string());
            let ip = caps.get(9).map(|m| m.as_str().to_string());
            let sql_type = caps.get(10).map(|m| m.as_str().to_string());
            let description = caps
                .get(11)
                .map(|m| m.as_str().to_string())
                .ok_or_else(|| SqllogError::Format {
                    line: line_num,
                    content: line.to_string(),
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
                content: line.to_string(),
            })
        }
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> SResult<Vec<Self>> {
        let data = std::fs::read(path.as_ref())?;
        let mut sqllogs = Vec::with_capacity(1_000_000);
        let mut start = 0;
        let mut current_log: Option<Self> = None;
        let file_name = path
            .as_ref()
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        println!("开始处理文件: {}", file_name);
        let total = data.len();
        let mut last_percent = 0;
        let mut line_num = 1;
        while start < total {
            let end = match memchr(b'\n', &data[start..]) {
                Some(e) => start + e,
                None => total,
            };
            let line = &data[start..end];
            start = end + 1;
            let line_trimmed = match line.iter().position(|&b| b != b' ' && b != b'\t') {
                Some(pos) => &line[pos..],
                None => line,
            };
            let current_percent = ((start as f64 / total as f64) * 100.0) as u8;
            if current_percent >= last_percent + 5 {
                print!("\r处理进度: {}% ", current_percent);
                std::io::Write::flush(&mut std::io::stdout()).ok();
                last_percent = current_percent;
            }
            let line_str = match std::str::from_utf8(line_trimmed) {
                Ok(s) => s,
                Err(e) => return Err(SqllogError::Utf8(e)),
            };
            if line_str.len() >= 23 && is_first_row(&line_str[0..23]) {
                if let Some(log) = current_log.take() {
                    sqllogs.push(log);
                }
                current_log = match Self::from_line(line_str, line_num)? {
                    Some(log) => Some(log),
                    None => None,
                };
            } else if let Some(log) = current_log.as_mut() {
                log.description.push('\n');
                log.description.push_str(line_str);
            }
            line_num += 1;
        }
        if let Some(log) = current_log {
            sqllogs.push(log);
        }
        // ...existing code...
        println!(
            "\n文件 {} 处理完成，共解析 {} 条记录",
            file_name,
            sqllogs.len()
        );
        Ok(sqllogs)
    }

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
