use anyhow::Result;
use lazy_static::lazy_static;
use regex::Regex;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

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

    pub fn from_line(line: &str) -> Result<Option<Self>> {
        lazy_static! {
            static ref SQLLOG_RE: Regex = Regex::new(
                r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \(EP\[(\d+)\] sess:(NULL|0x[0-9a-f]+) thrd:(-1|NULL|\d+) user:(NULL|\w+) trxid:(NULL|\d+) stmt:(NULL|0x[0-9a-f]+)(?:\sappname:(.*?))?(?:\sip:::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}))?\)\s(?:\[(INS|DEL|ORA|UPD|SEL)\]:?\s)?(.*)").unwrap();
            static ref DESC_RE: Regex = Regex::new(
                r"EXECTIME:\s*(\d+)\(ms\)\s*ROWCOUNT:\s*(\d+)\s*EXEC_ID:\s*(\d+)\.").unwrap();
        }

        // 首先检查开头的字符模式
        if let Some(datetime_str) = line.get(0..23) {
            if !is_first_row(datetime_str) {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        if let Some(caps) = SQLLOG_RE.captures(line) {
            let occurrence_time = caps.get(1).unwrap().as_str().to_string();
            let ep = caps.get(2).unwrap().as_str().parse()?;

            // 处理可能为 NULL 的字段
            let session = match caps.get(3).unwrap().as_str() {
                "NULL" => None,
                s => Some(s.to_string()),
            };

            let thread = match caps.get(4).unwrap().as_str() {
                "NULL" | "-1" => None,
                s => Some(s.to_string()),
            };

            let user = match caps.get(5).unwrap().as_str() {
                "NULL" => None,
                s => Some(s.to_string()),
            };

            let trx_id = match caps.get(6).unwrap().as_str() {
                "NULL" => None,
                s => Some(s.to_string()),
            };

            let statement = match caps.get(7).unwrap().as_str() {
                "NULL" => None,
                s => Some(s.to_string()),
            };

            // 可选字段
            let appname = caps.get(8).map(|m| m.as_str().to_string());
            let ip = caps.get(9).map(|m| m.as_str().to_string());
            let sql_type = caps.get(10).map(|m| m.as_str().to_string());

            let description = caps.get(11).unwrap().as_str().to_string();

            // 解析描述中的执行信息
            let (execute_time, rowcount, execute_id) =
                if let Some(desc_caps) = DESC_RE.captures(&description) {
                    (
                        Some(desc_caps.get(1).unwrap().as_str().parse()?),
                        Some(desc_caps.get(2).unwrap().as_str().parse()?),
                        Some(desc_caps.get(3).unwrap().as_str().parse()?),
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
            Ok(None)
        }
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Vec<Self>> {
        let file = File::open(path.as_ref())?;
        let file_size = file.metadata()?.len();
        let mut current_pos: u64 = 0;
        let reader = BufReader::new(file);
        let mut sqllogs = Vec::new();
        let mut last_percent = 0;
        let file_name = path
            .as_ref()
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        println!("开始处理文件: {}", file_name);

        for line in reader.lines() {
            let line = line?;
            current_pos += line.len() as u64 + 1; // +1 for newline

            // 每增加1%显示一次进度
            let current_percent = ((current_pos as f64 / file_size as f64) * 100.0) as u8;
            if current_percent > last_percent {
                print!("\r处理进度: {}% ", current_percent);
                std::io::Write::flush(&mut std::io::stdout())?;
                last_percent = current_percent;
            }

            if let Some(sqllog) = Self::from_line(&line)? {
                sqllogs.push(sqllog);
            }
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_sqllog_parsing() -> Result<()> {
        let test_log = r#"2025-10-10 10:10:10.100 (EP[1] sess:0x1234 thrd:1234 user:SYSDBA trxid:5678 stmt:0xabcd appname:TEST ip:::ffff:192.168.1.1) [SEL]: SELECT * FROM test;
2025-10-10 10:10:11.200 (EP[2] sess:0x5678 thrd:NULL user:USER1 trxid:NULL stmt:0xef12) EXECTIME: 100(ms) ROWCOUNT: 10 EXEC_ID: 1."#;

        // 创建临时目录和文件
        let dir = tempdir()?;
        let file_path = dir.path().join("test.log");
        let mut file = File::create(&file_path)?;
        writeln!(file, "{}", test_log)?;

        // 测试从文件解析
        let logs = Sqllog::from_file(&file_path)?;
        assert_eq!(logs.len(), 2);

        // 验证第一条日志
        let log1 = &logs[0];
        assert_eq!(log1.occurrence_time, "2025-10-10 10:10:10.100");
        assert_eq!(log1.ep, 1);
        assert_eq!(log1.session, Some("0x1234".to_string()));
        assert_eq!(log1.thread, Some("1234".to_string()));
        assert_eq!(log1.user, Some("SYSDBA".to_string()));
        assert_eq!(log1.trx_id, Some("5678".to_string()));
        assert_eq!(log1.statement, Some("0xabcd".to_string()));
        assert_eq!(log1.appname, Some("TEST".to_string()));
        assert_eq!(log1.ip, Some("192.168.1.1".to_string()));
        assert_eq!(log1.sql_type, Some("SEL".to_string()));
        assert_eq!(log1.description, "SELECT * FROM test;");
        assert_eq!(log1.execute_time, None);
        assert_eq!(log1.rowcount, None);
        assert_eq!(log1.execute_id, None);

        // 验证第二条日志
        let log2 = &logs[1];
        assert_eq!(log2.occurrence_time, "2025-10-10 10:10:11.200");
        assert_eq!(log2.ep, 2);
        assert_eq!(log2.session, Some("0x5678".to_string()));
        assert_eq!(log2.thread, None);
        assert_eq!(log2.user, Some("USER1".to_string()));
        assert_eq!(log2.trx_id, None);
        assert_eq!(log2.statement, Some("0xef12".to_string()));
        assert_eq!(log2.appname, None);
        assert_eq!(log2.ip, None);
        assert_eq!(log2.sql_type, None);
        assert_eq!(
            log2.description,
            "EXECTIME: 100(ms) ROWCOUNT: 10 EXEC_ID: 1."
        );
        assert_eq!(log2.execute_time, Some(100));
        assert_eq!(log2.rowcount, Some(10));
        assert_eq!(log2.execute_id, Some(1));

        Ok(())
    }

    #[test]
    fn test_is_first_row() {
        // 有效的日期时间
        assert!(is_first_row("2025-10-10 10:10:10.100"));
        assert!(is_first_row("2025-12-31 23:59:59.999"));
        assert!(is_first_row("2025-01-01 00:00:00.000"));

        // 测试月份天数
        assert!(is_first_row("2025-01-31 00:00:00.000")); // 1月31天
        assert!(is_first_row("2025-04-30 00:00:00.000")); // 4月30天
        assert!(!is_first_row("2025-04-31 00:00:00.000")); // 4月没有31天
        assert!(!is_first_row("2025-02-29 00:00:00.000")); // 2025年2月没有29天
        assert!(is_first_row("2024-02-29 00:00:00.000")); // 2024闰年2月有29天
        assert!(!is_first_row("2024-02-30 00:00:00.000")); // 闰年2月也没有30天

        // 无效的日期时间
        assert!(!is_first_row("0000-01-01 00:00:00.000")); // 无效年份
        assert!(!is_first_row("2025-00-01 00:00:00.000")); // 无效月份0
        assert!(!is_first_row("2025-13-10 10:10:10.100")); // 无效月份13
        assert!(!is_first_row("2025-10-00 10:10:10.100")); // 无效日期0
        assert!(!is_first_row("2025-10-32 10:10:10.100")); // 无效日期32
        assert!(!is_first_row("2025-10-10 24:10:10.100")); // 无效小时
        assert!(!is_first_row("2025-10-10 10:60:10.100")); // 无效分钟
        assert!(!is_first_row("2025-10-10 10:10:60.100")); // 无效秒数
        assert!(!is_first_row("2025-10-10 10:10:10.1000")); // 格式错误
        assert!(!is_first_row("2025-10-1010:10:10.100")); // 缺少空格
        assert!(!is_first_row("2025/10/10 10:10:10.100")); // 错误分隔符
        assert!(!is_first_row("")); // 空字符串
        assert!(!is_first_row("2024-6-12 12:34:56.789")); // 月份不符合格式
        assert!(!is_first_row("Invalid line"));
    }
}
