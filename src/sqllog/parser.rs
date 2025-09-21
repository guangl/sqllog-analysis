use crate::sqllog::types::SqllogError;
use crate::sqllog::types::{DescNumbers, SResult, Sqllog};
use lazy_static::lazy_static;
use log::trace;
use regex::Regex;

impl Sqllog {
    /// 从单段日志文本解析出 Sqllog 结构体
    ///
    /// # Errors
    ///
    /// 返回 `SqllogError::Format` 当行不匹配预期格式，或其它解析错误。
    pub fn from_line(segment: &str, line_num: usize) -> SResult<Option<Self>> {
        // 静态正则表达式，提升性能
        lazy_static! {
            static ref SQLLOG_RE: Regex = Regex::new(r"(?s)(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \(EP\[(\d+)\] sess:(NULL|0x[0-9a-f]+) thrd:(-1|NULL|\d+) user:(NULL|\w+) trxid:(NULL|\d+) stmt:(NULL|0x[0-9a-f]+)(?:\sappname:(.*?))?(?:\sip(?::(?:::ffff:)?([0-9]{1,3}(?:\.[0-9]{1,3}){3}))?)?\)\s(?:\[(INS|DEL|ORA|UPD|SEL)\]:?\s)?((?:.|\n)*)").unwrap();
        }

        // 只对完整段做正则匹配
        if let Some(caps) = SQLLOG_RE.captures(segment) {
            trace!("行{line_num} 匹配到 SQLLOG 正则，开始解析字段");
            // 将字段解析提取到私有方法，减少本方法长度
            let log = Self::parse_fields(&caps, segment, line_num)?;
            trace!("行{line_num} 字段解析成功");
            Ok(Some(log))
        } else {
            trace!("行{line_num} 未匹配到 SQLLOG 正则，内容: {segment}");
            Err(SqllogError::Format {
                line: line_num,
                content: segment.to_string(),
            })
        }
    }

    fn parse_fields(
        caps: &regex::Captures,
        segment: &str,
        line_num: usize,
    ) -> SResult<Self> {
        let occurrence_time = Self::get_capture(caps, 1, line_num, segment)?;
        let ep: i32 = Self::get_capture(caps, 2, line_num, segment)?
            .parse()
            .map_err(|_| Self::format_err(line_num, segment))?;

        let session = Self::parse_optional(caps, 3, line_num, segment)?;
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
        let user = Self::parse_optional(caps, 5, line_num, segment)?;
        let trx_id = Self::parse_optional(caps, 6, line_num, segment)?;
        let statement = Self::parse_optional(caps, 7, line_num, segment)?;
        let appname = caps.get(8).and_then(|m| {
            let s = m.as_str();
            if s.is_empty() { None } else { Some(s.to_string()) }
        });
        let ip = caps.get(9).and_then(|m| {
            let s = m.as_str();
            if s.is_empty() { None } else { Some(s.to_string()) }
        });
        let sql_type = caps.get(10).map(|m| m.as_str().to_string());
        let description = Self::get_capture(caps, 11, line_num, segment)?;

        let (execute_time, rowcount, execute_id): DescNumbers =
            Self::parse_desc_numbers(&description, line_num)?;

        Ok(Self {
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
        })
    }

    fn get_capture(
        caps: &regex::Captures,
        idx: usize,
        line_num: usize,
        seg: &str,
    ) -> Result<String, SqllogError> {
        caps.get(idx)
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| Self::format_err(line_num, seg))
    }

    fn parse_optional(
        caps: &regex::Captures,
        idx: usize,
        line_num: usize,
        seg: &str,
    ) -> Result<Option<String>, SqllogError> {
        match caps.get(idx).map(|m| m.as_str()) {
            Some("NULL") => Ok(None),
            Some(s) => Ok(Some(s.to_string())),
            None => Err(SqllogError::Format {
                line: line_num,
                content: seg.to_string(),
            }),
        }
    }

    fn format_err(line: usize, content: &str) -> SqllogError {
        SqllogError::Format { line, content: content.to_string() }
    }

    fn parse_desc_numbers(
        desc: &str,
        line_num: usize,
    ) -> Result<DescNumbers, SqllogError> {
        lazy_static! {
            static ref DESC_RE_INNER: Regex = Regex::new(r"EXECTIME:\s*(\d+)\(ms\)\s*ROWCOUNT:\s*(\d+)\s*EXEC_ID:\s*(\d+).").unwrap();
        }

        if let Some(desc_caps) = DESC_RE_INNER.captures(desc) {
            let parse_group = |i: usize| -> Result<i64, SqllogError> {
                desc_caps
                    .get(i)
                    .ok_or_else(|| Self::format_err(line_num, desc))?
                    .as_str()
                    .parse::<i64>()
                    .map_err(|_| Self::format_err(line_num, desc))
            };

            let et = parse_group(1)?;
            let rc = parse_group(2)?;
            let eid = parse_group(3)?;
            Ok((Some(et), Some(rc), Some(eid)))
        } else {
            Ok((None, None, None))
        }
    }

    // helper: flush 当前 content，调用 from_line 并把结果写入 sqllogs/errors
    pub(crate) fn flush_content(
        content: &str,
        line_num: usize,
        sqllogs: &mut Vec<Self>,
        errors: &mut Vec<(usize, String, SqllogError)>,
    ) {
        // 忽略仅包含空白或换行的段，避免将其作为格式错误上报
        if content.trim().is_empty() {
            return;
        }

        match Self::from_line(content, line_num) {
            Ok(Some(log)) => sqllogs.push(log),
            Ok(None) => errors.push((
                line_num,
                content.to_string(),
                SqllogError::Format {
                    line: line_num,
                    content: content.to_string(),
                },
            )),
            Err(e) => errors.push((line_num, content.to_string(), e)),
        }
    }

    // helper: 处理单行文本，包含新段检测、flush 与合并到 content
    pub(crate) fn process_line(
        line_str: &str,
        has_first_row: &mut bool,
        content: &mut String,
        line_num: &mut usize,
        sqllogs: &mut Vec<Self>,
        errors: &mut Vec<(usize, String, SqllogError)>,
    ) {
        // 为了兼容可能带有前导空白的日志行，仅在检测和插入内容时去除前导空白与替换字符。
        // 同时移除行尾的 CR/LF，否则会导致 description 包含多余的尾部换行（测试期望没有）。
        let tmp = line_str.trim_start_matches(&[' ', '\t', '\u{FFFD}'][..]);
        let clean = tmp.trim_end_matches(&['\r', '\n'][..]);
        let is_new_segment =
            clean.get(0..23).is_some_and(crate::sqllog::utils::is_first_row);

        if is_new_segment {
            *has_first_row = true;
            if !content.is_empty() {
                Self::flush_content(content, *line_num, sqllogs, errors);
                content.clear();
            }
            *line_num = 1;
        }

        if !content.is_empty() {
            content.push('\n');
        }
        content.push_str(clean);
        *line_num += 1;
    }
}
