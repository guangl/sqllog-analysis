//! SQL 日志解析的工具函数

use crate::error::SqllogError;
use crate::sqllog::types;
use std::{borrow::Cow, str};

/// 判断年份是否为闰年
#[must_use]
pub const fn is_leap_year(year: u16) -> bool {
    (year.trailing_zeros() >= 2 && year % 100 != 0) || year % 400 == 0
}

/// 判断一行是否为 SQL 日志的首行（时间戳格式）
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
    let mut max_days = types::DAYS_IN_MONTH[month as usize - 1];
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

/// 在给定字符串中查找第一个符合首行时间戳格式的位置（返回起始索引）。
///
/// 参数：
/// - `s`：待搜索的字符串（可能包含多行）。
///
/// 返回：找到则返回 `Some(index)`，否则返回 `None`。
#[must_use]
pub fn find_first_row_pos(s: &str) -> Option<usize> {
    if s.len() < 23 {
        return None;
    }
    (0..=s.len().saturating_sub(23)).find(|&i| is_first_row(&s[i..i + 23]))
}

/// 将读取到的字节转换为字符串（尽可能为 Borrowed），并在遇到无效 UTF-8 时记录错误。
///
/// 行为说明：
/// - 如果字节序列是有效的 UTF-8，则返回 `Cow::Borrowed(&str)`，避免额外分配。
/// - 若遇到无效 UTF-8，会将错误以 `(line_num, brief_msg, SqllogError::Utf8(_))` 的形式添加到 `errors`，
///   并返回一个经过修复与重同步的 owned `String`（`Cow::Owned`）。
///
/// 参数：
/// - `line_bytes`：原始行字节切片（可能包含换行符）。
/// - `line_num`：当前行号（用于错误记录）。
/// - `errors`：用于收集解析期间遇到的错误条目。
///
/// 返回：`Cow<'a, str>`，在无错误时尽量返回 Borrowed，否则返回 Owned。
pub fn line_bytes_to_str_impl<'a>(
    line_bytes: &'a [u8],
    line_num: usize,
    errors: &mut Vec<(usize, String, SqllogError)>,
) -> Cow<'a, str> {
    match str::from_utf8(line_bytes) {
        Ok(s) => {
            #[cfg(feature = "logging")]
            tracing::trace!(
                line = line_num,
                len = line_bytes.len(),
                "UTF-8 字节序列有效"
            );
            Cow::Borrowed(s)
        }
        Err(e) => {
            #[cfg(feature = "logging")]
            tracing::warn!(line = line_num, error = %e, "发现无效 UTF-8 字节序列");

            // 更保守的错误记录：仅记录总长度和前缀（最多 8 字节）以避免日志膨胀
            let prefix_len = 8usize.min(line_bytes.len());
            let prefix = &line_bytes[..prefix_len];
            let mut err_msg =
                format!("len={} prefix={:?}", line_bytes.len(), prefix);
            if prefix_len < line_bytes.len() {
                err_msg.push_str("...");
            }
            errors.push((line_num, err_msg, SqllogError::Utf8(e)));

            // 使用 lossy 转换得到 owned String，可就地重同步以避免额外分配
            let mut s = String::from_utf8_lossy(line_bytes).into_owned();
            // 裁剪开头的空格/制表符和可能来自无效 UTF-8 序列的替换字符。
            let trimmed_start =
                s.trim_start_matches(&[' ', '\t', '\u{FFFD}'][..]).to_string();
            // 如果 trim 改变了内容，直接使用 trimmed_start
            if trimmed_start.len() != s.len() {
                s = trimmed_start;
            }

            // 尝试重同步到下一个首行时间戳，使用可变操作避免额外分配
            if let Some(pos) = find_first_row_pos(&s) {
                if pos > 0 {
                    #[cfg(feature = "logging")]
                    tracing::trace!(
                        line = line_num,
                        offset = pos,
                        "重同步到下一个首行时间戳"
                    );
                    // 移除前缀以在原有字符串上就地重同步
                    s.drain(0..pos);
                }
            }

            Cow::Owned(s)
        }
    }
}
