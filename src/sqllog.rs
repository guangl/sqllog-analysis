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

#[cfg(test)]
mod tests {
    use super::*;

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
