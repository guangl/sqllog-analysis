//! utils.rs 模块的单元测试
//!
//! 测试工具函数，包括：
//! - 闰年判断
//! - 首行时间戳识别
//! - 首行位置查找
//! - UTF-8 字节转换处理
//! - 错误处理与恢复

use sqllog_analysis::sqllog::{types::DAYS_IN_MONTH, utils::*};
use sqllog_analysis::error::SqllogError;
use std::borrow::Cow;

#[test]
fn test_is_leap_year_common_cases() {
    // 测试常见的闰年
    assert!(is_leap_year(2000)); // 世纪年，能被400整除
    assert!(is_leap_year(2004)); // 普通能被4整除
    assert!(is_leap_year(2008)); // 普通能被4整除
    assert!(is_leap_year(2012)); // 普通能被4整除
    assert!(is_leap_year(2016)); // 普通能被4整除
    assert!(is_leap_year(2020)); // 普通能被4整除
    assert!(is_leap_year(2024)); // 普通能被4整除
}

#[test]
fn test_is_leap_year_non_leap_cases() {
    // 测试常见的非闰年
    assert!(!is_leap_year(1900)); // 世纪年，不能被400整除
    assert!(!is_leap_year(2100)); // 世纪年，不能被400整除
    assert!(!is_leap_year(2001)); // 不能被4整除
    assert!(!is_leap_year(2002)); // 不能被4整除
    assert!(!is_leap_year(2003)); // 不能被4整除
    assert!(!is_leap_year(2005)); // 不能被4整除
    assert!(!is_leap_year(2019)); // 不能被4整除
    assert!(!is_leap_year(2021)); // 不能被4整除
}

#[test]
fn test_is_leap_year_edge_cases() {
    // 测试边界情况
    assert!(is_leap_year(0)); // 年份0按数学定义是闰年（能被400整除）
    assert!(!is_leap_year(1)); // 年份为1
    assert!(!is_leap_year(3)); // 年份为3
    assert!(is_leap_year(4)); // 最小的闰年
    assert!(!is_leap_year(100)); // 世纪年但不能被400整除
    assert!(is_leap_year(400)); // 能被400整除的世纪年
    assert!(!is_leap_year(500)); // 能被100但不能被400整除
    assert!(is_leap_year(800)); // 能被400整除
}

#[test]
fn test_is_leap_year_century_years() {
    // 专门测试世纪年的闰年判断
    assert!(!is_leap_year(1700)); // 不是闰年
    assert!(!is_leap_year(1800)); // 不是闰年
    assert!(!is_leap_year(1900)); // 不是闰年
    assert!(is_leap_year(2000));  // 是闰年
    assert!(!is_leap_year(2100)); // 不是闰年
    assert!(!is_leap_year(2200)); // 不是闰年
    assert!(!is_leap_year(2300)); // 不是闰年
    assert!(is_leap_year(2400));  // 是闰年
}

#[test]
fn test_is_first_row_valid_timestamps() {
    // 测试有效的时间戳格式
    assert!(is_first_row("2025-09-16 20:02:53.562"));
    assert!(is_first_row("2024-12-31 23:59:59.999"));
    assert!(is_first_row("2000-01-01 00:00:00.000"));
    assert!(is_first_row("2023-02-28 12:30:45.123"));
    assert!(is_first_row("2024-02-29 08:15:22.456")); // 闰年2月29日
    assert!(is_first_row("1999-12-25 18:45:33.789"));
}

#[test]
fn test_is_first_row_invalid_length() {
    // 测试长度不正确的字符串
    assert!(!is_first_row("")); // 空字符串
    assert!(!is_first_row("2025-09-16")); // 太短
    assert!(!is_first_row("2025-09-16 20:02:53")); // 缺少毫秒
    assert!(!is_first_row("2025-09-16 20:02:53.56")); // 毫秒位数不够
    assert!(!is_first_row("2025-09-16 20:02:53.5629")); // 太长
    assert!(!is_first_row("a")); // 单个字符
}

#[test]
fn test_is_first_row_invalid_separators() {
    // 测试分隔符错误的情况
    assert!(!is_first_row("2025/09/16 20:02:53.562")); // 日期分隔符错误
    assert!(!is_first_row("2025-09-16T20:02:53.562")); // 日期时间分隔符错误
    assert!(!is_first_row("2025-09-16 20-02-53.562")); // 时间分隔符错误
    assert!(!is_first_row("2025-09-16 20:02:53,562")); // 毫秒分隔符错误
    assert!(!is_first_row("2025 09 16 20:02:53.562")); // 年月分隔符错误
}

#[test]
fn test_is_first_row_invalid_digits() {
    // 测试非数字字符的情况
    assert!(!is_first_row("202X-09-16 20:02:53.562")); // 年份有字母
    assert!(!is_first_row("2025-0X-16 20:02:53.562")); // 月份有字母
    assert!(!is_first_row("2025-09-1X 20:02:53.562")); // 日期有字母
    assert!(!is_first_row("2025-09-16 2X:02:53.562")); // 小时有字母
    assert!(!is_first_row("2025-09-16 20:0X:53.562")); // 分钟有字母
    assert!(!is_first_row("2025-09-16 20:02:5X.562")); // 秒有字母
    assert!(!is_first_row("2025-09-16 20:02:53.5X2")); // 毫秒有字母
}

#[test]
fn test_is_first_row_invalid_dates() {
    // 测试无效日期
    assert!(!is_first_row("0000-09-16 20:02:53.562")); // 年份为0
    assert!(!is_first_row("2025-00-16 20:02:53.562")); // 月份为0
    assert!(!is_first_row("2025-13-16 20:02:53.562")); // 月份超过12
    assert!(!is_first_row("2025-09-00 20:02:53.562")); // 日期为0
    assert!(!is_first_row("2025-09-32 20:02:53.562")); // 9月只有30天
    assert!(!is_first_row("2025-02-29 20:02:53.562")); // 非闰年2月没有29日
    assert!(!is_first_row("2023-02-29 20:02:53.562")); // 非闰年2月29日
}

#[test]
fn test_is_first_row_invalid_times() {
    // 测试无效时间
    assert!(!is_first_row("2025-09-16 24:02:53.562")); // 小时超过23
    assert!(!is_first_row("2025-09-16 20:60:53.562")); // 分钟超过59
    assert!(!is_first_row("2025-09-16 20:02:60.562")); // 秒超过59
    assert!(!is_first_row("2025-09-16 25:02:53.562")); // 小时超过24
}

#[test]
fn test_is_first_row_leap_year_dates() {
    // 测试闰年相关的日期
    assert!(is_first_row("2024-02-29 12:00:00.000")); // 2024年是闰年，2月29日有效
    assert!(!is_first_row("2023-02-29 12:00:00.000")); // 2023年非闰年，2月29日无效
    assert!(is_first_row("2000-02-29 12:00:00.000")); // 2000年是闰年
    assert!(!is_first_row("1900-02-29 12:00:00.000")); // 1900年不是闰年
}

#[test]
fn test_is_first_row_month_day_limits() {
    // 测试各月份的天数限制
    assert!(is_first_row("2025-01-31 12:00:00.000")); // 1月有31天
    assert!(!is_first_row("2025-01-32 12:00:00.000")); // 1月没有32天
    assert!(is_first_row("2025-02-28 12:00:00.000")); // 2月有28天（非闰年）
    assert!(!is_first_row("2025-02-29 12:00:00.000")); // 非闰年2月没有29天
    assert!(is_first_row("2025-04-30 12:00:00.000")); // 4月有30天
    assert!(!is_first_row("2025-04-31 12:00:00.000")); // 4月没有31天
}

#[test]
fn test_find_first_row_pos_basic() {
    // 测试基本的首行查找
    let input = "2025-09-16 20:02:53.562 some content";
    assert_eq!(find_first_row_pos(input), Some(0));

    let input = "prefix 2025-09-16 20:02:53.562 content";
    assert_eq!(find_first_row_pos(input), Some(7));

    let input = "no timestamp here";
    assert_eq!(find_first_row_pos(input), None);
}

#[test]
fn test_find_first_row_pos_multiple_timestamps() {
    // 测试包含多个时间戳的情况
    let input = "2025-09-16 20:02:53.562\n2025-09-16 20:02:54.563";
    assert_eq!(find_first_row_pos(input), Some(0)); // 应该找到第一个

    let input = "invalid 2025-09-16 20:02:53.562 and 2025-09-16 20:02:54.563";
    assert_eq!(find_first_row_pos(input), Some(8)); // 应该找到第一个有效的
}

#[test]
fn test_find_first_row_pos_edge_cases() {
    // 测试边界情况
    assert_eq!(find_first_row_pos(""), None); // 空字符串
    assert_eq!(find_first_row_pos("2025-09-16 20:02:53.56"), None); // 长度不够

    // 恰好23个字符的有效时间戳
    assert_eq!(find_first_row_pos("2025-09-16 20:02:53.562"), Some(0));

    // 在字符串末尾的时间戳
    let input = "some prefix 2025-09-16 20:02:53.562";
    assert_eq!(find_first_row_pos(input), Some(12));
}

#[test]
fn test_find_first_row_pos_no_valid_timestamp() {
    // 测试不包含有效时间戳的情况
    let inputs = vec![
        "2025-13-16 20:02:53.562", // 无效月份
        "2025-09-32 20:02:53.562", // 无效日期
        "2025-09-16 25:02:53.562", // 无效小时
        "abcdefghijklmnopqrstuvw", // 长度够但不是时间戳
        "202X-09-16 20:02:53.562", // 包含字母
    ];

    for input in inputs {
        assert_eq!(find_first_row_pos(input), None, "应该找不到时间戳: {}", input);
    }
}

#[test]
fn test_line_bytes_to_str_impl_valid_utf8() {
    // 测试有效的UTF-8字节
    let mut errors = Vec::new();
    let bytes = "正常的中文内容".as_bytes();
    let result = line_bytes_to_str_impl(bytes, 1, &mut errors);

    assert!(matches!(result, Cow::Borrowed(_)));
    assert_eq!(result.as_ref(), "正常的中文内容");
    assert!(errors.is_empty());
}

#[test]
fn test_line_bytes_to_str_impl_ascii() {
    // 测试ASCII内容
    let mut errors = Vec::new();
    let bytes = "Normal ASCII content".as_bytes();
    let result = line_bytes_to_str_impl(bytes, 1, &mut errors);

    assert!(matches!(result, Cow::Borrowed(_)));
    assert_eq!(result.as_ref(), "Normal ASCII content");
    assert!(errors.is_empty());
}

#[test]
fn test_line_bytes_to_str_impl_invalid_utf8() {
    // 测试无效的UTF-8字节
    let mut errors = Vec::new();
    let bytes = b"\xFF\xFE\x41\x42\x43"; // 无效的UTF-8序列 + ABC
    let result = line_bytes_to_str_impl(bytes, 5, &mut errors);

    assert!(matches!(result, Cow::Owned(_)));
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, 5); // 行号应该是5
    assert!(errors[0].1.contains("len=5")); // 错误信息应该包含长度
    assert!(matches!(errors[0].2, SqllogError::Utf8(_))); // 应该是UTF-8错误
}

#[test]
fn test_line_bytes_to_str_impl_with_timestamp_recovery() {
    // 测试包含时间戳的UTF-8恢复
    let mut errors = Vec::new();
    let invalid_prefix = b"\xFF\xFE"; // 无效UTF-8前缀
    let valid_timestamp = "2025-09-16 20:02:53.562 content".as_bytes();
    let mut combined = invalid_prefix.to_vec();
    combined.extend_from_slice(valid_timestamp);

    let result = line_bytes_to_str_impl(&combined, 1, &mut errors);

    assert!(matches!(result, Cow::Owned(_)));
    assert_eq!(errors.len(), 1);
    // 结果应该已经同步到时间戳位置
    assert!(result.starts_with("2025-09-16 20:02:53.562"));
}

#[test]
fn test_line_bytes_to_str_impl_empty_bytes() {
    // 测试空字节序列
    let mut errors = Vec::new();
    let bytes = b"";
    let result = line_bytes_to_str_impl(bytes, 1, &mut errors);

    assert!(matches!(result, Cow::Borrowed(_)));
    assert_eq!(result.as_ref(), "");
    assert!(errors.is_empty());
}

#[test]
fn test_line_bytes_to_str_impl_with_trimming() {
    // 测试需要修剪的情况
    let mut errors = Vec::new();
    let bytes = b"\xFF\xFE   \t  content"; // 无效UTF-8 + 空白字符 + 内容
    let result = line_bytes_to_str_impl(bytes, 1, &mut errors);

    assert!(matches!(result, Cow::Owned(_)));
    assert_eq!(errors.len(), 1);
    // 结果应该已经去掉前导空白
    assert!(result.starts_with("content"));
    assert!(!result.starts_with(" "));
    assert!(!result.starts_with("\t"));
}

#[test]
fn test_line_bytes_to_str_impl_multiple_errors() {
    // 测试多次调用收集多个错误
    let mut errors = Vec::new();

    // 第一次调用
    let bytes1 = b"\xFF\xFE\x41";
    let _result1 = line_bytes_to_str_impl(bytes1, 1, &mut errors);

    // 第二次调用
    let bytes2 = b"\x80\x81\x42";
    let _result2 = line_bytes_to_str_impl(bytes2, 2, &mut errors);

    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0].0, 1); // 第一个错误在第1行
    assert_eq!(errors[1].0, 2); // 第二个错误在第2行
}

#[test]
fn test_line_bytes_to_str_impl_error_message_format() {
    // 测试错误信息格式
    let mut errors = Vec::new();
    let bytes = b"\xFF\xFE\x41\x42\x43\x44\x45\x46\x47\x48\x49"; // 超过8字节的无效序列
    let _result = line_bytes_to_str_impl(bytes, 10, &mut errors);

    assert_eq!(errors.len(), 1);
    let error_msg = &errors[0].1;
    assert!(error_msg.starts_with("len=11")); // 应该包含总长度
    assert!(error_msg.contains("prefix=")); // 应该包含前缀
    assert!(error_msg.ends_with("...")); // 长度超过8应该有省略号
}

#[test]
fn test_days_in_month_constant() {
    // 测试DAYS_IN_MONTH常量的正确性
    assert_eq!(DAYS_IN_MONTH.len(), 12);
    assert_eq!(DAYS_IN_MONTH[0], 31); // 1月
    assert_eq!(DAYS_IN_MONTH[1], 28); // 2月
    assert_eq!(DAYS_IN_MONTH[2], 31); // 3月
    assert_eq!(DAYS_IN_MONTH[3], 30); // 4月
    assert_eq!(DAYS_IN_MONTH[4], 31); // 5月
    assert_eq!(DAYS_IN_MONTH[5], 30); // 6月
    assert_eq!(DAYS_IN_MONTH[6], 31); // 7月
    assert_eq!(DAYS_IN_MONTH[7], 31); // 8月
    assert_eq!(DAYS_IN_MONTH[8], 30); // 9月
    assert_eq!(DAYS_IN_MONTH[9], 31); // 10月
    assert_eq!(DAYS_IN_MONTH[10], 30); // 11月
    assert_eq!(DAYS_IN_MONTH[11], 31); // 12月
}

#[test]
fn test_timestamp_edge_cases_boundary() {
    // 测试时间边界情况
    assert!(is_first_row("2025-09-16 00:00:00.000")); // 最小时间
    assert!(is_first_row("2025-09-16 23:59:59.999")); // 最大时间
    assert!(is_first_row("9999-12-31 23:59:59.999")); // 最大可能日期
    assert!(is_first_row("0001-01-01 00:00:00.000")); // 最小非零年份
}

#[test]
fn test_timestamp_validation_comprehensive() {
    // 综合测试时间戳验证的各种情况
    let valid_cases = vec![
        "2025-01-01 00:00:00.000", // 新年第一天
        "2025-12-31 23:59:59.999", // 年末最后一刻
        "2024-02-29 12:00:00.000", // 闰年2月29日
        "2025-06-15 12:30:45.123", // 普通日期
    ];

    let invalid_cases = vec![
        "2025-01-00 12:00:00.000", // 0号日期
        "2025-00-15 12:00:00.000", // 0号月份
        "0000-01-01 12:00:00.000", // 0年
        "2025-01-32 12:00:00.000", // 1月32号
        "2025-04-31 12:00:00.000", // 4月31号
        "2023-02-29 12:00:00.000", // 非闰年2月29日
    ];

    for case in valid_cases {
        assert!(is_first_row(case), "应该是有效时间戳: {}", case);
    }

    for case in invalid_cases {
        assert!(!is_first_row(case), "应该是无效时间戳: {}", case);
    }
}